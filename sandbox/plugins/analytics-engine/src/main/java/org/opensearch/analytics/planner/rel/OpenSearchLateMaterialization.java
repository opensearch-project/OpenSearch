/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.engine.dataformat.DocumentInput;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Stage marker for QTF (Query-Then-Fetch / late materialization). Sits above the anchor Sort.
 * Input rowType is whatever the anchor produces ({@code [reduce-set, ___row_id, ___ugsi]} in
 * the typical shape) — the wrapper does <em>not</em> derive its output rowType from input.
 * Output rowType is {@code aboveAnchorPhysicalFields} only (in topmost-op order, named with
 * physical names). The Scatter-Gather stage drains {@code (___row_id, ___ugsi)} pairs from
 * input, fetches the {@code aboveAnchorPhysicalFields} per survivor, and emits batches
 * matching the wrapper's output rowType.
 *
 * <p>{@code DAGBuilder} pattern-matches this node and emits a {@code LATE_MATERIALIZATION}
 * stage with custom execution. No backend {@link org.opensearch.analytics.spi.FragmentConvertor}
 * is invoked — the stage drives fetch transport directly.
 *
 * <p>TODO: revisit when extending QTF to Joins / Unions — multi-source row-id semantics
 * (which side's {@code ___row_id} / {@code ___ugsi} survives, fetch fan-out across
 * multiple input branches) are not handled here and the single-input {@code SingleRel}
 * shape will need to grow.
 *
 * <p>TODO: this wrapper is effectively an exchange (data crosses node boundaries via
 * scatter-gather fetch), but it isn't introduced by CBO trait propagation like
 * {@code OpenSearchExchangeReducer} — the QTF rewriter inserts it as a post-CBO HEP
 * pass, so its child stage receives a stitched VSR with no Substrait {@code ExchangeRel}
 * representing the boundary. As a consequence, post-LM stages need a special
 * {@code allChildrenAreStageInputScan} branch in {@link
 * org.opensearch.analytics.planner.dag.FragmentConversionDriver}. Revisit whether QTF
 * detection can be modeled as a custom distribution trait so CBO inserts the LM node
 * the same way it inserts gather Reducers, removing the special-case in conversion.
 *
 * @opensearch.internal
 */
public class OpenSearchLateMaterialization extends SingleRel implements OpenSearchRelNode {

    /** Shard-produced row id, last column on Scan output, propagates up through Sort. */
    public static final String ROW_ID_FIELD = DocumentInput.ROW_ID_FIELD;

    /** Coord-appended UGSI (shardOrd + indexUUID + nodeId), declared on ER output. */
    public static final String UGSI_FIELD = "___ugsi";

    /** Helper columns consumed internally by the Scatter-Gather stage (not in wrapper output). */
    public static final Set<String> RESERVED_LATE_MATERIALIZATION_FIELDS = Set.of(ROW_ID_FIELD, UGSI_FIELD);

    private final List<RelDataTypeField> aboveAnchorPhysicalFields;
    private final List<FieldStorageInfo> aboveAnchorPhysicalFieldStorage;
    private final List<String> viableBackends;

    public OpenSearchLateMaterialization(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<RelDataTypeField> aboveAnchorPhysicalFields,
        List<FieldStorageInfo> aboveAnchorPhysicalFieldStorage,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input);
        if (aboveAnchorPhysicalFields.size() != aboveAnchorPhysicalFieldStorage.size()) {
            throw new IllegalArgumentException(
                "aboveAnchorPhysicalFields size "
                    + aboveAnchorPhysicalFields.size()
                    + " != aboveAnchorPhysicalFieldStorage size "
                    + aboveAnchorPhysicalFieldStorage.size()
            );
        }
        this.aboveAnchorPhysicalFields = List.copyOf(aboveAnchorPhysicalFields);
        this.aboveAnchorPhysicalFieldStorage = List.copyOf(aboveAnchorPhysicalFieldStorage);
        this.viableBackends = viableBackends;
        this.rowType = computeRowType(cluster.getTypeFactory(), this.aboveAnchorPhysicalFields);
    }

    /** Output rowType = aboveAnchorPhysicalFields, in iteration order. Input rowType is irrelevant. */
    private static RelDataType computeRowType(RelDataTypeFactory typeFactory, List<RelDataTypeField> aboveAnchorPhysicalFields) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (RelDataTypeField f : aboveAnchorPhysicalFields) {
            builder.add(f.getName(), f.getType());
        }
        return builder.build();
    }

    public List<RelDataTypeField> getAboveAnchorPhysicalFields() {
        return aboveAnchorPhysicalFields;
    }

    /** Per-column storage info for {@link #aboveAnchorPhysicalFields}, in the same order. */
    public List<FieldStorageInfo> getAboveAnchorPhysicalFieldStorage() {
        return aboveAnchorPhysicalFieldStorage;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Output storage = {@code aboveAnchorPhysicalFieldStorage} only — same length and order
     * as the wrapper's output rowType. Input storage is irrelevant; the wrapper exposes
     * fetched physical fields, nothing else.
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        return aboveAnchorPhysicalFieldStorage;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchLateMaterialization(
            getCluster(),
            traitSet,
            sole(inputs),
            aboveAnchorPhysicalFields,
            aboveAnchorPhysicalFieldStorage,
            viableBackends
        );
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Reduces wire bytes by deferring aboveAnchorPhysicalFields until after Sort+Limit; cheap relative to ER.
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> aboveAnchorPhysicalFieldNames = new ArrayList<>(aboveAnchorPhysicalFields.size());
        for (RelDataTypeField f : aboveAnchorPhysicalFields) {
            aboveAnchorPhysicalFieldNames.add(f.getName());
        }
        return super.explainTerms(pw).item("aboveAnchorPhysicalFields", aboveAnchorPhysicalFieldNames)
            .item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchLateMaterialization(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            aboveAnchorPhysicalFields,
            aboveAnchorPhysicalFieldStorage,
            List.of(backend)
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // Stage marker — must be cut by DAGBuilder.cutAtLateMaterialization so it never
        // reaches FragmentConversionDriver. If we hit this path, DAGBuilder didn't cut
        // at the wrapper and the backend's FragmentConvertor would choke on an unknown
        // RelNode. Fail loud instead.
        throw new IllegalStateException(
            "OpenSearchLateMaterialization reached FragmentConversionDriver — DAGBuilder "
                + "must cut at the wrapper. This is a planner / DAGBuilder bug."
        );
    }
}
