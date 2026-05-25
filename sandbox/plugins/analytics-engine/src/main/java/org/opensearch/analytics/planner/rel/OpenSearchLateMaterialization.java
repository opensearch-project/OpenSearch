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
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Stage marker for QTF (Query-Then-Fetch / late materialization). Sits above the anchor Sort
 * with input schema {@code [<sort/filter cols>, ___row_id, ___ugsi]} and output schema
 * {@code [<sort/filter cols>, <fetch cols...>]} — both helper columns are stripped because
 * the Scatter-Gather stage uses them internally to fan out fetch-by-rowid and joins
 * results back by row position before emitting upward.
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
 * @opensearch.internal
 */
public class OpenSearchLateMaterialization extends SingleRel implements OpenSearchRelNode {

    /** Shard-produced row id, last column on Scan output, propagates up through Sort. */
    public static final String ROW_ID_FIELD = "___row_id";

    /** Coord-appended UGSI (shardOrd + indexUUID + nodeId), declared on ER output. */
    public static final String UGSI_FIELD = "___ugsi";

    /** Helper columns stripped from wrapper output (consumed internally by Scatter-Gather). */
    public static final Set<String> RESERVED_LATE_MATERIALIZATION_FIELDS = Set.of(ROW_ID_FIELD, UGSI_FIELD);

    private final List<RelDataTypeField> fetchList;
    private final List<FieldStorageInfo> fetchListStorage;
    private final List<String> viableBackends;

    public OpenSearchLateMaterialization(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<RelDataTypeField> fetchList,
        List<FieldStorageInfo> fetchListStorage,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input);
        if (fetchList.size() != fetchListStorage.size()) {
            throw new IllegalArgumentException(
                "fetchList size " + fetchList.size() + " != fetchListStorage size " + fetchListStorage.size()
            );
        }
        this.fetchList = List.copyOf(fetchList);
        this.fetchListStorage = List.copyOf(fetchListStorage);
        this.viableBackends = viableBackends;
        this.rowType = computeRowType(cluster.getTypeFactory(), input.getRowType(), this.fetchList);
    }

    /** Output = input fields minus helper columns ++ fetchList. */
    private static RelDataType computeRowType(RelDataTypeFactory typeFactory, RelDataType inputRowType, List<RelDataTypeField> fetchList) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (RelDataTypeField f : inputRowType.getFieldList()) {
            if (RESERVED_LATE_MATERIALIZATION_FIELDS.contains(f.getName())) continue;
            builder.add(f.getName(), f.getType());
        }
        for (RelDataTypeField f : fetchList) {
            builder.add(f.getName(), f.getType());
        }
        return builder.build();
    }

    public List<RelDataTypeField> getFetchList() {
        return fetchList;
    }

    /** Per-column storage info for {@link #fetchList}, in the same order. */
    public List<FieldStorageInfo> getFetchListStorage() {
        return fetchListStorage;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Output storage = input storage with helper-column slots stripped, ++ fetchListStorage.
     * Read by {@code BackendPlanAdapter.adaptProject} to translate Post-Sort RexNodes that
     * reference fetched columns.
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        OpenSearchRelNode openSearchInput = (OpenSearchRelNode) RelNodeUtils.unwrapHep(getInput());
        List<FieldStorageInfo> inputStorage = openSearchInput.getOutputFieldStorage();
        List<RelDataTypeField> inputFields = getInput().getRowType().getFieldList();
        List<FieldStorageInfo> out = new ArrayList<>(inputFields.size() - RESERVED_LATE_MATERIALIZATION_FIELDS.size() + fetchListStorage.size());
        for (int i = 0; i < inputFields.size(); i++) {
            if (RESERVED_LATE_MATERIALIZATION_FIELDS.contains(inputFields.get(i).getName())) continue;
            out.add(inputStorage.get(i));
        }
        out.addAll(fetchListStorage);
        return out;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchLateMaterialization(getCluster(), traitSet, sole(inputs), fetchList, fetchListStorage, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Reduces wire bytes by deferring fetchList until after Sort+Limit; cheap relative to ER.
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> fetchListNames = new ArrayList<>(fetchList.size());
        for (RelDataTypeField f : fetchList) {
            fetchListNames.add(f.getName());
        }
        return super.explainTerms(pw).item("fetchList", fetchListNames).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchLateMaterialization(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            fetchList,
            fetchListStorage,
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
