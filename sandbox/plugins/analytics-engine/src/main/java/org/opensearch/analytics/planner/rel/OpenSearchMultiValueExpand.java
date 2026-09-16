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

/**
 * Appends one row-expanded scalar column for a LIST-valued input column: each input row is
 * emitted once per <em>distinct</em> non-null element of the list, with that element in the
 * appended column. The source LIST column is preserved at its original ordinal so aggregate
 * arguments that reference it (e.g. {@code stats list(tags) by tags}) keep seeing the list.
 *
 * <p>Emitted by {@code OpenSearchMultiValueGroupByRewriter} in {@code PlannerImpl} <em>before</em>
 * {@code decomposeAggregates}, so Calcite's PARTIAL/FINAL aggregate split sees the scalar element
 * type on the GROUP BY key in both fragments (the fix for the multi-shard {@code List(Utf8)} vs
 * {@code Utf8View} mismatch that resulted from expanding after decomposition on the backend).
 *
 * <p><b>Why an engine-side rel rather than a {@code LogicalCorrelate + Uncollect}:</b> that is
 * the ideal end state (one shape shared with an explicit {@code mvexpand}), but this branch has
 * no Correlate marking / distribution support; building it is a feature in its own right, tracked
 * for the frontend {@code mvexpand} work. This rel is the minimum-intrusive carrier for the
 * ordering fix: a {@link SingleRel} that rides its child's distribution exactly like
 * {@link OpenSearchFilter}, needs no capability negotiation (its viable backends are its child's),
 * and lowers at fragment conversion to the same {@code MULTI_VALUE_EXPAND} Substrait extension
 * an explicit {@code mvexpand} Correlate does.
 *
 * <p>This single class is BOTH the pre-marking logical node the rewriter emits (empty
 * {@code viableBackends}) and the marked node ({@code OpenSearchMultiValueExpandRule} fills in the
 * child's viable backends). {@link #stripAnnotations} therefore returns a copy of itself rather
 * than a Calcite {@code Logical*} node — there is no Calcite equivalent to strip down to.
 *
 * @opensearch.internal
 */
public class OpenSearchMultiValueExpand extends SingleRel implements OpenSearchRelNode, DistributionAware {

    private final int fieldIndex;
    private final String expandedFieldName;
    private final List<String> viableBackends;

    public OpenSearchMultiValueExpand(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        int fieldIndex,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input);
        this.fieldIndex = fieldIndex;
        if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() == null) {
            throw new IllegalArgumentException("field " + fieldIndex + " is not a collection");
        }
        String candidate = "___mvexpand_" + fieldIndex;
        while (input.getRowType().getFieldNames().contains(candidate)) {
            candidate += "_";
        }
        this.expandedFieldName = candidate;
        this.viableBackends = viableBackends;
    }

    /** Convenience for the pre-marking rewriter: no backends resolved yet. */
    public static OpenSearchMultiValueExpand create(RelNode input, int fieldIndex) {
        return new OpenSearchMultiValueExpand(input.getCluster(), input.getTraitSet(), input, fieldIndex, List.of());
    }

    /** Ordinal of the LIST column in the input that is expanded. */
    public int getFieldIndex() {
        return fieldIndex;
    }

    /** Ordinal (in this rel's output) of the appended scalar element column. */
    public int getExpandedFieldIndex() {
        return getInput().getRowType().getFieldCount();
    }

    public String getExpandedFieldName() {
        return expandedFieldName;
    }

    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        List<RelDataTypeField> fields = getInput().getRowType().getFieldList();
        for (RelDataTypeField field : fields) {
            builder.add(field.getName(), field.getType());
        }
        RelDataType elementType = fields.get(fieldIndex).getType().getComponentType();
        builder.add(expandedFieldName, getCluster().getTypeFactory().createTypeWithNullability(elementType, true));
        return builder.build();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchMultiValueExpand(getCluster(), traitSet, sole(inputs), fieldIndex, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    /**
     * Assumed average number of distinct elements per multi-valued document. Calcite's default
     * {@link SingleRel#estimateRowCount} would report the CHILD's row count, i.e. claim the
     * expansion is row-neutral — which is exactly backwards (an unnest fans out) and led CBO to
     * gather the shards BEFORE expanding and run a single coordinator-side aggregate, shipping
     * every expanded row instead of pre-aggregated partials. Any factor {@code > 1} restores the
     * correct preference (PARTIAL beneath the gather); the specific value only matters relative
     * to the exchange's per-row cost, and 3 is a conservative middle ground for tag-like fields.
     */
    static final double ASSUMED_FAN_OUT = 3.0;

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getInput()) * ASSUMED_FAN_OUT;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("field", getInput().getRowType().getFieldNames().get(fieldIndex))
            .item("output", expandedFieldName)
            .item("distinct", true)
            .item("viableBackends", viableBackends);
    }

    // ---- OpenSearchRelNode ----

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Child's storage for the passthrough columns, plus a synthetic non-stored entry for the appended element. */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        List<FieldStorageInfo> childStorage = input instanceof OpenSearchRelNode osInput ? osInput.getOutputFieldStorage() : List.of();
        List<FieldStorageInfo> result = new ArrayList<>(childStorage.size() + 1);
        result.addAll(childStorage);
        if (!childStorage.isEmpty()) {
            RelDataType elementType = getRowType().getFieldList().get(getExpandedFieldIndex()).getType();
            result.add(FieldStorageInfo.derivedColumn(expandedFieldName, elementType.getSqlTypeName()));
        }
        return result;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchMultiValueExpand(getCluster(), getTraitSet(), children.getFirst(), fieldIndex, List.of(backend));
    }

    /** No Calcite logical equivalent exists — the stripped form is this same rel with backends cleared. */
    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return new OpenSearchMultiValueExpand(getCluster(), getTraitSet(), strippedChildren.getFirst(), fieldIndex, List.of());
    }

    // ---- DistributionAware ----

    /** Per-row expansion imposes no partitioning requirement on its input. */
    @Override
    public OpenSearchDistribution requiredInputDistribution(int inputIndex, int partitionCount, OpenSearchDistributionTraitDef traitDef) {
        return null;
    }

    /** Expands rows within whatever partition they arrive in — output distribution = the child's. */
    @Override
    public OpenSearchDistribution deriveOutputDistribution(
        List<OpenSearchDistribution> childDistributions,
        OpenSearchDistributionTraitDef traitDef
    ) {
        return childDistributions.size() == 1 ? childDistributions.get(0) : null;
    }
}
