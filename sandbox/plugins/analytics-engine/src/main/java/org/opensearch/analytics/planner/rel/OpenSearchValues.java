/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Constant-row source — emits a fixed list of literal tuples on the coordinator.
 * Used for {@code SELECT 1+1, NOW()} and similar source-less queries; planned at
 * {@code COORDINATOR+SINGLETON} with no shard scan and no exchange.
 *
 * @opensearch.internal
 */
public class OpenSearchValues extends Values implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchValues(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples,
        List<String> viableBackends
    ) {
        super(cluster, rowType, tuples, traitSet);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** All output fields are derived — there's no physical storage behind a literal row. */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        List<FieldStorageInfo> out = new ArrayList<>(getRowType().getFieldCount());
        for (var field : getRowType().getFieldList()) {
            out.add(FieldStorageInfo.derivedColumn(field.getName(), field.getType().getSqlTypeName()));
        }
        return out;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchValues(getCluster(), traitSet, getRowType(), getTuples(), viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchValues(getCluster(), getTraitSet(), getRowType(), getTuples(), List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalValues.create(getCluster(), getRowType(), getTuples());
    }
}
