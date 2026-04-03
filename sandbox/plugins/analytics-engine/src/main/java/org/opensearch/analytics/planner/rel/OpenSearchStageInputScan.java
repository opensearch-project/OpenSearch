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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Leaf node representing data arriving from a child stage via exchange.
 * Replaces the severed child subtree in the parent stage fragment during
 * DAG construction.
 *
 * <p>Carries the child stage ID and the row type of the child subtree's
 * output. During fragment conversion (RelNode → Substrait/QueryBuilder),
 * the backend registers a streaming source or shuffle source for this node.
 *
 * @opensearch.internal
 */
public class OpenSearchStageInputScan extends AbstractRelNode {

    private final int childStageId;
    private final RelDataType rowType;

    public OpenSearchStageInputScan(RelOptCluster cluster, RelTraitSet traitSet,
                                    int childStageId, RelDataType rowType) {
        super(cluster, traitSet);
        this.childStageId = childStageId;
        this.rowType = rowType;
    }

    public int getChildStageId() {
        return childStageId;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchStageInputScan(getCluster(), traitSet, childStageId, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("childStageId", childStageId);
    }
}
