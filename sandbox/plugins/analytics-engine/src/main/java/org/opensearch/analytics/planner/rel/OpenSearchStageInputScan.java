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
import org.opensearch.analytics.planner.FieldStorageInfo;

import java.util.List;

/**
 * Leaf node representing data arriving from a child stage via exchange.
 * Replaces the severed child subtree in the parent stage fragment during
 * DAG construction.
 *
 * <p>Carries the child stage ID, the row type of the child subtree's output,
 * and the viable backends from the parent exchange operator (ExchangeReducer
 * or ShuffleReader).
 *
 * @opensearch.internal
 */
public class OpenSearchStageInputScan extends AbstractRelNode implements OpenSearchRelNode {

    private final int childStageId;
    private final RelDataType rowType;
    private final List<String> viableBackends;

    public OpenSearchStageInputScan(RelOptCluster cluster, RelTraitSet traitSet,
                                    int childStageId, RelDataType rowType,
                                    List<String> viableBackends) {
        super(cluster, traitSet);
        this.childStageId = childStageId;
        this.rowType = rowType;
        this.viableBackends = viableBackends;
    }

    public int getChildStageId() {
        return childStageId;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    // TODO: split OpenSearchRelNode into a marking-phase interface (with getOutputFieldStorage)
    // and a base interface (viableBackends, annotations, copyResolved, stripAnnotations).
    // StageInputScan only needs the base — it's created after marking, never participates in it.
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        throw new UnsupportedOperationException(
            "getOutputFieldStorage should not be called on StageInputScan — "
                + "this node is created after marking, not during it");
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children,
                                List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchStageInputScan(getCluster(), getTraitSet(),
            childStageId, rowType, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return this;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchStageInputScan(getCluster(), traitSet, childStageId, rowType, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("childStageId", childStageId)
            .item("viableBackends", viableBackends);
    }
}
