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
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Leaf placeholder in the root stage fragment representing the input from a child stage.
 * Replaces the child subtree after DAG construction cuts at exchange boundaries.
 *
 * <p>The Scheduler feeds Arrow Record Batches from the child stage into the coordinator
 * sink — this node signals where that input arrives in the root fragment.
 *
 * @opensearch.internal
 */
public class OpenSearchStageInputScan extends AbstractRelNode implements OpenSearchRelNode {

    private final int childStageId;
    private final List<String> viableBackends;

    public OpenSearchStageInputScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        int childStageId,
        RelDataType rowType,
        List<String> viableBackends
    ) {
        super(cluster, traitSet);
        this.childStageId = childStageId;
        this.viableBackends = viableBackends;
        this.rowType = rowType;
    }

    public int getChildStageId() {
        return childStageId;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        return List.of();
    }

    @Override
    public OpenSearchStageInputScan copy(RelTraitSet traitSet, java.util.List<org.apache.calcite.rel.RelNode> inputs) {
        return new OpenSearchStageInputScan(getCluster(), traitSet, childStageId, rowType, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.item("childStageId", childStageId).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchStageInputScan(getCluster(), getTraitSet(), childStageId, rowType, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return this; // Leaf placeholder — no annotations, no children to strip.
    }
}
