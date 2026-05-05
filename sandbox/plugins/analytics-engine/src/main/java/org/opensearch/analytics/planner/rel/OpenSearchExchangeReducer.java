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
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Coordinator-side reducer for SINGLETON exchanges. Receives streaming
 * Arrow batches from data nodes via Analytics Core transport. The backend
 * decides internally how to reduce (in-memory table, streaming sink, etc.).
 *
 * <p>Only used for SINGLETON distribution. Shuffle exchanges (HASH/RANGE) are
 * not yet implemented — see {@link OpenSearchDistributionTraitDef}.
 *
 * @opensearch.internal
 */
public class OpenSearchExchangeReducer extends SingleRel implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchExchangeReducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<String> viableBackends) {
        super(cluster, traitSet, input);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchExchangeReducer(getCluster(), traitSet, sole(inputs), viableBackends);
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
        return new OpenSearchExchangeReducer(getCluster(), getTraitSet(), children.getFirst(), List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // ExchangeReducer is an infrastructure node — strip children but keep the node itself.
        return new OpenSearchExchangeReducer(getCluster(), getTraitSet(), strippedChildren.getFirst(), viableBackends);
    }
}
