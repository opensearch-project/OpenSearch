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
import org.opensearch.analytics.planner.FieldStorageInfo;

import java.util.List;

/**
 * Read side of a shuffle exchange. Reads shuffle files or streams from
 * source data nodes. Its child is an {@link OpenSearchExchangeWriter}.
 * The {@link #shuffleImpl} matches the Writer's impl.
 *
 * <p>Only used for HASH/RANGE distributions. SINGLETON exchanges use
 * {@link OpenSearchExchangeReducer} instead.
 *
 * @opensearch.internal
 */
public class OpenSearchShuffleReader extends SingleRel implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final ShuffleImpl shuffleImpl;

    public OpenSearchShuffleReader(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                   List<String> viableBackends, ShuffleImpl shuffleImpl) {
        super(cluster, traitSet, input);
        this.viableBackends = viableBackends;
        this.shuffleImpl = shuffleImpl;
    }

    public ShuffleImpl getShuffleImpl() {
        return shuffleImpl;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        if (getInput() instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchShuffleReader(getCluster(), traitSet, sole(inputs), viableBackends, shuffleImpl);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends).item("shuffleImpl", shuffleImpl);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children,
                                List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchShuffleReader(getCluster(), getTraitSet(),
            children.getFirst(), List.of(backend), shuffleImpl);
    }
}
