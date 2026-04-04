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
 * Write side of an exchange boundary. Sits at the top of a child stage's fragment.
 *
 * <p>For SINGLETON distribution: streams Arrow batches to coordinator via
 * Analytics Core transport. {@link #shuffleImpl} is null.
 * <p>For HASH/RANGE distribution: partitions and writes data between data nodes.
 * {@link #shuffleImpl} is FILE or STREAM based on backend capability.
 *
 * @opensearch.internal
 */
public class OpenSearchExchangeWriter extends SingleRel implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final ShuffleImpl shuffleImpl;
    private final List<Integer> keys;

    public OpenSearchExchangeWriter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                    List<String> viableBackends, ShuffleImpl shuffleImpl, List<Integer> keys) {
        super(cluster, traitSet, input);
        this.viableBackends = viableBackends;
        this.shuffleImpl = shuffleImpl;
        this.keys = keys;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    public ShuffleImpl getShuffleImpl() {
        return shuffleImpl;
    }

    public List<Integer> getKeys() {
        return keys;
    }

    public boolean isShuffle() {
        return shuffleImpl != null;
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
        return new OpenSearchExchangeWriter(getCluster(), traitSet, sole(inputs),
            viableBackends, shuffleImpl, keys);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw).item("viableBackends", viableBackends);
        if (shuffleImpl != null) {
            writer.item("shuffleImpl", shuffleImpl).item("keys", keys);
        }
        return writer;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children,
                                List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchExchangeWriter(getCluster(), getTraitSet(),
            children.getFirst(), List.of(backend), shuffleImpl, keys);
    }
}
