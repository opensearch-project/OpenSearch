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
 * Hash-shuffle exchange: partitions the input stream by {@code hashKeys} into {@code partitionCount}
 * output partitions, each routed to a worker node. Produced by {@link OpenSearchDistributionTraitDef}
 * when Volcano's trait conversion encounters a HASH_DISTRIBUTED requirement. Currently unreachable
 * because no rule emits HASH-distributed alternatives under PR #21639's split-rule design — kept
 * for the M2 hash-shuffle redesign.
 *
 * <p>Sibling of {@link OpenSearchExchangeReducer} (SINGLETON gather). The {@code DAGBuilder} cuts
 * at this node to produce hash-shuffle scan stages (M2).
 *
 * @opensearch.internal
 */
public class OpenSearchShuffleExchange extends SingleRel implements OpenSearchRelNode {

    private final List<Integer> hashKeys;
    private final int partitionCount;
    private final List<String> viableBackends;

    public OpenSearchShuffleExchange(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<Integer> hashKeys,
        int partitionCount,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input);
        this.hashKeys = List.copyOf(hashKeys);
        this.partitionCount = partitionCount;
        this.viableBackends = viableBackends;
    }

    public List<Integer> getHashKeys() {
        return hashKeys;
    }

    public int getPartitionCount() {
        return partitionCount;
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
        return new OpenSearchShuffleExchange(getCluster(), traitSet, sole(inputs), hashKeys, partitionCount, viableBackends);
    }

    /** Per-partition fixed setup cost — captures TCP setup, NamedScan registration, and
     *  per-partition state on the consumer. Discourages shuffle for tiny inputs that would
     *  otherwise tie broadcast on raw transfer. */
    private static final double SETUP_COST_PER_PARTITION = 5.0;

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = mq.getRowCount(getInput());
        // Hash shuffle moves all input rows once, partitioned across N consumers. Cost is
        // dominated by the transfer term (proportional to rows) plus a per-partition setup
        // cost. This is the discriminator vs broadcast: shuffle's transfer term doesn't
        // multiply by node count (each row goes to exactly one consumer), so for a small
        // build side broadcast can win even though it pays N copies of setup, while for a
        // large build side shuffle wins because broadcast multiplies rows×N.
        double cost = rows + SETUP_COST_PER_PARTITION * partitionCount;
        return planner.getCostFactory().makeCost(cost, cost, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("hashKeys", hashKeys)
            .item("partitionCount", partitionCount)
            .item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchShuffleExchange(getCluster(), getTraitSet(), children.getFirst(), hashKeys, partitionCount, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return new OpenSearchShuffleExchange(
            getCluster(),
            getTraitSet(),
            strippedChildren.getFirst(),
            hashKeys,
            partitionCount,
            viableBackends
        );
    }
}
