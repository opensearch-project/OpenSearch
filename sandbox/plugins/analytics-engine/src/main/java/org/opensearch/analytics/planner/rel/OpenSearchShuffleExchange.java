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
 * when Volcano's trait conversion encounters a HASH_DISTRIBUTED requirement, and by
 * {@link org.opensearch.analytics.planner.rules.OpenSearchHashJoinRule} which inserts explicit
 * shuffle exchanges above both join inputs with each side's key set.
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

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Deliberately huge so Volcano never picks a plan rooted in hash-shuffle until the
        // fragment conversion path supports OpenSearchJoin with OpenSearchShuffleExchange children.
        // The rule ({@code OpenSearchHashJoinRule}) still registers HASH-shuffle alternatives so
        // {@link org.opensearch.analytics.exec.join.JoinStrategyAdvisor} can observe the shape for
        // strategy-selection logging, but {@code findBestExp()} always resolves to the cheaper
        // SINGLETON-reducer alternative until conversion is wired.
        //
        // Codex P1: without this bump Volcano's cost totals are tiny×N for both alternatives and
        // the shuffle tree can beat the reducer tree by one node's worth of cost; {@code
        // FragmentConversionDriver.convertReduceNode()} then fails with "no Join rewire case in
        // DataFusionFragmentConvertor.replaceInput()" because the coordinator fragment shape
        // (OpenSearchJoin over two shuffle exchanges) is unsupported by the reduce-fragment path.
        //
        // TODO: drop this to {@code makeTinyCost()} (or a data-aware cost proportional to row
        // count × partitionCount) when FragmentConversionDriver and DataFusionFragmentConvertor
        // handle shuffle-joined coordinator fragments end-to-end.
        return planner.getCostFactory().makeHugeCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("hashKeys", hashKeys)
            .item("partitionCount", partitionCount)
            .item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchShuffleExchange(getCluster(), getTraitSet(), children.getFirst(), hashKeys, partitionCount, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return new OpenSearchShuffleExchange(getCluster(), getTraitSet(), strippedChildren.getFirst(), hashKeys, partitionCount, viableBackends);
    }
}
