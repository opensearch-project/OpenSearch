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
 * Broadcast exchange: replicates the input stream's full row set to every probe-side worker.
 * Produced by {@link OpenSearchDistributionTraitDef} when Volcano's trait conversion encounters
 * a {@code BROADCAST_DISTRIBUTED + REPLICATED} requirement on the build side of an MPP join.
 * Sibling of {@link OpenSearchExchangeReducer} (SINGLETON gather) and
 * {@link OpenSearchShuffleExchange} (HASH partition).
 *
 * <p><b>Cost model.</b> Broadcast moves one copy of the input rows to each probe node. Cost is
 * roughly {@code inputRowCount × probeNodeEstimate} — this is what discriminates BROADCAST from
 * HASH_SHUFFLE during CBO: a small build side broadcasts cheaply (small × N), but a large build
 * gets expensive (rows × N grows fast), so HASH wins when both sides are large.
 *
 * <p>{@code probeNodeEstimate} is resolved at split-rule time from the cluster setting
 * {@code analytics.mpp.broadcast.probe_estimate}, defaulting to the cluster's data-node count.
 *
 * <p>TODO(trait-propagation): this node is NOT only a cost holder — {@code DAGBuilder} cuts a child
 * {@code Stage} at each exchange, so it is also the physical stage boundary that becomes a
 * {@code BROADCAST_BUILD} fragment with its own transport (distinct from a shuffle edge). If a future
 * top-down migration ({@code setTopDownOpt} + Calcite {@code PhysicalNode}) folds the broadcast COST
 * into {@link OpenSearchJoin}'s derived distribution trait, this boundary node must still exist for the
 * DAG builder to cut on — a trait on the join alone gives the executor no stage to schedule.
 *
 * @opensearch.internal
 */
public class OpenSearchBroadcastExchange extends SingleRel implements OpenSearchRelNode {

    /** Per-replica fixed setup cost. Originally 5 to discourage broadcast for tiny tables,
     *  but with the post-join gather cost (~SETUP_COST_PER_ER + join_rows) added by
     *  {@link OpenSearchExchangeReducer} above the worker-join, the total broadcast plan was
     *  losing the cost race against the coord-centric 2×ER plan for small build × medium
     *  probe shapes (5×30 in BroadcastJoinIT). Drop to 0 so the broadcast plan's transfer
     *  cost reflects raw build_rows×probeNodes; the +10 ER setup above the worker join
     *  remains the only non-row-proportional term in the plan total. */
    private static final double SETUP_COST_PER_REPLICA = 0.0;

    private final int probeNodeEstimate;
    private final List<String> viableBackends;

    public OpenSearchBroadcastExchange(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        int probeNodeEstimate,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input);
        this.probeNodeEstimate = probeNodeEstimate;
        this.viableBackends = viableBackends;
    }

    public int getProbeNodeEstimate() {
        return probeNodeEstimate;
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
        return new OpenSearchBroadcastExchange(getCluster(), traitSet, sole(inputs), probeNodeEstimate, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = mq.getRowCount(getInput());
        // Broadcast moves N copies of the row set, where N = probe-node count. Each copy
        // also pays a fixed setup cost (TCP setup + NamedScan registration on each probe).
        // The transfer term is what Volcano uses to discriminate broadcast-vs-shuffle: a
        // tiny build side dominates with the setup term (broadcast cheap), a fat build side
        // dominates with the transfer term and grows linearly with the cluster.
        double cost = (rows + SETUP_COST_PER_REPLICA) * probeNodeEstimate;
        return planner.getCostFactory().makeCost(cost, cost, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("probeNodeEstimate", probeNodeEstimate).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchBroadcastExchange(getCluster(), getTraitSet(), children.getFirst(), probeNodeEstimate, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return new OpenSearchBroadcastExchange(getCluster(), getTraitSet(), strippedChildren.getFirst(), probeNodeEstimate, viableBackends);
    }
}
