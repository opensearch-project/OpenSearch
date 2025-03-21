/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntroSorter;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardMovementStrategy;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.MoveDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.gateway.PriorityComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.opensearch.action.admin.indices.tiering.TieringUtils.isPartialShard;
import static org.opensearch.cluster.routing.ShardRoutingState.RELOCATING;

/**
 * A {@link LocalShardsBalancer} used by the {@link BalancedShardsAllocator} to perform allocation operations
 * for local shards within the cluster.
 *
 * @opensearch.internal
 */
public class LocalShardsBalancer extends ShardsBalancer {
    private final Logger logger;
    private final Map<String, BalancedShardsAllocator.ModelNode> nodes;
    private final RoutingAllocation allocation;
    private final RoutingNodes routingNodes;
    private final ShardMovementStrategy shardMovementStrategy;

    private final boolean preferPrimaryBalance;
    private final boolean preferPrimaryRebalance;

    private final boolean ignoreThrottleInRestore;
    private final BalancedShardsAllocator.WeightFunction weight;

    private final float threshold;
    private final Metadata metadata;

    private final float avgPrimaryShardsPerNode;
    private final BalancedShardsAllocator.NodeSorter sorter;
    private final Set<RoutingNode> inEligibleTargetNode;
    private final Supplier<Boolean> timedOutFunc;
    private int totalShardCount = 0;

    public LocalShardsBalancer(
        Logger logger,
        RoutingAllocation allocation,
        ShardMovementStrategy shardMovementStrategy,
        BalancedShardsAllocator.WeightFunction weight,
        float threshold,
        boolean preferPrimaryBalance,
        boolean preferPrimaryRebalance,
        boolean ignoreThrottleInRestore,
        Supplier<Boolean> timedOutFunc
    ) {
        this.logger = logger;
        this.allocation = allocation;
        this.weight = weight;
        this.threshold = threshold;
        this.routingNodes = allocation.routingNodes();
        this.metadata = allocation.metadata();
        avgPrimaryShardsPerNode = (float) (StreamSupport.stream(metadata.spliterator(), false)
            .mapToInt(IndexMetadata::getNumberOfShards)
            .sum()) / routingNodes.size();
        nodes = Collections.unmodifiableMap(buildModelFromAssigned());
        sorter = newNodeSorter();
        inEligibleTargetNode = new HashSet<>();
        this.preferPrimaryBalance = preferPrimaryBalance;
        this.preferPrimaryRebalance = preferPrimaryRebalance;
        this.shardMovementStrategy = shardMovementStrategy;
        this.ignoreThrottleInRestore = ignoreThrottleInRestore;
        this.timedOutFunc = timedOutFunc;
    }

    /**
     * Returns an array view on the nodes in the balancer. Nodes should not be removed from this list.
     */
    private BalancedShardsAllocator.ModelNode[] nodesArray() {
        return nodes.values().toArray(new BalancedShardsAllocator.ModelNode[0]);
    }

    /**
     * Returns the average of shards per node for the given index
     */
    @Override
    public float avgShardsPerNode(String index) {
        return ((float) metadata.index(index).getTotalNumberOfShards()) / nodes.size();
    }

    @Override
    public float avgPrimaryShardsPerNode(String index) {
        return ((float) metadata.index(index).getNumberOfShards()) / nodes.size();
    }

    @Override
    public float avgPrimaryShardsPerNode() {
        return avgPrimaryShardsPerNode;
    }

    /**
     * Returns the global average of shards per node
     */
    @Override
    public float avgShardsPerNode() {
        return totalShardCount / nodes.size();
    }

    /**
     * Returns a new {@link BalancedShardsAllocator.NodeSorter} that sorts the nodes based on their
     * current weight with respect to the index passed to the sorter. The
     * returned sorter is not sorted. Use {@link BalancedShardsAllocator.NodeSorter#reset(String)}
     * to sort based on an index.
     */
    private BalancedShardsAllocator.NodeSorter newNodeSorter() {
        return new BalancedShardsAllocator.NodeSorter(nodesArray(), weight, this);
    }

    /**
     * The absolute value difference between two weights.
     */
    private static float absDelta(float lower, float higher) {
        assert higher >= lower : higher + " lt " + lower + " but was expected to be gte";
        return Math.abs(higher - lower);
    }

    /**
     * Returns {@code true} iff the weight delta between two nodes is under a defined threshold.
     * See {@link BalancedShardsAllocator#THRESHOLD_SETTING} for defining the threshold.
     */
    private static boolean lessThan(float delta, float threshold) {
        /* deltas close to the threshold are "rounded" to the threshold manually
           to prevent floating point problems if the delta is very close to the
           threshold ie. 1.000000002 which can trigger unnecessary balance actions*/
        return delta <= (threshold + 0.001f);
    }

    /**
     * Balances the nodes on the cluster model according to the weight function.
     * The actual balancing is delegated to {@link #balanceByWeights()}
     */
    @Override
    void balance() {
        if (logger.isTraceEnabled()) {
            logger.trace("Start balancing cluster");
        }
        if (allocation.hasPendingAsyncFetch()) {
            /*
             * see https://github.com/elastic/elasticsearch/issues/14387
             * if we allow rebalance operations while we are still fetching shard store data
             * we might end up with unnecessary rebalance operations which can be super confusion/frustrating
             * since once the fetches come back we might just move all the shards back again.
             * Therefore we only do a rebalance if we have fetched all information.
             */
            logger.debug("skipping rebalance due to in-flight shard/store fetches");
            return;
        }
        if (allocation.deciders().canRebalance(allocation).type() != Decision.Type.YES) {
            logger.trace("skipping rebalance as it is disabled");
            return;
        }
        if (nodes.size() < 2) { /* skip if we only have one node */
            logger.trace("skipping rebalance as single node only");
            return;
        }
        balanceByWeights();
    }

    /**
     * Makes a decision about moving a single shard to a different node to form a more
     * optimally balanced cluster.  This method is invoked from the cluster allocation
     * explain API only.
     */
    @Override
    MoveDecision decideRebalance(final ShardRouting shard) {
        if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(shard, allocation))) {
            return MoveDecision.NOT_TAKEN;
        }

        if (shard.started() == false) {
            // we can only rebalance started shards
            return MoveDecision.NOT_TAKEN;
        }

        Decision canRebalance = allocation.deciders().canRebalance(shard, allocation);

        sorter.reset(shard.getIndexName());
        BalancedShardsAllocator.ModelNode[] modelNodes = sorter.modelNodes;
        final String currentNodeId = shard.currentNodeId();
        // find currently assigned node
        BalancedShardsAllocator.ModelNode currentNode = null;
        for (BalancedShardsAllocator.ModelNode node : modelNodes) {
            if (node.getNodeId().equals(currentNodeId)) {
                currentNode = node;
                break;
            }
        }
        assert currentNode != null : "currently assigned node could not be found";

        // balance the shard, if a better node can be found
        final String idxName = shard.getIndexName();
        final float currentWeight = weight.weight(this, currentNode, idxName);
        final AllocationDeciders deciders = allocation.deciders();
        Decision.Type rebalanceDecisionType = Decision.Type.NO;
        BalancedShardsAllocator.ModelNode assignedNode = null;
        List<Tuple<BalancedShardsAllocator.ModelNode, Decision>> betterBalanceNodes = new ArrayList<>();
        List<Tuple<BalancedShardsAllocator.ModelNode, Decision>> sameBalanceNodes = new ArrayList<>();
        List<Tuple<BalancedShardsAllocator.ModelNode, Decision>> worseBalanceNodes = new ArrayList<>();
        for (BalancedShardsAllocator.ModelNode node : modelNodes) {
            if (node == currentNode) {
                continue; // skip over node we're currently allocated to
            }
            final Decision canAllocate = deciders.canAllocate(shard, node.getRoutingNode(), allocation);
            // the current weight of the node in the cluster, as computed by the weight function;
            // this is a comparison of the number of shards on this node to the number of shards
            // that should be on each node on average (both taking the cluster as a whole into account
            // as well as shards per index)
            final float nodeWeight = weight.weightWithRebalanceConstraints(this, node, idxName);
            // if the node we are examining has a worse (higher) weight than the node the shard is
            // assigned to, then there is no way moving the shard to the node with the worse weight
            // can make the balance of the cluster better, so we check for that here
            final boolean betterWeightThanCurrent = nodeWeight <= currentWeight;
            boolean rebalanceConditionsMet = false;
            if (betterWeightThanCurrent) {
                // get the delta between the weights of the node we are checking and the node that holds the shard
                float currentDelta = absDelta(nodeWeight, currentWeight);
                // checks if the weight delta is above a certain threshold; if it is not above a certain threshold,
                // then even though the node we are examining has a better weight and may make the cluster balance
                // more even, it doesn't make sense to execute the heavyweight operation of relocating a shard unless
                // the gains make it worth it, as defined by the threshold
                boolean deltaAboveThreshold = lessThan(currentDelta, threshold) == false;
                // calculate the delta of the weights of the two nodes if we were to add the shard to the
                // node in question and move it away from the node that currently holds it.
                // hence we add 2.0f to the weight delta
                float proposedDelta = 2.0f + nodeWeight - currentWeight;
                boolean betterWeightWithShardAdded = proposedDelta < currentDelta;

                rebalanceConditionsMet = deltaAboveThreshold && betterWeightWithShardAdded;
                // if the simulated weight delta with the shard moved away is better than the weight delta
                // with the shard remaining on the current node, and we are allowed to allocate to the
                // node in question, then allow the rebalance
                if (rebalanceConditionsMet && canAllocate.type().higherThan(rebalanceDecisionType)) {
                    // rebalance to the node, only will get overwritten if the decision here is to
                    // THROTTLE and we get a decision with YES on another node
                    rebalanceDecisionType = canAllocate.type();
                    assignedNode = node;
                }
            }
            Tuple<BalancedShardsAllocator.ModelNode, Decision> nodeResult = Tuple.tuple(node, canAllocate);
            if (rebalanceConditionsMet) {
                betterBalanceNodes.add(nodeResult);
            } else if (betterWeightThanCurrent) {
                sameBalanceNodes.add(nodeResult);
            } else {
                worseBalanceNodes.add(nodeResult);
            }
        }

        int weightRanking = 0;
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>(modelNodes.length - 1);
        for (Tuple<BalancedShardsAllocator.ModelNode, Decision> result : betterBalanceNodes) {
            nodeDecisions.add(
                new NodeAllocationResult(
                    result.v1().getRoutingNode().node(),
                    AllocationDecision.fromDecisionType(result.v2().type()),
                    result.v2(),
                    ++weightRanking
                )
            );
        }
        int currentNodeWeightRanking = ++weightRanking;
        for (Tuple<BalancedShardsAllocator.ModelNode, Decision> result : sameBalanceNodes) {
            AllocationDecision nodeDecision = result.v2().type() == Decision.Type.NO
                ? AllocationDecision.NO
                : AllocationDecision.WORSE_BALANCE;
            nodeDecisions.add(
                new NodeAllocationResult(result.v1().getRoutingNode().node(), nodeDecision, result.v2(), currentNodeWeightRanking)
            );
        }
        for (Tuple<BalancedShardsAllocator.ModelNode, Decision> result : worseBalanceNodes) {
            AllocationDecision nodeDecision = result.v2().type() == Decision.Type.NO
                ? AllocationDecision.NO
                : AllocationDecision.WORSE_BALANCE;
            nodeDecisions.add(new NodeAllocationResult(result.v1().getRoutingNode().node(), nodeDecision, result.v2(), ++weightRanking));
        }

        if (canRebalance.type() != Decision.Type.YES || allocation.hasPendingAsyncFetch()) {
            AllocationDecision allocationDecision = allocation.hasPendingAsyncFetch()
                ? AllocationDecision.AWAITING_INFO
                : AllocationDecision.fromDecisionType(canRebalance.type());
            return MoveDecision.cannotRebalance(canRebalance, allocationDecision, currentNodeWeightRanking, nodeDecisions);
        } else {
            return MoveDecision.rebalance(
                canRebalance,
                AllocationDecision.fromDecisionType(rebalanceDecisionType),
                assignedNode != null ? assignedNode.getRoutingNode().node() : null,
                currentNodeWeightRanking,
                nodeDecisions
            );
        }
    }

    /**
     * Balances the nodes on the cluster model according to the weight
     * function. The configured threshold is the minimum delta between the
     * weight of the maximum node and the minimum node according to the
     * {@link BalancedShardsAllocator.WeightFunction}. This weight is calculated per index to
     * distribute shards evenly per index. The balancer tries to relocate
     * shards only if the delta exceeds the threshold. In the default case
     * the threshold is set to {@code 1.0} to enforce gaining relocation
     * only, or in other words relocations that move the weight delta closer
     * to {@code 0.0}
     */
    private void balanceByWeights() {
        final AllocationDeciders deciders = allocation.deciders();
        final BalancedShardsAllocator.ModelNode[] modelNodes = sorter.modelNodes;
        final float[] weights = sorter.weights;
        for (String index : buildWeightOrderedIndices()) {
            // Terminate if the time allocated to the balanced shards allocator has elapsed
            if (timedOutFunc != null && timedOutFunc.get()) {
                logger.info(
                    "Cannot balance any shard in the cluster as time allocated to balanced shards allocator has elapsed"
                        + ". Skipping indices iteration"
                );
                return;
            }
            IndexMetadata indexMetadata = metadata.index(index);

            // find nodes that have a shard of this index or where shards of this index are allowed to be allocated to,
            // move these nodes to the front of modelNodes so that we can only balance based on these nodes
            int relevantNodes = 0;
            for (int i = 0; i < modelNodes.length; i++) {
                BalancedShardsAllocator.ModelNode modelNode = modelNodes[i];
                if (modelNode.getIndex(index) != null
                    || deciders.canAllocate(indexMetadata, modelNode.getRoutingNode(), allocation).type() != Decision.Type.NO) {
                    // swap nodes at position i and relevantNodes
                    modelNodes[i] = modelNodes[relevantNodes];
                    modelNodes[relevantNodes] = modelNode;
                    relevantNodes++;
                }
            }

            if (relevantNodes < 2) {
                continue;
            }

            sorter.reset(index, 0, relevantNodes);
            int lowIdx = 0;
            int highIdx = relevantNodes - 1;
            while (true) {
                // break if the time allocated to the balanced shards allocator has elapsed
                if (timedOutFunc != null && timedOutFunc.get()) {
                    logger.info(
                        "Cannot balance any shard in the cluster as time allocated to balanced shards allocator has elapsed"
                            + ". Skipping relevant nodes iteration"
                    );
                    return;
                }
                final BalancedShardsAllocator.ModelNode minNode = modelNodes[lowIdx];
                final BalancedShardsAllocator.ModelNode maxNode = modelNodes[highIdx];
                advance_range: if (maxNode.numShards(index) > 0) {
                    final float delta = absDelta(weights[lowIdx], weights[highIdx]);
                    if (lessThan(delta, threshold)) {
                        if (lowIdx > 0
                            && highIdx - 1 > 0 // is there a chance for a higher delta?
                            && (absDelta(weights[0], weights[highIdx - 1]) > threshold) // check if we need to break at all
                        ) {
                            /* This is a special case if allocations from the "heaviest" to the "lighter" nodes is not possible
                             * due to some allocation decider restrictions like zone awareness. if one zone has for instance
                             * less nodes than another zone. so one zone is horribly overloaded from a balanced perspective but we
                             * can't move to the "lighter" shards since otherwise the zone would go over capacity.
                             *
                             * This break jumps straight to the condition below were we start moving from the high index towards
                             * the low index to shrink the window we are considering for balance from the other direction.
                             * (check shrinking the window from MAX to MIN)
                             * See #3580
                             */
                            break advance_range;
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace(
                                "Stop balancing index [{}]  min_node [{}] weight: [{}]" + "  max_node [{}] weight: [{}]  delta: [{}]",
                                index,
                                maxNode.getNodeId(),
                                weights[highIdx],
                                minNode.getNodeId(),
                                weights[lowIdx],
                                delta
                            );
                        }
                        break;
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Balancing from node [{}] weight: [{}] to node [{}] weight: [{}]  delta: [{}]",
                            maxNode.getNodeId(),
                            weights[highIdx],
                            minNode.getNodeId(),
                            weights[lowIdx],
                            delta
                        );
                    }
                    if (delta <= 1.0f) {
                        /*
                         * prevent relocations that only swap the weights of the two nodes. a relocation must bring us closer to the
                         * balance if we only achieve the same delta the relocation is useless
                         *
                         * NB this comment above was preserved from an earlier version but doesn't obviously describe the code today. We
                         * already know that lessThan(delta, threshold) == false and threshold defaults to 1.0, so by default we never
                         * hit this case anyway.
                         */
                        logger.trace(
                            "Couldn't find shard to relocate from node [{}] to node [{}]",
                            maxNode.getNodeId(),
                            minNode.getNodeId()
                        );
                    } else if (tryRelocateShard(minNode, maxNode, index)) {
                        /*
                         * TODO we could be a bit smarter here, we don't need to fully sort necessarily
                         * we could just find the place to insert linearly but the win might be minor
                         * compared to the added complexity
                         */
                        weights[lowIdx] = sorter.weight(modelNodes[lowIdx]);
                        weights[highIdx] = sorter.weight(modelNodes[highIdx]);
                        sorter.sort(0, relevantNodes);
                        lowIdx = 0;
                        highIdx = relevantNodes - 1;
                        continue;
                    }
                }
                if (lowIdx < highIdx - 1) {
                    /* Shrinking the window from MIN to MAX
                     * we can't move from any shard from the min node lets move on to the next node
                     * and see if the threshold still holds. We either don't have any shard of this
                     * index on this node of allocation deciders prevent any relocation.*/
                    lowIdx++;
                } else if (lowIdx > 0) {
                    /* Shrinking the window from MAX to MIN
                     * now we go max to min since obviously we can't move anything to the max node
                     * lets pick the next highest */
                    lowIdx = 0;
                    highIdx--;
                } else {
                    /* we are done here, we either can't relocate anymore or we are balanced */
                    break;
                }
            }
        }
    }

    /**
     * This builds a initial index ordering where the indices are returned
     * in most unbalanced first. We need this in order to prevent over
     * allocations on added nodes from one index when the weight parameters
     * for global balance overrule the index balance at an intermediate
     * state. For example this can happen if we have 3 nodes and 3 indices
     * with 3 primary and 1 replica shards. At the first stage all three nodes hold
     * 2 shard for each index. Now we add another node and the first index
     * is balanced moving three shards from two of the nodes over to the new node since it
     * has no shards yet and global balance for the node is way below
     * average. To re-balance we need to move shards back eventually likely
     * to the nodes we relocated them from.
     */
    private String[] buildWeightOrderedIndices() {

        final List<String> localIndices = new ArrayList<>();
        for (String index : allocation.routingTable().indicesRouting().keySet().toArray(new String[0])) {
            if (RoutingPool.LOCAL_ONLY.equals(RoutingPool.getIndexPool(metadata.index(index)))) {
                localIndices.add(index);
            }
        }
        final String[] indices = localIndices.toArray(new String[0]);

        final float[] deltas = new float[indices.length];
        for (int i = 0; i < deltas.length; i++) {
            sorter.reset(indices[i]);
            deltas[i] = sorter.delta();
        }
        new IntroSorter() {

            float pivotWeight;

            @Override
            protected void swap(int i, int j) {
                final String tmpIdx = indices[i];
                indices[i] = indices[j];
                indices[j] = tmpIdx;
                final float tmpDelta = deltas[i];
                deltas[i] = deltas[j];
                deltas[j] = tmpDelta;
            }

            @Override
            protected int compare(int i, int j) {
                return Float.compare(deltas[j], deltas[i]);
            }

            @Override
            protected void setPivot(int i) {
                pivotWeight = deltas[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Float.compare(deltas[j], pivotWeight);
            }
        }.sort(0, deltas.length);

        return indices;
    }

    /**
     * Checks if target node is ineligible and if so, adds to the list
     * of ineligible target nodes
     */
    private void checkAndAddInEligibleTargetNode(RoutingNode targetNode) {
        Decision nodeLevelAllocationDecision = allocation.deciders().canAllocateAnyShardToNode(targetNode, allocation);
        if (nodeLevelAllocationDecision.type() != Decision.Type.YES) {
            inEligibleTargetNode.add(targetNode);
        }
    }

    /**
     * Checks if the shard can be skipped from the local shard balancer operations
     * @param shardRouting the shard to be checked
     * @return true if the shard can be skipped, false otherwise
     */
    private boolean canShardBeSkipped(ShardRouting shardRouting) {
        return (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getShardPool(shardRouting, allocation))
            && !(FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG) && isPartialShard(shardRouting, allocation)));
    }

    /**
     * Move started shards that can not be allocated to a node anymore
     * <p>
     * For each shard to be moved this function executes a move operation
     * to the minimal eligible node with respect to the
     * weight function. If a shard is moved the shard will be set to
     * {@link ShardRoutingState#RELOCATING} and a shadow instance of this
     * shard is created with an incremented version in the state
     * {@link ShardRoutingState#INITIALIZING}.
     */
    @Override
    void moveShards() {
        // Iterate over the started shards interleaving between nodes, and check if they can remain. In the presence of throttling
        // shard movements, the goal of this iteration order is to achieve a fairer movement of shards from the nodes that are
        // offloading the shards.

        // Trying to eliminate target nodes so that we do not unnecessarily iterate over source nodes
        // when no target is eligible
        for (BalancedShardsAllocator.ModelNode currentNode : sorter.modelNodes) {
            checkAndAddInEligibleTargetNode(currentNode.getRoutingNode());
        }
        boolean primariesThrottled = false;
        for (Iterator<ShardRouting> it = allocation.routingNodes().nodeInterleavedShardIterator(shardMovementStrategy); it.hasNext();) {
            // Verify if the cluster concurrent recoveries have been reached.
            if (allocation.deciders().canMoveAnyShard(allocation).type() != Decision.Type.YES) {
                logger.info(
                    "Cannot move any shard in the cluster due to cluster concurrent recoveries getting breached"
                        + ". Skipping shard iteration"
                );
                return;
            }
            // Early terminate node interleaved shard iteration when no eligible target nodes are available
            if (sorter.modelNodes.length == inEligibleTargetNode.size()) {
                logger.info(
                    "Cannot move any shard in the cluster as there is no node on which shards can be allocated"
                        + ". Skipping shard iteration"
                );
                return;
            }

            // Terminate if the time allocated to the balanced shards allocator has elapsed
            if (timedOutFunc != null && timedOutFunc.get()) {
                logger.info(
                    "Cannot move any shard in the cluster as time allocated to balanced shards allocator has elapsed"
                        + ". Skipping shard iteration"
                );
                return;
            }

            ShardRouting shardRouting = it.next();

            if (canShardBeSkipped(shardRouting)) {
                continue;
            }

            // Ensure that replicas don't relocate if primaries are being throttled and primary first shard movement strategy is enabled
            if ((shardMovementStrategy == ShardMovementStrategy.PRIMARY_FIRST) && primariesThrottled && !shardRouting.primary()) {
                logger.info(
                    "Cannot move any replica shard in the cluster as movePrimaryFirst is enabled and primary shards"
                        + "are being throttled. Skipping shard iteration"
                );
                return;
            }

            // Verify if the shard is allowed to move if outgoing recovery on the node hosting the primary shard
            // is not being throttled.
            Decision canMoveAwayDecision = allocation.deciders().canMoveAway(shardRouting, allocation);
            if (canMoveAwayDecision.type() != Decision.Type.YES) {
                if (logger.isDebugEnabled()) logger.debug("Cannot move away shard [{}] Skipping this shard", shardRouting);
                if (shardRouting.primary() && canMoveAwayDecision.type() == Decision.Type.THROTTLE) {
                    primariesThrottled = true;
                }
                continue;
            }

            final MoveDecision moveDecision = decideMove(shardRouting);
            if (moveDecision.isDecisionTaken() && moveDecision.forceMove()) {
                final BalancedShardsAllocator.ModelNode sourceNode = nodes.get(shardRouting.currentNodeId());
                final BalancedShardsAllocator.ModelNode targetNode = nodes.get(moveDecision.getTargetNode().getId());
                sourceNode.removeShard(shardRouting);
                --totalShardCount;
                Tuple<ShardRouting, ShardRouting> relocatingShards = routingNodes.relocateShard(
                    shardRouting,
                    targetNode.getNodeId(),
                    allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    allocation.changes()
                );
                targetNode.addShard(relocatingShards.v2());
                ++totalShardCount;
                if (logger.isTraceEnabled()) {
                    logger.trace("Moved shard [{}] to node [{}]", shardRouting, targetNode.getRoutingNode());
                }

                // Verifying if this node can be considered ineligible for further iterations
                if (targetNode != null) {
                    checkAndAddInEligibleTargetNode(targetNode.getRoutingNode());
                }
            } else if (moveDecision.isDecisionTaken() && moveDecision.canRemain() == false) {
                logger.trace("[{}][{}] can't move", shardRouting.index(), shardRouting.id());
            }
        }
    }

    /**
     * Makes a decision on whether to move a started shard to another node.  The following rules apply
     * to the {@link MoveDecision} return object:
     *   1. If the shard is not started, no decision will be taken and {@link MoveDecision#isDecisionTaken()} will return false.
     *   2. If the shard is allowed to remain on its current node, no attempt will be made to move the shard and
     *      {@link MoveDecision#getCanRemainDecision} will have a decision type of YES.  All other fields in the object will be null.
     *   3. If the shard is not allowed to remain on its current node, then {@link MoveDecision#getAllocationDecision()} will be
     *      populated with the decision of moving to another node.  If {@link MoveDecision#forceMove()} ()} returns {@code true}, then
     *      {@link MoveDecision#getTargetNode} will return a non-null value, otherwise the assignedNodeId will be null.
     *   4. If the method is invoked in explain mode (e.g. from the cluster allocation explain APIs), then
     *      {@link MoveDecision#getNodeDecisions} will have a non-null value.
     */
    @Override
    MoveDecision decideMove(final ShardRouting shardRouting) {
        if (canShardBeSkipped(shardRouting)) {
            return MoveDecision.NOT_TAKEN;
        }

        if (shardRouting.started() == false) {
            // we can only move started shards
            return MoveDecision.NOT_TAKEN;
        }

        final boolean explain = allocation.debugDecision();
        final BalancedShardsAllocator.ModelNode sourceNode = nodes.get(shardRouting.currentNodeId());
        assert sourceNode != null && sourceNode.containsShard(shardRouting);
        RoutingNode routingNode = sourceNode.getRoutingNode();
        Decision canRemain = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
        if (canRemain.type() != Decision.Type.NO) {
            return MoveDecision.stay(canRemain);
        }

        sorter.reset(shardRouting.getIndexName());
        /*
         * the sorter holds the minimum weight node first for the shards index.
         * We now walk through the nodes until we find a node to allocate the shard.
         * This is not guaranteed to be balanced after this operation we still try best effort to
         * allocate on the minimal eligible node.
         */
        Decision.Type bestDecision = Decision.Type.NO;
        RoutingNode targetNode = null;
        final List<NodeAllocationResult> nodeExplanationMap = explain ? new ArrayList<>() : null;
        int weightRanking = 0;
        for (BalancedShardsAllocator.ModelNode currentNode : sorter.modelNodes) {
            if (currentNode != sourceNode) {
                RoutingNode target = currentNode.getRoutingNode();
                if (!explain && inEligibleTargetNode.contains(target)) continue;
                // don't use canRebalance as we want hard filtering rules to apply. See #17698
                if (!explain) {
                    // If we cannot allocate any shard to node marking it in eligible
                    Decision nodeLevelAllocationDecision = allocation.deciders().canAllocateAnyShardToNode(target, allocation);
                    if (nodeLevelAllocationDecision.type() != Decision.Type.YES) {
                        inEligibleTargetNode.add(currentNode.getRoutingNode());
                        continue;
                    }
                }
                // don't use canRebalance as we want hard filtering rules to apply. See #17698
                Decision allocationDecision = allocation.deciders().canAllocate(shardRouting, target, allocation);
                if (explain) {
                    nodeExplanationMap.add(
                        new NodeAllocationResult(currentNode.getRoutingNode().node(), allocationDecision, ++weightRanking)
                    );
                }
                // TODO maybe we can respect throttling here too?
                if (allocationDecision.type().higherThan(bestDecision)) {
                    bestDecision = allocationDecision.type();
                    if (bestDecision == Decision.Type.YES) {
                        targetNode = target;
                        if (explain == false) {
                            // we are not in explain mode and already have a YES decision on the best weighted node,
                            // no need to continue iterating
                            break;
                        }
                    }
                }
            }
        }

        return MoveDecision.cannotRemain(
            canRemain,
            AllocationDecision.fromDecisionType(bestDecision),
            targetNode != null ? targetNode.node() : null,
            nodeExplanationMap
        );
    }

    /**
     * Builds the internal model from all shards in the given
     * {@link Iterable}. All shards in the {@link Iterable} must be assigned
     * to a node. This method will skip shards in the state
     * {@link ShardRoutingState#RELOCATING} since each relocating shard has
     * a shadow shard in the state {@link ShardRoutingState#INITIALIZING}
     * on the target node which we respect during the allocation / balancing
     * process. In short, this method recreates the status-quo in the cluster.
     */
    private Map<String, BalancedShardsAllocator.ModelNode> buildModelFromAssigned() {
        Map<String, BalancedShardsAllocator.ModelNode> nodes = new HashMap<>();
        for (RoutingNode rn : routingNodes) {
            BalancedShardsAllocator.ModelNode node = new BalancedShardsAllocator.ModelNode(rn);
            nodes.put(rn.nodeId(), node);
            for (ShardRouting shard : rn) {
                assert rn.nodeId().equals(shard.currentNodeId());
                /* we skip relocating shards here since we expect an initializing shard with the same id coming in */
                if ((RoutingPool.LOCAL_ONLY.equals(RoutingPool.getShardPool(shard, allocation))
                    || (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG) && isPartialShard(shard, allocation)))
                    && shard.state() != RELOCATING) {
                    node.addShard(shard);
                    ++totalShardCount;
                    if (logger.isTraceEnabled()) {
                        logger.trace("Assigned shard [{}] to node [{}]", shard, node.getNodeId());
                    }
                }
            }
        }
        return nodes;
    }

    /**
     * Allocates all given shards on the minimal eligible node for the shards index
     * with respect to the weight function. All given shards must be unassigned.
     */
    @Override
    void allocateUnassigned() {
        RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
        assert !nodes.isEmpty();
        if (logger.isTraceEnabled()) {
            logger.trace("Start allocating unassigned shards");
        }
        if (unassigned.isEmpty()) {
            return;
        }

        /*
         * TODO: We could be smarter here and group the shards by index and then
         * use the sorter to save some iterations.
         */
        final PriorityComparator secondaryComparator = PriorityComparator.getAllocationComparator(allocation);
        final Comparator<ShardRouting> comparator = (o1, o2) -> {
            if (o1.primary() ^ o2.primary()) {
                // If one is primary and the other isn't, primary comes first
                return o1.primary() ? -1 : 1;
            }
            final int indexCmp;

            if ((indexCmp = o1.getIndexName().compareTo(o2.getIndexName())) == 0) {
                if (o1.isSearchOnly() ^ o2.isSearchOnly()) {
                    // Orders replicas first, followed by search replicas (e.g., R1, R1, S1, S1).
                    // This order is maintained because the logic that moves all replicas to unassigned
                    // when a replica cannot be allocated relies on this comparator.
                    // Ensures that a failed replica allocation does not block the allocation of a search replica.
                    return o1.isSearchOnly() ? 1 : -1;
                }

                // If both are primary or both are non-primary, compare by ID
                return o1.getId() - o2.getId();
            }
            // this comparator is more expensive than all the others up there
            // that's why it's added last even though it could be easier to read
            // if we'd apply it earlier. this comparator will only differentiate across
            // indices all shards of the same index is treated equally.
            final int secondary = secondaryComparator.compare(o1, o2);
            return secondary == 0 ? indexCmp : secondary;
        };
        /*
         * we use 2 arrays and move replicas to the second array once we allocated an identical
         * replica in the current iteration to make sure all indices get allocated in the same manner.
         * The arrays are sorted by primaries first and then by index and shard ID so a 2 indices with
         * 2 replica and 1 shard would look like:
         * [(0,P,IDX1), (0,P,IDX2), (0,R,IDX1), (0,R,IDX1), (0,R,IDX2), (0,R,IDX2)]
         * if we allocate for instance (0, R, IDX1) we move the second replica to the secondary array and proceed with
         * the next replica. If we could not find a node to allocate (0,R,IDX1) we move all it's replicas to ignoreUnassigned.
         */
        List<ShardRouting> primaryList = new ArrayList<>();
        for (ShardRouting shard : unassigned.drain()) {
            if (RoutingPool.LOCAL_ONLY.equals(RoutingPool.getShardPool(shard, allocation))) {
                primaryList.add(shard);
            } else {
                routingNodes.unassigned().add(shard);
            }
        }

        ShardRouting[] primary = primaryList.toArray(new ShardRouting[0]);
        ShardRouting[] secondary = new ShardRouting[primary.length];
        int secondaryLength = 0;
        int primaryLength = primary.length;
        ArrayUtil.timSort(primary, comparator);
        if (logger.isTraceEnabled()) {
            logger.trace("Staring allocation of [{}] unassigned shards", primaryLength);
        }
        do {
            for (int i = 0; i < primaryLength; i++) {
                if (timedOutFunc != null && timedOutFunc.get()) {
                    // TODO - maybe check if we can allow wait for active shards thingy bypass this condition
                    logger.info(
                        "Ignoring [{}] unassigned shards for allocation as time allocated to balanced shards allocator has elapsed",
                        (primaryLength - i)
                    );
                    while (i < primaryLength) {
                        unassigned.ignoreShard(primary[i], UnassignedInfo.AllocationStatus.NO_ATTEMPT, allocation.changes());
                        i++;
                    }
                    return;
                }
                ShardRouting shard = primary[i];
                final AllocateUnassignedDecision allocationDecision = decideAllocateUnassigned(shard);
                final String assignedNodeId = allocationDecision.getTargetNode() != null
                    ? allocationDecision.getTargetNode().getId()
                    : null;
                final BalancedShardsAllocator.ModelNode minNode = assignedNodeId != null ? nodes.get(assignedNodeId) : null;

                if (allocationDecision.getAllocationDecision() == AllocationDecision.YES) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Assigned shard [{}] to [{}]", shard, minNode.getNodeId());
                    }

                    final long shardSize = DiskThresholdDecider.getExpectedShardSize(
                        shard,
                        ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                        allocation.clusterInfo(),
                        allocation.snapshotShardSizeInfo(),
                        allocation.metadata(),
                        allocation.routingTable()
                    );
                    shard = routingNodes.initializeShard(shard, minNode.getNodeId(), null, shardSize, allocation.changes());
                    minNode.addShard(shard);
                    ++totalShardCount;
                    if (!shard.primary()) {
                        // copy over the same replica shards to the secondary array so they will get allocated
                        // in a subsequent iteration, allowing replicas of other shards to be allocated first
                        while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                            secondary[secondaryLength++] = primary[++i];
                        }
                    }
                } else {
                    // did *not* receive a YES decision
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "No eligible node found to assign shard [{}] allocation_status [{}]",
                            shard,
                            allocationDecision.getAllocationStatus()
                        );
                    }

                    if (minNode != null) {
                        // throttle decision scenario
                        assert allocationDecision.getAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED;
                        final long shardSize = DiskThresholdDecider.getExpectedShardSize(
                            shard,
                            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                            allocation.clusterInfo(),
                            allocation.snapshotShardSizeInfo(),
                            allocation.metadata(),
                            allocation.routingTable()
                        );
                        minNode.addShard(shard.initialize(minNode.getNodeId(), null, shardSize));
                        ++totalShardCount;
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace("No Node found to assign shard [{}]", shard);
                        }
                    }

                    unassigned.ignoreShard(shard, allocationDecision.getAllocationStatus(), allocation.changes());
                    if (!shard.primary()) { // we could not allocate it and we are a replica - check if we can ignore the other replicas
                        while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                            unassigned.ignoreShard(primary[++i], allocationDecision.getAllocationStatus(), allocation.changes());
                        }
                    }
                }
            }
            primaryLength = secondaryLength;
            ShardRouting[] tmp = primary;
            primary = secondary;
            secondary = tmp;
            secondaryLength = 0;
        } while (primaryLength > 0);
        // clear everything we have either added it or moved to ignoreUnassigned
    }

    /**
     * Make a decision for allocating an unassigned shard.  This method returns a two values in a tuple: the
     * first value is the {@link Decision} taken to allocate the unassigned shard, the second value is the
     * {@link BalancedShardsAllocator.ModelNode} representing the node that the shard should be assigned to.  If the decision returned
     * is of type {@link Decision.Type#NO}, then the assigned node will be null.
     */
    @Override
    AllocateUnassignedDecision decideAllocateUnassigned(final ShardRouting shard) {
        if (shard.assignedToNode()) {
            // we only make decisions for unassigned shards here
            return AllocateUnassignedDecision.NOT_TAKEN;
        }

        final boolean explain = allocation.debugDecision();
        Decision shardLevelDecision = allocation.deciders().canAllocate(shard, allocation);
        if (shardLevelDecision.type() == Decision.Type.NO && explain == false) {
            // NO decision for allocating the shard, irrespective of any particular node, so exit early
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.DECIDERS_NO, null);
        }

        /* find an node with minimal weight we can allocate on*/
        float minWeight = Float.POSITIVE_INFINITY;
        BalancedShardsAllocator.ModelNode minNode = null;
        Decision decision = null;
        /* Don't iterate over an identity hashset here the
         * iteration order is different for each run and makes testing hard */
        Map<String, NodeAllocationResult> nodeExplanationMap = explain ? new HashMap<>() : null;
        List<Tuple<String, Float>> nodeWeights = explain ? new ArrayList<>() : null;
        for (BalancedShardsAllocator.ModelNode node : nodes.values()) {
            if (node.containsShard(shard) && explain == false) {
                // decision is NO without needing to check anything further, so short circuit
                continue;
            }

            // weight of this index currently on the node
            float currentWeight = weight.weightWithAllocationConstraints(this, node, shard.getIndexName());
            // moving the shard would not improve the balance, and we are not in explain mode, so short circuit
            if (currentWeight > minWeight && explain == false) {
                continue;
            }

            Decision currentDecision = allocation.deciders().canAllocate(shard, node.getRoutingNode(), allocation);
            if (explain) {
                nodeExplanationMap.put(node.getNodeId(), new NodeAllocationResult(node.getRoutingNode().node(), currentDecision, 0));
                nodeWeights.add(Tuple.tuple(node.getNodeId(), currentWeight));
            }

            // For REMOTE_STORE recoveries, THROTTLE is as good as NO as we want faster recoveries
            // The side effect of this are increased relocations post these allocations.
            boolean considerThrottleAsNo = ignoreThrottleInRestore
                && shard.recoverySource().getType() == RecoverySource.Type.REMOTE_STORE
                && shard.primary();

            if (currentDecision.type() == Decision.Type.YES
                || (currentDecision.type() == Decision.Type.THROTTLE && considerThrottleAsNo == false)) {
                final boolean updateMinNode;
                if (currentWeight == minWeight) {
                    /*  we have an equal weight tie breaking:
                     *  1. if one decision is YES prefer it
                     *  2. prefer the node that holds the primary for this index with the next id in the ring ie.
                     *  for the 3 shards 2 replica case we try to build up:
                     *    1 2 0
                     *    2 0 1
                     *    0 1 2
                     *  such that if we need to tie-break we try to prefer the node holding a shard with the minimal id greater
                     *  than the id of the shard we need to assign. This works find when new indices are created since
                     *  primaries are added first and we only add one shard set a time in this algorithm.
                     */
                    if (currentDecision.type() == decision.type()) {
                        final int repId = shard.id();
                        final int nodeHigh = node.highestPrimary(shard.index().getName());
                        final int minNodeHigh = minNode.highestPrimary(shard.getIndexName());
                        updateMinNode = ((((nodeHigh > repId && minNodeHigh > repId) || (nodeHigh < repId && minNodeHigh < repId))
                            && (nodeHigh < minNodeHigh)) || (nodeHigh > repId && minNodeHigh < repId));
                    } else {
                        updateMinNode = currentDecision.type() == Decision.Type.YES;
                    }
                } else {
                    updateMinNode = currentWeight < minWeight;
                }
                if (updateMinNode) {
                    minNode = node;
                    minWeight = currentWeight;
                    decision = currentDecision;
                }
            }
        }
        if (decision == null) {
            // decision was not set and a node was not assigned, so treat it as a NO decision
            decision = Decision.NO;
        }
        List<NodeAllocationResult> nodeDecisions = null;
        if (explain) {
            nodeDecisions = new ArrayList<>();
            // fill in the correct weight ranking, once we've been through all nodes
            nodeWeights.sort((nodeWeight1, nodeWeight2) -> Float.compare(nodeWeight1.v2(), nodeWeight2.v2()));
            int weightRanking = 0;
            for (Tuple<String, Float> nodeWeight : nodeWeights) {
                NodeAllocationResult current = nodeExplanationMap.get(nodeWeight.v1());
                nodeDecisions.add(new NodeAllocationResult(current.getNode(), current.getCanAllocateDecision(), ++weightRanking));
            }
        }
        return AllocateUnassignedDecision.fromDecision(decision, minNode != null ? minNode.getRoutingNode().node() : null, nodeDecisions);
    }

    private static final Comparator<ShardRouting> BY_DESCENDING_SHARD_ID = Comparator.comparing(ShardRouting::shardId).reversed();
    private static final Comparator<ShardRouting> PRIMARY_FIRST = Comparator.comparing(ShardRouting::primary).reversed();

    /**
     * Tries to find a relocation from the max node to the minimal node for an arbitrary shard of the given index on the
     * balance model. Iff this method returns a <code>true</code> the relocation has already been executed on the
     * simulation model as well as on the cluster.
     */
    private boolean tryRelocateShard(BalancedShardsAllocator.ModelNode minNode, BalancedShardsAllocator.ModelNode maxNode, String idx) {
        final BalancedShardsAllocator.ModelIndex index = maxNode.getIndex(idx);
        if (index != null) {
            logger.trace("Try relocating shard of [{}] from [{}] to [{}]", idx, maxNode.getNodeId(), minNode.getNodeId());
            Stream<ShardRouting> routingStream = StreamSupport.stream(index.spliterator(), false)
                .filter(ShardRouting::started) // cannot rebalance unassigned, initializing or relocating shards anyway
                .filter(maxNode::containsShard) // check shards which are present on heaviest node
                .sorted(BY_DESCENDING_SHARD_ID); // check in descending order of shard id so that the decision is deterministic

            // If primary balance is preferred then prioritize moving primaries first
            if (preferPrimaryBalance == true) {
                routingStream = routingStream.sorted(PRIMARY_FIRST);
            }
            final Iterable<ShardRouting> shardRoutings = routingStream::iterator;
            final AllocationDeciders deciders = allocation.deciders();
            for (ShardRouting shard : shardRoutings) {
                final Decision rebalanceDecision = deciders.canRebalance(shard, allocation);
                if (rebalanceDecision.type() == Decision.Type.NO) {
                    continue;
                }
                final Decision allocationDecision = deciders.canAllocate(shard, minNode.getRoutingNode(), allocation);
                if (allocationDecision.type() == Decision.Type.NO) {
                    continue;
                }
                // This is a safety net which prevents un-necessary primary shard relocations from maxNode to minNode when
                // doing such relocation wouldn't help in primary balance. The condition won't be applicable when we enable node level
                // primary rebalance
                if (preferPrimaryBalance == true
                    && preferPrimaryRebalance == false
                    && shard.primary()
                    && maxNode.numPrimaryShards(shard.getIndexName()) - minNode.numPrimaryShards(shard.getIndexName()) < 2) {
                    continue;
                }
                // Relax the above condition to per node to allow rebalancing to attain global balance
                if (preferPrimaryRebalance == true && shard.primary() && maxNode.numPrimaryShards() - minNode.numPrimaryShards() < 2) {
                    continue;
                }
                final Decision decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecision);
                maxNode.removeShard(shard);
                --totalShardCount;
                long shardSize = allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

                if (decision.type() == Decision.Type.YES) {
                    /* only allocate on the cluster if we are not throttled */
                    logger.debug("Relocate [{}] from [{}] to [{}]", shard, maxNode.getNodeId(), minNode.getNodeId());
                    minNode.addShard(routingNodes.relocateShard(shard, minNode.getNodeId(), shardSize, allocation.changes()).v1());
                    ++totalShardCount;
                    return true;
                } else {
                    /* allocate on the model even if throttled */
                    logger.debug("Simulate relocation of [{}] from [{}] to [{}]", shard, maxNode.getNodeId(), minNode.getNodeId());
                    assert decision.type() == Decision.Type.THROTTLE;
                    minNode.addShard(shard.relocate(minNode.getNodeId(), shardSize));
                    ++totalShardCount;
                    return false;
                }
            }
        }
        logger.trace("No shards of [{}] can relocate from [{}] to [{}]", idx, maxNode.getNodeId(), minNode.getNodeId());
        return false;
    }

}
