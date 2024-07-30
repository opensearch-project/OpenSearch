/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.AsyncShardFetch.FetchResult;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Allocates replica shards in a batch mode
 *
 * @opensearch.internal
 */
public abstract class ReplicaShardBatchAllocator extends ReplicaShardAllocator {

    /**
     * Process existing recoveries of replicas and see if we need to cancel them if we find a better
     * match. Today, a better match is one that can perform a no-op recovery while the previous recovery
     * has to copy segment files.
     *
     * @param allocation   the overall routing allocation
     * @param shardBatches a list of shard batches to check for existing recoveries
     */
    public void processExistingRecoveries(RoutingAllocation allocation, List<List<ShardRouting>> shardBatches) {
        List<Runnable> shardCancellationActions = new ArrayList<>();
        // iterate through the batches, each batch needs to be processed together as fetch call should be made for shards from same batch
        for (List<ShardRouting> shardBatch : shardBatches) {
            List<ShardRouting> eligibleShards = new ArrayList<>();
            List<ShardRouting> ineligibleShards = new ArrayList<>();
            // iterate over shards to check for match for each of those
            for (ShardRouting shard : shardBatch) {
                if (shard != null && !shard.primary()) {
                    // need to iterate over all the nodes to find matching shard
                    if (shouldSkipFetchForRecovery(shard)) {
                        // shard should just be skipped for fetchData, no need to remove from batch
                        continue;
                    }
                    eligibleShards.add(shard);
                }
            }
            AsyncShardFetch.FetchResult<NodeStoreFilesMetadataBatch> shardState = fetchData(eligibleShards, ineligibleShards, allocation);
            if (!shardState.hasData()) {
                logger.trace("{}: fetching new stores for initializing shard batch", eligibleShards);
                continue; // still fetching
            }
            for (ShardRouting shard : eligibleShards) {
                Map<DiscoveryNode, StoreFilesMetadata> nodeShardStores = convertToNodeStoreFilesMetadataMap(shard, shardState);

                Runnable cancellationAction = cancelExistingRecoveryForBetterMatch(shard, allocation, nodeShardStores);
                if (cancellationAction != null) {
                    shardCancellationActions.add(cancellationAction);
                }
            }
        }
        for (Runnable action : shardCancellationActions) {
            action.run();
        }
    }

    abstract protected FetchResult<NodeStoreFilesMetadataBatch> fetchData(
        List<ShardRouting> eligibleShards,
        List<ShardRouting> ineligibleShards,
        RoutingAllocation allocation
    );

    @Override
    protected FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(
        ShardRouting shard,
        RoutingAllocation allocation
    ) {
        logger.error("fetchData for single shard called via batch allocator");
        throw new IllegalStateException("ReplicaShardBatchAllocator should only be used for a batch of shards");
    }

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard, RoutingAllocation allocation, Logger logger) {
        Supplier<Map<DiscoveryNode, StoreFilesMetadata>> fetchDataResultSupplier = () -> {
            return convertToNodeStoreFilesMetadataMap(
                unassignedShard,
                fetchData(List.of(unassignedShard), Collections.emptyList(), allocation)
            );
        };
        return getUnassignedShardAllocationDecision(unassignedShard, allocation, fetchDataResultSupplier);
    }

    /**
     * Allocate Batch of unassigned shard  to nodes where valid copies of the shard already exists
     *
     * @param shardRoutings the shards to allocate
     * @param allocation    the allocation state container object
     */
    public void allocateUnassignedBatch(List<ShardRouting> shardRoutings, RoutingAllocation allocation) {
        logger.trace("Starting shard allocation execution for unassigned replica shards: {}", shardRoutings.size());
        List<ShardRouting> eligibleShards = new ArrayList<>();
        List<ShardRouting> ineligibleShards = new ArrayList<>();
        Map<ShardRouting, AllocateUnassignedDecision> ineligibleShardAllocationDecisions = new HashMap<>();

        for (ShardRouting shard : shardRoutings) {
            AllocateUnassignedDecision shardDecisionWithoutFetch = getUnassignedShardAllocationDecision(shard, allocation, null);
            // Without fetchData, decision for in-eligible shards is non-null from our preliminary checks and null for eligible shards.
            if (shardDecisionWithoutFetch != null) {
                ineligibleShards.add(shard);
                ineligibleShardAllocationDecisions.put(shard, shardDecisionWithoutFetch);
            } else {
                eligibleShards.add(shard);
            }
        }

        // only fetch data for eligible shards
        final FetchResult<NodeStoreFilesMetadataBatch> shardsState = fetchData(eligibleShards, ineligibleShards, allocation);

        Set<ShardId> shardIdsFromBatch = new HashSet<>();
        for (ShardRouting shardRouting : shardRoutings) {
            ShardId shardId = shardRouting.shardId();
            shardIdsFromBatch.add(shardId);
        }
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            ShardRouting unassignedShard = iterator.next();
            // There will be only one entry for the shard in the unassigned shards batch
            // for a shard with multiple unassigned replicas, hence we are comparing the shard ids
            // instead of ShardRouting in-order to evaluate shard assignment for all unassigned replicas of a shard.
            if (!unassignedShard.primary() && shardIdsFromBatch.contains(unassignedShard.shardId())) {
                AllocateUnassignedDecision allocateUnassignedDecision;
                if (ineligibleShardAllocationDecisions.containsKey(unassignedShard)) {
                    allocateUnassignedDecision = ineligibleShardAllocationDecisions.get(unassignedShard);
                } else {
                    // The shard's eligibility is being recomputed again as
                    // the routing allocation state is updated during shard allocation decision execution
                    // because of which allocation eligibility of other unassigned shards can change.
                    allocateUnassignedDecision = getUnassignedShardAllocationDecision(
                        unassignedShard,
                        allocation,
                        () -> convertToNodeStoreFilesMetadataMap(unassignedShard, shardsState)
                    );
                }
                executeDecision(unassignedShard, allocateUnassignedDecision, allocation, iterator);
            }
        }
        logger.trace("Finished shard allocation execution for unassigned replica shards: {}", shardRoutings.size());
    }

    private AllocateUnassignedDecision getUnassignedShardAllocationDecision(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        Supplier<Map<DiscoveryNode, StoreFilesMetadata>> nodeStoreFileMetaDataMapSupplier
    ) {
        if (!isResponsibleFor(shardRouting)) {
            return AllocateUnassignedDecision.NOT_TAKEN;
        }
        Tuple<Decision, Map<String, NodeAllocationResult>> result = canBeAllocatedToAtLeastOneNode(shardRouting, allocation);

        final boolean explain = allocation.debugDecision();
        Decision allocationDecision = result.v1();
        if (allocationDecision.type() != Decision.Type.YES && (!explain || !hasInitiatedFetching(shardRouting))) {
            // only return early if we are not in explain mode, or we are in explain mode but we have not
            // yet attempted to fetch any shard data
            logger.trace("{}: ignoring allocation, can't be allocated on any node. Decision: {}", shardRouting, allocationDecision.type());
            return AllocateUnassignedDecision.no(
                UnassignedInfo.AllocationStatus.fromDecision(allocationDecision.type()),
                result.v2() != null ? new ArrayList<>(result.v2().values()) : null
            );
        }
        if (nodeStoreFileMetaDataMapSupplier != null) {
            Map<DiscoveryNode, StoreFilesMetadata> discoveryNodeStoreFilesMetadataMap = nodeStoreFileMetaDataMapSupplier.get();
            return getAllocationDecision(shardRouting, allocation, discoveryNodeStoreFilesMetadataMap, result, logger);
        }
        return null;
    }

    private Map<DiscoveryNode, StoreFilesMetadata> convertToNodeStoreFilesMetadataMap(
        ShardRouting unassignedShard,
        FetchResult<NodeStoreFilesMetadataBatch> data
    ) {
        if (!data.hasData()) {
            return null;
        }

        Map<DiscoveryNode, StoreFilesMetadata> map = new HashMap<>();

        data.getData().forEach((discoveryNode, value) -> {
            Map<ShardId, NodeStoreFilesMetadata> batch = value.getNodeStoreFilesMetadataBatch();
            NodeStoreFilesMetadata metadata = batch.get(unassignedShard.shardId());
            if (metadata != null) {
                map.put(discoveryNode, metadata.storeFilesMetadata());
            }
        });

        return map;
    }
}
