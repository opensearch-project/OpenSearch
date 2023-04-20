/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.set.Sets;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Allocator for the gateway
 *
 * @opensearch.internal
 */
public class GatewayAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "gateway_allocator";

    private static final Logger logger = LogManager.getLogger(GatewayAllocator.class);

    private final RerouteService rerouteService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final ConcurrentMap<
        ShardId,
        AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>> asyncFetchStarted = ConcurrentCollections
            .newConcurrentMap();
        private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata>> asyncFetchStore =
        ConcurrentCollections.newConcurrentMap();


    private Map<DiscoveryNode, TransportNodesCollectGatewayStartedShard.ListOfNodeGatewayStartedShards> shardsPerNode= ConcurrentCollections.newConcurrentMap();

    private AsyncShardsFetchPerNode<TransportNodesCollectGatewayStartedShard.ListOfNodeGatewayStartedShards> fetchShardsFromNodes=null;

    private Set<String> lastSeenEphemeralIds = Collections.emptySet();
    TransportNodesCollectGatewayStartedShard testAction;

    @Inject
    public GatewayAllocator(
        RerouteService rerouteService,
        TransportNodesListGatewayStartedShards startedAction,
        TransportNodesListShardStoreMetadata storeAction,
        TransportNodesCollectGatewayStartedShard testAction
    ) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new TestInternalPrimaryShardAllocator(testAction);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(storeAction);
        this.testAction=testAction;
    }

    @Override
    public void cleanCaches() {
        Releasables.close(asyncFetchStarted.values());
        asyncFetchStarted.clear();
        Releasables.close(asyncFetchStore.values());
        asyncFetchStore.clear();
        Releasables.close(fetchShardsFromNodes);
        shardsPerNode.clear();
    }

    // for tests
    protected GatewayAllocator() {
        this.rerouteService = null;
        this.primaryShardAllocator = null;
        this.replicaShardAllocator = null;
    }

    @Override
    public int getNumberOfInFlightFetches() {
        int count = 0;
        for (AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch : asyncFetchStore.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        return count;
    }

    @Override
    public void applyStartedShards(final List<ShardRouting> startedShards, final RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchStarted.remove(startedShard.shardId()));
            Releasables.close(asyncFetchStore.remove(startedShard.shardId()));
        }

        // clean async object and cache for per DiscoverNode if all shards are assigned and none are ignore list
        if (allocation.routingNodes().unassigned().isEmpty() && allocation.routingNodes().unassigned().isIgnoredEmpty()){
            Releasables.close(fetchShardsFromNodes);
            shardsPerNode.clear();
            fetchShardsFromNodes =null;
        }
    }

    @Override
    public void applyFailedShards(final List<FailedShard> failedShards, final RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchStarted.remove(failedShard.getRoutingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.getRoutingEntry().shardId()));
        }

        // clean async object and cache for per DiscoverNode if all shards are assigned and none are ignore list
        if (allocation.routingNodes().unassigned().isEmpty() && allocation.routingNodes().unassigned().isIgnoredEmpty()){
            Releasables.close(fetchShardsFromNodes);
            shardsPerNode.clear();
        }
    }

    @Override
    public void beforeAllocation(final RoutingAllocation allocation) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);

        //build the view of shards per node here by doing transport calls on nodes and populate shardsPerNode
        collectShardsPerNode(allocation);

    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        assert replicaShardAllocator != null;
        if (allocation.routingNodes().hasInactiveShards()) {
            // cancel existing recoveries if we have a better match
            replicaShardAllocator.processExistingRecoveries(allocation);
        }
    }

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        final RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator, shardRouting, unassignedAllocationHandler);
    }

    private synchronized Map<DiscoveryNode, TransportNodesCollectGatewayStartedShard.ListOfNodeGatewayStartedShards> collectShardsPerNode(RoutingAllocation allocation) {

        Map<ShardId, String> batchOfUnassignedShardsWithCustomDataPath = getBatchOfUnassignedShardsWithCustomDataPath(allocation);
        if (fetchShardsFromNodes == null) {
            if (batchOfUnassignedShardsWithCustomDataPath.isEmpty()){
                return null;
            }
            fetchShardsFromNodes = new TestAsyncShardFetch<>(logger, "collect_shards", batchOfUnassignedShardsWithCustomDataPath, testAction);
        } else {
            //verify if any new shards need to be batched?

            // even if one shard is not in the map, we now update the batch with all unassigned shards
            if (batchOfUnassignedShardsWithCustomDataPath.keySet().stream().allMatch(shard -> fetchShardsFromNodes.shardsToCustomDataPathMap.containsKey(shard)) == false) {
                // right now update the complete map, but this can be optimized with only the diff
                logger.info("Shards Batch not equal, updating it");
                if (fetchShardsFromNodes.shardsToCustomDataPathMap.keySet().equals(batchOfUnassignedShardsWithCustomDataPath.keySet()) == false) {
                    fetchShardsFromNodes.updateBatchOfShards(batchOfUnassignedShardsWithCustomDataPath);
                }
            }
        }

        AsyncShardsFetchPerNode.TestFetchResult<TransportNodesCollectGatewayStartedShard.ListOfNodeGatewayStartedShards> listOfNodeGatewayStartedShardsTestFetchResult = fetchShardsFromNodes.testFetchData(allocation.nodes());

        if (listOfNodeGatewayStartedShardsTestFetchResult.getNodesToShards()==null)
        {
            logger.info("Fetching probably still going on some nodes for number of shards={}, current fetch = {}",fetchShardsFromNodes.shardsToCustomDataPathMap.size(),fetchShardsFromNodes.cache.size());
            return null;
        }
        else {
            logger.info("Collecting of total shards ={}, over transport done", fetchShardsFromNodes.shardsToCustomDataPathMap.size());
            logger.info("Fetching from nodes done with size of nodes fetched= {}", listOfNodeGatewayStartedShardsTestFetchResult.getNodesToShards().size());
            // update the view for GatewayAllocator
            shardsPerNode = listOfNodeGatewayStartedShardsTestFetchResult.getNodesToShards();
            return shardsPerNode;
        }
    }

    private Map<ShardId, String> getBatchOfUnassignedShardsWithCustomDataPath(RoutingAllocation allocation){
        Map<ShardId, String> map = new HashMap<>();
        RoutingNodes.UnassignedShards allUnassignedShards = allocation.routingNodes().unassigned();
        for (ShardRouting shardIterator : allUnassignedShards) {
            if (shardIterator.primary())
                map.put(shardIterator.shardId(), IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shardIterator.index()).getSettings()));
        }
        return map;
    }

    // allow for testing infra to change shard allocators implementation
    protected static void innerAllocatedUnassigned(
        RoutingAllocation allocation,
        PrimaryShardAllocator primaryShardAllocator,
        ReplicaShardAllocator replicaShardAllocator,
        ShardRouting shardRouting,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert shardRouting.unassigned();

        if (shardRouting.primary()) {
            primaryShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        } else {
            replicaShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        assert unassignedShard.unassigned();
        assert routingAllocation.debugDecision();
        if (unassignedShard.primary()) {
            assert primaryShardAllocator != null;
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            assert replicaShardAllocator != null;
            return replicaShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        }
    }

    /**
     * Clear the fetched data for the primary to ensure we do not cancel recoveries based on excessively stale data.
     */
    private void ensureAsyncFetchStorePrimaryRecency(RoutingAllocation allocation) {
        DiscoveryNodes nodes = allocation.nodes();
        if (hasNewNodes(nodes)) {
            final Set<String> newEphemeralIds = StreamSupport.stream(nodes.getDataNodes().spliterator(), false)
                .map(node -> node.value.getEphemeralId())
                .collect(Collectors.toSet());
            // Invalidate the cache if a data node has been added to the cluster. This ensures that we do not cancel a recovery if a node
            // drops out, we fetch the shard data, then some indexing happens and then the node rejoins the cluster again. There are other
            // ways we could decide to cancel a recovery based on stale data (e.g. changing allocation filters or a primary failure) but
            // making the wrong decision here is not catastrophic so we only need to cover the common case.
            logger.trace(
                () -> new ParameterizedMessage(
                    "new nodes {} found, clearing primary async-fetch-store cache",
                    Sets.difference(newEphemeralIds, lastSeenEphemeralIds)
                )
            );
            asyncFetchStore.values().forEach(fetch -> clearCacheForPrimary(fetch, allocation));
            // recalc to also (lazily) clear out old nodes.
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    private static void clearCacheForPrimary(
        AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch,
        RoutingAllocation allocation
    ) {
        ShardRouting primary = allocation.routingNodes().activePrimary(fetch.shardId);
        if (primary != null) {
            fetch.clearCacheForNode(primary.currentNodeId());
        }
    }

    private boolean hasNewNodes(DiscoveryNodes nodes) {
        for (ObjectObjectCursor<String, DiscoveryNode> node : nodes.getDataNodes()) {
            if (lastSeenEphemeralIds.contains(node.value.getEphemeralId()) == false) {
                return true;
            }
        }
        return false;
    }

    class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        InternalAsyncFetch(
            Logger logger,
            String type,
            ShardId shardId,
            String customDataPath,
            Lister<? extends BaseNodesResponse<T>, T> action
        ) {
            super(logger, type, shardId, customDataPath, action);
        }

        @Override
        protected void reroute(ShardId shardId, String reason) {
            logger.trace("{} scheduling reroute for {}", shardId, reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "async_shard_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("{} scheduled reroute completed for {}", shardId, reason),
                    e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", shardId, reason), e)
                )
            );
        }
    }

    class TestAsyncShardFetch<T extends BaseNodeResponse> extends AsyncShardsFetchPerNode<T>
    {
        TestAsyncShardFetch(
            Logger logger,
            String type,
            Map<ShardId, String> map,
            AsyncShardsFetchPerNode.Lister<? extends BaseNodesResponse<T>, T> action
        ) {
            super(logger, type, map, action);
        }

        @Override
        protected void reroute( String reason) {
            logger.trace("TEST--scheduling reroute for {}", reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "TEST_async_shard_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("TEST-scheduled reroute completed for {}", reason),
                    e -> logger.debug(new ParameterizedMessage("TEST- scheduled reroute failed for {}",  reason), e)
                )
            );
        }
    }
    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final TransportNodesListGatewayStartedShards startedAction;

        InternalPrimaryShardAllocator(TransportNodesListGatewayStartedShards startedAction) {
            this.startedAction = startedAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(
            ShardRouting shard,
            RoutingAllocation allocation
        ) {
            AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch = asyncFetchStarted.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_started",
                    shardId,
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    startedAction
                )
            );
            AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> shardState = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId())
            );

            if (shardState.hasData()) {
                shardState.processAllocation(allocation);
            }
            return shardState;
        }
    }

    class TestInternalPrimaryShardAllocator extends PrimaryShardAllocator {
        private TransportNodesCollectGatewayStartedShard transportNodesCollectGatewayStartedShardAction;

        public TestInternalPrimaryShardAllocator(TransportNodesCollectGatewayStartedShard transportNodesCollectGatewayStartedShard) {
            this.transportNodesCollectGatewayStartedShardAction = transportNodesCollectGatewayStartedShard;
        }

        @Override
        // send the current view that gateway allocator has
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            ShardId shardId = shard.shardId();
            Map<DiscoveryNode, TransportNodesCollectGatewayStartedShard.ListOfNodeGatewayStartedShards> discoveryNodeListOfNodeGatewayStartedShardsMap = shardsPerNode;

            if (shardsPerNode.isEmpty()) {
                return new AsyncShardFetch.FetchResult<>(shardId, null, Collections.emptySet());
            }

            HashMap<DiscoveryNode, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> dataToAdapt = new HashMap<>();
            for (DiscoveryNode node : discoveryNodeListOfNodeGatewayStartedShardsMap.keySet()) {

                TransportNodesCollectGatewayStartedShard.ListOfNodeGatewayStartedShards shardsOnThatNode = discoveryNodeListOfNodeGatewayStartedShardsMap.get(node);
                if (shardsOnThatNode.getListOfNodeGatewayStartedShards().containsKey(shardId)) {
                    TransportNodesCollectGatewayStartedShard.NodeGatewayStartedShards nodeGatewayStartedShardsFromAdapt = shardsOnThatNode.getListOfNodeGatewayStartedShards().get(shardId);
                    // construct a object to adapt
                    TransportNodesListGatewayStartedShards.NodeGatewayStartedShards nodeGatewayStartedShardsToAdapt = new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(node, nodeGatewayStartedShardsFromAdapt.allocationId(),
                        nodeGatewayStartedShardsFromAdapt.primary(), nodeGatewayStartedShardsFromAdapt.replicationCheckpoint(), nodeGatewayStartedShardsFromAdapt.storeException());
                    dataToAdapt.put(node, nodeGatewayStartedShardsToAdapt);
                }
            }
            return new AsyncShardFetch.FetchResult<>(shardId, dataToAdapt, Collections.emptySet());
        }
    }


    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final TransportNodesListShardStoreMetadata storeAction;

        InternalReplicaShardAllocator(TransportNodesListShardStoreMetadata storeAction) {
            this.storeAction = storeAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(
            ShardRouting shard,
            RoutingAllocation allocation
        ) {
            AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch = asyncFetchStore.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_store",
                    shard.shardId(),
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    storeAction
                )
            );
            AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> shardStores = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId())
            );
            if (shardStores.hasData()) {
                shardStores.processAllocation(allocation);
            }
            return shardStores;
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return asyncFetchStore.get(shard.shardId()) != null;
        }
    }
}
