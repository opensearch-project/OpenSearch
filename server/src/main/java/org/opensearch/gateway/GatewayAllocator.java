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
import org.opensearch.common.UUIDs;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.set.Sets;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private static final long MAX_BATCH_SIZE = 2000; // will change it to a dynamic setting later

    private final RerouteService rerouteService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final PrimaryShardAllocator primaryBatchShardAllocator;
    private final ReplicaShardAllocator replicaBatchShardAllocator;
    private final TransportNodesListGatewayStartedShardsBatch batchStartedAction;
    private final TransportNodesListShardStoreMetadataBatch batchStoreAction;

    private final ConcurrentMap<
        ShardId,
        AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>> asyncFetchStarted = ConcurrentCollections
            .newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata>> asyncFetchStore =
        ConcurrentCollections.newConcurrentMap();
    private Set<String> lastSeenEphemeralIds = Collections.emptySet();
    private final ConcurrentMap<ShardId, String> startedShardBatchLookup = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<String, ShardsBatch> batchIdToStartedShardBatch = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<ShardId, String> storeShardBatchLookup = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<String, ShardsBatch> batchIdToStoreShardBatch = ConcurrentCollections.newConcurrentMap();

    @Inject
    public GatewayAllocator(
        RerouteService rerouteService,
        TransportNodesListGatewayStartedShards startedAction,
        TransportNodesListShardStoreMetadata storeAction,
        TransportNodesListGatewayStartedShardsBatch batchStartedAction,
        TransportNodesListShardStoreMetadataBatch batchStoreAction
    ) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(startedAction);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(storeAction);
        this.batchStartedAction = batchStartedAction;
        this.primaryBatchShardAllocator = new InternalPrimaryBatchShardAllocator(batchStartedAction);
        this.batchStoreAction = batchStoreAction;
        this.replicaBatchShardAllocator = new InternalReplicaBatchShardAllocator(batchStoreAction);
    }

    @Override
    public void cleanCaches() {
        Releasables.close(asyncFetchStarted.values());
        asyncFetchStarted.clear();
        Releasables.close(asyncFetchStore.values());
        asyncFetchStore.clear();
        batchIdToStartedShardBatch.clear();
        batchIdToStoreShardBatch.clear();
        startedShardBatchLookup.clear();
        storeShardBatchLookup.clear();
    }

    // for tests
    protected GatewayAllocator() {
        this.rerouteService = null;
        this.primaryShardAllocator = null;
        this.replicaShardAllocator = null;
        this.batchStartedAction = null;
        this.primaryBatchShardAllocator = null;
        this.batchStoreAction = null;
        this.replicaBatchShardAllocator = null;
    }

    @Override
    public int getNumberOfInFlightFetches() {
        boolean batchModeEnabled = true;
        int count = 0;
        if (batchModeEnabled) {
            for (ShardsBatch batch : batchIdToStartedShardBatch.values()) {
                count += batch.getNumberOfInFlightFetches();
            }
            for (ShardsBatch batch : batchIdToStoreShardBatch.values()) {
                count += batch.getNumberOfInFlightFetches();
            }
        }
        else {
            for (AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
                count += fetch.getNumberOfInFlightFetches();
            }
            for (AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch : asyncFetchStore.values()) {
                count += fetch.getNumberOfInFlightFetches();
            }
        }
        return count;
    }

    @Override
    public void applyStartedShards(final List<ShardRouting> startedShards, final RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchStarted.remove(startedShard.shardId()));
            Releasables.close(asyncFetchStore.remove(startedShard.shardId()));
            safelyRemoveShardFromBatch(startedShard);
        }
    }

    @Override
    public void applyFailedShards(final List<FailedShard> failedShards, final RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchStarted.remove(failedShard.getRoutingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.getRoutingEntry().shardId()));
            safelyRemoveShardFromBatch(failedShard.getRoutingEntry());
        }
    }

    @Override
    public void beforeAllocation(final RoutingAllocation allocation) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);
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

    @Override
    public void allocateUnassignedBatch(final RoutingAllocation allocation, boolean primary) {
        // create batches for unassigned shards
        createBatches(allocation, primary);

        assert primaryBatchShardAllocator != null;
        assert replicaBatchShardAllocator != null;
        if (primary) {
           batchIdToStartedShardBatch.values().forEach(shardsBatch -> primaryBatchShardAllocator.allocateUnassignedBatch(shardsBatch.getBatchedShardRoutings(), allocation));
        } else {
            batchIdToStoreShardBatch.values().forEach(batch -> replicaBatchShardAllocator.allocateUnassignedBatch(batch.getBatchedShardRoutings(), allocation));
        }
    }

    private void createBatches(RoutingAllocation allocation, boolean primary) {
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        // fetch all current batched shards
        Set<ShardId> currentBatchedShards = primary? startedShardBatchLookup.keySet() : storeShardBatchLookup.keySet();
        Set<ShardRouting> shardsToBatch = Sets.newHashSet();
        // add all unassigned shards to the batch if they are not already in a batch
        unassigned.forEach(shardRouting -> {
            if ((currentBatchedShards.contains(shardRouting.shardId()) == false) && (shardRouting.primary() == primary)) {
                assert shardRouting.unassigned();
                shardsToBatch.add(shardRouting);
            }
        });
        Iterator<ShardRouting> iterator = shardsToBatch.iterator();
        long batchSize = MAX_BATCH_SIZE;
        Map<ShardId, ShardBatchEntry> addToCurrentBatch = new HashMap<>();
        while (iterator.hasNext()) {
            ShardRouting currentShard = iterator.next();
            if (batchSize > 0) {
                ShardBatchEntry shardBatchEntry = new ShardBatchEntry(IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(currentShard.index()).getSettings())
                    , currentShard);
                addToCurrentBatch.put(currentShard.shardId(), shardBatchEntry);
                batchSize--;
                iterator.remove();
            }
            // add to batch if batch size full or last shard in unassigned list
            if (batchSize == 0 || iterator.hasNext() == false) {
                String batchUUId = UUIDs.base64UUID();
                ShardsBatch shardsBatch = new ShardsBatch(batchUUId, addToCurrentBatch, primary);
                // add the batch to list of current batches
                addBatch(shardsBatch, primary);
                addShardsIdsToLookup(addToCurrentBatch.keySet(), batchUUId, primary);
                addToCurrentBatch.clear();
                batchSize = MAX_BATCH_SIZE;
            }
        }
    }

    private void addBatch(ShardsBatch shardsBatch, boolean primary) {
        ConcurrentMap<String, ShardsBatch> batches = primary ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
        if (batches.containsKey(shardsBatch.getBatchId())) {
            throw new IllegalStateException("Batch already exists. BatchId = " + shardsBatch.getBatchId());
        }
        batches.put(shardsBatch.getBatchId(), shardsBatch);
    }

    private void addShardsIdsToLookup(Set<ShardId> shards, String batchId, boolean primary) {
        ConcurrentMap<ShardId, String> lookupMap = primary ? startedShardBatchLookup : storeShardBatchLookup;
        shards.forEach(shardId -> {
            if(lookupMap.containsKey(shardId)){
                throw new IllegalStateException("Shard is already Batched. ShardId = " + shardId + "Batch Id="+ lookupMap.get(shardId));
            }
            lookupMap.put(shardId, batchId);
        });
    }

    /**
     * Safely remove a shard from the appropriate batch.
     * If the shard is not in a batch, this is a no-op.
     * Cleans the batch if it is empty after removing the shard.
     * This method should be called when removing the shard from the batch instead {@link ShardsBatch#removeFromBatch(ShardRouting)}
     * so that we can clean up the batch if it is empty and release the fetching resources
     * @param shardRouting
     */
    private void safelyRemoveShardFromBatch(ShardRouting shardRouting) {
        String batchId = shardRouting.primary() ? startedShardBatchLookup.get(shardRouting.shardId()) : storeShardBatchLookup.get(shardRouting.shardId());
        if (batchId == null) {
            return;
        }
        ConcurrentMap<String, ShardsBatch> batches = shardRouting.primary() ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
        ShardsBatch batch = batches.get(batchId);
        batch.removeFromBatch(shardRouting);
        // remove the batch if it is empty
        if (batch.getBatchedShards().isEmpty()) {
            Releasables.close(batch.getAsyncFetcher());
            batches.remove(batchId);
        }
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

    class InternalBatchAsyncFetch<T extends BaseNodeResponse> extends AsyncBatchShardFetch<T> {

        InternalBatchAsyncFetch(Logger logger,
                                String type,
                                Map<ShardId, String> map,
                                AsyncBatchShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
                                String batchUUId
        ) {
            super(logger, type, map, action, batchUUId);
        }

        @Override
        protected void reroute(String batchUUId, String reason) {
            logger.trace("{} scheduling reroute for {}", batchUUId, reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "async_shard_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("{} scheduled reroute completed for {}", batchUUId, reason),
                    e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", batchUUId, reason), e)
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

    class InternalPrimaryBatchShardAllocator extends PrimaryShardAllocator {
        private final TransportNodesListGatewayStartedShardsBatch startedAction;

        InternalPrimaryBatchShardAllocator(TransportNodesListGatewayStartedShardsBatch startedAction) {
            this.startedAction = startedAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            return null;
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

    class InternalReplicaBatchShardAllocator extends ReplicaShardAllocator {

        private final TransportNodesListShardStoreMetadataBatch storeAction;

        InternalReplicaBatchShardAllocator(TransportNodesListShardStoreMetadataBatch storeAction) {
            this.storeAction = storeAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            return null;
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return false;
        }
    }

    /**
     * Holds information about a batch of shards to be allocated.
     * Async fetcher is used to fetch the data for the batch.
     */
    private class ShardsBatch {
        private final String batchId;
        boolean primary;

        private final AsyncBatchShardFetch<? extends BaseNodeResponse> asyncBatch;

        private final Map<ShardId, ShardBatchEntry> batchInfo;

        public ShardsBatch(String batchId, Map<ShardId, ShardBatchEntry> shardsWithInfo, boolean primary) {
            this.batchId = batchId;
            this.batchInfo = new HashMap<>(shardsWithInfo);
            // create a ShardId -> customDataPath map for async fetch
            Map<ShardId, String> shardIdsMap = batchInfo.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().getCustomDataPath()
            ));
            this.primary = primary;
            if (primary) {
                asyncBatch = new InternalBatchAsyncFetch<>(
                    logger,
                    "batch_shards_started",
                    shardIdsMap,
                    batchStartedAction,
                    batchId);
            } else {
                asyncBatch = new InternalBatchAsyncFetch<>(
                    logger,
                    "batch_shards_started",
                    shardIdsMap,
                    batchStoreAction,
                    batchId);

            }
        }

        private void removeFromBatch(ShardRouting shard) {

            batchInfo.remove(shard.shardId());
            asyncBatch.shardsToCustomDataPathMap.remove(shard.shardId());
            assert shard.primary() == primary : "Illegal call to delete shard from batch";
            // remove from lookup
            if (this.primary) {
                startedShardBatchLookup.remove(shard.shardId());
            } else {
                storeShardBatchLookup.remove(shard.shardId());
            }
            // assert that fetcher and shards are the same as batched shards
            assert batchInfo.size() == asyncBatch.shardsToCustomDataPathMap.size() : "Shards size is not equal to fetcher size";
        }

        Set<ShardRouting> getBatchedShardRoutings() {
            return batchInfo.values().stream().map(ShardBatchEntry::getShardRouting).collect(Collectors.toSet());
        }

        Set<ShardId> getBatchedShards() {
            return batchInfo.keySet();
        }

        public String getBatchId() {
            return batchId;
        }

        AsyncBatchShardFetch<? extends BaseNodeResponse> getAsyncFetcher() {
            return asyncBatch;
        }

        public int getNumberOfInFlightFetches() {
            return asyncBatch.getNumberOfInFlightFetches();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || o instanceof ShardsBatch == false) {
                return false;
            }
            ShardsBatch shardsBatch = (ShardsBatch) o;
            return batchId.equals(shardsBatch.getBatchId()) && batchInfo.keySet().equals(shardsBatch.getBatchedShards());
        }

        @Override
        public int hashCode() {
            return Objects.hash(batchId);
        }

        @Override
        public String toString() {
            return "batchId: " + batchId;
        }

    }

    /**
     * Holds information about a shard to be allocated in a batch.
     */
    private class ShardBatchEntry {

        private final String customDataPath;
        private final ShardRouting shardRouting;

        public ShardBatchEntry(String customDataPath, ShardRouting shardRouting) {
            this.customDataPath = customDataPath;
            this.shardRouting = shardRouting;
        }

        public ShardRouting getShardRouting() {
            return shardRouting;
        }

        public String getCustomDataPath() {
            return customDataPath;
        }
    }

}
