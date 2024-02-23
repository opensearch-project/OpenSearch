/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Allocator for the gateway to assign batch of shards.
 *
 * @opensearch.internal
 */
public class ShardsBatchGatewayAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "shards_batch_gateway_allocator";

    private static final Logger logger = LogManager.getLogger(ShardsBatchGatewayAllocator.class);
    private final long maxBatchSize;
    private static final short DEFAULT_SHARD_BATCH_SIZE = 2000;

    /**
     * Number of shards we send in one batch to data nodes for fetching metadata
     */
    public static final Setting<Long> GATEWAY_ALLOCATOR_BATCH_SIZE = Setting.longSetting(
        "cluster.allocator.gateway.batch_size",
        DEFAULT_SHARD_BATCH_SIZE,
        1,
        10000,
        Setting.Property.NodeScope
    );

    private final RerouteService rerouteService;

    private PrimaryShardBatchAllocator primaryShardBatchAllocator;
    private ReplicaShardBatchAllocator replicaShardBatchAllocator;

    private Set<String> lastSeenEphemeralIds = Collections.emptySet();

    // visble for testing
    protected final ConcurrentMap<String, ShardsBatch> batchIdToStartedShardBatch = ConcurrentCollections.newConcurrentMap();

    // visible for testing
    protected final ConcurrentMap<String, ShardsBatch> batchIdToStoreShardBatch = ConcurrentCollections.newConcurrentMap();

    private final TransportNodesListGatewayStartedBatchShards batchStartedAction;
    private final TransportNodesListShardStoreMetadataBatch batchStoreAction;

    @Inject
    public ShardsBatchGatewayAllocator(
        RerouteService rerouteService,
        TransportNodesListGatewayStartedBatchShards batchStartedAction,
        TransportNodesListShardStoreMetadataBatch batchStoreAction,
        Settings settings
    ) {
        this.rerouteService = rerouteService;
        this.primaryShardBatchAllocator = new InternalPrimaryBatchShardAllocator();
        this.replicaShardBatchAllocator = new InternalReplicaBatchShardAllocator();
        this.batchStartedAction = batchStartedAction;
        this.batchStoreAction = batchStoreAction;
        this.maxBatchSize = GATEWAY_ALLOCATOR_BATCH_SIZE.get(settings);
    }

    @Override
    public void cleanCaches() {
        Releasables.close(
            batchIdToStartedShardBatch.values().stream().map(shardsBatch -> shardsBatch.asyncBatch).collect(Collectors.toList())
        );
        batchIdToStartedShardBatch.clear();
        Releasables.close(
            batchIdToStoreShardBatch.values().stream().map(shardsBatch -> shardsBatch.asyncBatch).collect(Collectors.toList())
        );
        batchIdToStoreShardBatch.clear();
    }

    // for tests
    protected ShardsBatchGatewayAllocator() {
        this.rerouteService = null;
        this.batchStartedAction = null;
        this.primaryShardBatchAllocator = null;
        this.batchStoreAction = null;
        this.replicaShardBatchAllocator = null;
        this.maxBatchSize = DEFAULT_SHARD_BATCH_SIZE;
    }

    // for tests

    @Override
    public int getNumberOfInFlightFetches() {
        int count = 0;
        for (ShardsBatch batch : batchIdToStartedShardBatch.values()) {
            count += (batch.getNumberOfInFlightFetches() * batch.getBatchedShards().size());
        }
        for (ShardsBatch batch : batchIdToStoreShardBatch.values()) {
            count += (batch.getNumberOfInFlightFetches() * batch.getBatchedShards().size());
        }

        return count;
    }

    @Override
    public void applyStartedShards(final List<ShardRouting> startedShards, final RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            safelyRemoveShardFromBothBatch(startedShard);
        }
    }

    @Override
    public void applyFailedShards(final List<FailedShard> failedShards, final RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            safelyRemoveShardFromBothBatch(failedShard.getRoutingEntry());
        }
    }

    @Override
    public void beforeAllocation(final RoutingAllocation allocation) {
        assert primaryShardBatchAllocator != null;
        assert replicaShardBatchAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);
    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        assert replicaShardBatchAllocator != null;
        List<Set<ShardRouting>> storedShardBatches = batchIdToStoreShardBatch.values()
            .stream()
            .map(ShardsBatch::getBatchedShardRoutings)
            .collect(Collectors.toList());
        if (allocation.routingNodes().hasInactiveShards()) {
            // cancel existing recoveries if we have a better match
            replicaShardBatchAllocator.processExistingRecoveries(allocation, storedShardBatches);
        }
    }

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        throw new UnsupportedOperationException("ShardsBatchGatewayAllocator does not support allocating unassigned shards");
    }

    @Override
    public void allocateAllUnassignedShards(final RoutingAllocation allocation, boolean primary) {

        assert primaryShardBatchAllocator != null;
        assert replicaShardBatchAllocator != null;
        innerAllocateUnassignedBatch(allocation, primaryShardBatchAllocator, replicaShardBatchAllocator, primary);
    }

    protected void innerAllocateUnassignedBatch(
        RoutingAllocation allocation,
        PrimaryShardBatchAllocator primaryBatchShardAllocator,
        ReplicaShardBatchAllocator replicaBatchShardAllocator,
        boolean primary
    ) {
        // create batches for unassigned shards
        Set<String> batchesToAssign = createAndUpdateBatches(allocation, primary);
        if (batchesToAssign.isEmpty()) {
            return;
        }
        if (primary) {
            batchIdToStartedShardBatch.values()
                .stream()
                .filter(batch -> batchesToAssign.contains(batch.batchId))
                .forEach(
                    shardsBatch -> primaryBatchShardAllocator.allocateUnassignedBatch(shardsBatch.getBatchedShardRoutings(), allocation)
                );
        } else {
            batchIdToStoreShardBatch.values()
                .stream()
                .filter(batch -> batchesToAssign.contains(batch.batchId))
                .forEach(batch -> replicaBatchShardAllocator.allocateUnassignedBatch(batch.getBatchedShardRoutings(), allocation));
        }
    }

    // visible for testing
    protected Set<String> createAndUpdateBatches(RoutingAllocation allocation, boolean primary) {
        Set<String> batchesToBeAssigned = new HashSet<>();
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        ConcurrentMap<String, ShardsBatch> currentBatches = primary ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
        // get all batched shards
        Set<ShardId> currentBatchedShards = currentBatches.values()
            .stream()
            .map(ShardsBatch::getBatchedShards)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

        Set<ShardRouting> shardsToBatch = Sets.newHashSet();
        // add all unassigned shards to the batch if they are not already in a batch
        unassigned.forEach(shardRouting -> {
            if ((currentBatchedShards.contains(shardRouting.shardId()) == false) && (shardRouting.primary() == primary)) {
                assert shardRouting.unassigned();
                shardsToBatch.add(shardRouting);
            }
            // if shard is already batched update to latest shardRouting information in the batches
            else if (shardRouting.primary() == primary) {
                String batchId = getBatchId(shardRouting, shardRouting.primary());
                batchesToBeAssigned.add(batchId);
                currentBatches.get(batchId).batchInfo.get(shardRouting.shardId()).setShardRouting(shardRouting);
            }
        });
        Iterator<ShardRouting> iterator = shardsToBatch.iterator();
        assert maxBatchSize > 0 : "Shards batch size must be greater than 0";

        long batchSize = maxBatchSize;
        Map<ShardId, ShardEntry> shardsToAddToCurrentBatch = new HashMap<>();
        while (iterator.hasNext()) {
            ShardRouting currentShard = iterator.next();
            if (batchSize > 0) {
                ShardEntry shardEntry = new ShardEntry(
                    new ShardAttributes(
                        currentShard.shardId(),
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(currentShard.index()).getSettings())
                    ),
                    currentShard
                );
                shardsToAddToCurrentBatch.put(currentShard.shardId(), shardEntry);
                batchSize--;
                iterator.remove();
            }
            // add to batch if batch size full or last shard in unassigned list
            if (batchSize == 0 || iterator.hasNext() == false) {
                String batchUUId = UUIDs.base64UUID();
                ShardsBatch shardsBatch = new ShardsBatch(batchUUId, shardsToAddToCurrentBatch, primary);
                // add the batch to list of current batches
                addBatch(shardsBatch, primary);
                batchesToBeAssigned.add(batchUUId);
                shardsToAddToCurrentBatch.clear();
                batchSize = maxBatchSize;
            }
        }
        return batchesToBeAssigned;
    }

    private void addBatch(ShardsBatch shardsBatch, boolean primary) {
        ConcurrentMap<String, ShardsBatch> batches = primary ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
        if (batches.containsKey(shardsBatch.getBatchId())) {
            throw new IllegalStateException("Batch already exists. BatchId = " + shardsBatch.getBatchId());
        }
        batches.put(shardsBatch.getBatchId(), shardsBatch);
    }

    /**
     * Safely remove a shard from the appropriate batch depending on if it is primary or replica
     * If the shard is not in a batch, this is a no-op.
     * Cleans the batch if it is empty after removing the shard.
     * This method should be called when removing the shard from the batch instead {@link ShardsBatch#removeFromBatch(ShardRouting)}
     * so that we can clean up the batch if it is empty and release the fetching resources
     *
     * @param shardRouting shard to be removed
     */
    protected void safelyRemoveShardFromBatch(ShardRouting shardRouting) {
        String batchId = shardRouting.primary() ? getBatchId(shardRouting, true) : getBatchId(shardRouting, false);
        if (batchId == null) {
            logger.debug("Shard[{}] is not batched", shardRouting);
            return;
        }
        ConcurrentMap<String, ShardsBatch> batches = shardRouting.primary() ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
        ShardsBatch batch = batches.get(batchId);
        batch.removeFromBatch(shardRouting);
        deleteBatchIfEmpty(batches, batchId);
    }

    /**
     * Safely remove shard from both the batches irrespective of its primary or replica,
     * For the corresponding shardId. The method intends to clean up the batch if it is empty
     * after removing the shard
     * @param shardRouting shard to remove
     */
    protected void safelyRemoveShardFromBothBatch(ShardRouting shardRouting) {
        String primaryBatchId = getBatchId(shardRouting, true);
        String replicaBatchId = getBatchId(shardRouting, false);
        if (primaryBatchId == null && replicaBatchId == null) {
            return;
        }
        if (primaryBatchId != null) {
            ShardsBatch batch = batchIdToStartedShardBatch.get(primaryBatchId);
            batch.removeFromBatch(shardRouting);
            deleteBatchIfEmpty(batchIdToStartedShardBatch, primaryBatchId);
        }
        if (replicaBatchId != null) {
            ShardsBatch batch = batchIdToStoreShardBatch.get(replicaBatchId);
            batch.removeFromBatch(shardRouting);
            deleteBatchIfEmpty(batchIdToStoreShardBatch, replicaBatchId);
        }
    }

    private void deleteBatchIfEmpty(ConcurrentMap<String, ShardsBatch> batches, String batchId) {
        if (batches.containsKey(batchId)) {
            ShardsBatch batch = batches.get(batchId);
            if (batch.getBatchedShards().isEmpty()) {
                Releasables.close(batch.getAsyncFetcher());
                batches.remove(batchId);
            }
        }
    }

    protected String getBatchId(ShardRouting shardRouting, boolean primary) {
        ConcurrentMap<String, ShardsBatch> batches = primary ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;

        return batches.entrySet()
            .stream()
            .filter(entry -> entry.getValue().getBatchedShards().contains(shardRouting.shardId()))
            .findFirst()
            .map(Map.Entry::getKey)
            .orElse(null);
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        assert unassignedShard.unassigned();
        assert routingAllocation.debugDecision();
        if (getBatchId(unassignedShard, unassignedShard.primary()) == null) {
            createAndUpdateBatches(routingAllocation, unassignedShard.primary());
        }
        assert getBatchId(unassignedShard, unassignedShard.primary()) != null;
        if (unassignedShard.primary()) {
            assert primaryShardBatchAllocator != null;
            return primaryShardBatchAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            assert replicaShardBatchAllocator != null;
            return replicaShardBatchAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        }
    }

    /**
     * Clear the fetched data for the primary to ensure we do not cancel recoveries based on excessively stale data.
     */
    private void ensureAsyncFetchStorePrimaryRecency(RoutingAllocation allocation) {
        DiscoveryNodes nodes = allocation.nodes();
        if (hasNewNodes(nodes)) {
            final Set<String> newEphemeralIds = StreamSupport.stream(Spliterators.spliterator(nodes.getDataNodes().entrySet(), 0), false)
                .map(node -> node.getValue().getEphemeralId())
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
            // ToDo : Validate that we don't need below call for batch allocation
            // storeShardBatchLookup.values().forEach(batch ->
            // clearCacheForBatchPrimary(batchIdToStoreShardBatch.get(batch), allocation)
            // );
            batchIdToStoreShardBatch.values().forEach(batch -> clearCacheForBatchPrimary(batch, allocation));

            // recalc to also (lazily) clear out old nodes.
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    private static void clearCacheForBatchPrimary(ShardsBatch batch, RoutingAllocation allocation) {
        // We're not running below code because for removing a node from cache we need all replica's primaries
        // to be assigned on same node. This was easy in single shard case and we're saving a call for a node
        // if primary was already assigned for a replica. But here we don't keep track of per shard data in cache
        // so it's not feasible to do any removal of node entry just based on single shard.
        // ONLY run if single shard is present in the batch, to maintain backward compatibility
        if (batch.getBatchedShards().size() == 1) {
            List<ShardRouting> primaries = batch.getBatchedShards()
                .stream()
                .map(allocation.routingNodes()::activePrimary)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            AsyncShardFetch<? extends BaseNodeResponse> fetch = batch.getAsyncFetcher();
            primaries.forEach(node -> fetch.clearCacheForNode(node.currentNodeId()));
        }
    }

    private boolean hasNewNodes(DiscoveryNodes nodes) {
        for (final DiscoveryNode node : nodes.getDataNodes().values()) {
            if (lastSeenEphemeralIds.contains(node.getEphemeralId()) == false) {
                return true;
            }
        }
        return false;
    }

    class InternalBatchAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {
        InternalBatchAsyncFetch(
            Logger logger,
            String type,
            Map<ShardId, ShardAttributes> map,
            AsyncShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
            String batchUUId
        ) {
            super(logger, type, map, action, batchUUId);
        }

        @Override
        protected void reroute(String reroutingKey, String reason) {
            logger.trace("{} scheduling reroute for {}", reroutingKey, reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "async_shard_batch_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("{} scheduled reroute completed for {}", reroutingKey, reason),
                    e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", reroutingKey, reason), e)
                )
            );
        }
    }

    class InternalPrimaryBatchShardAllocator extends PrimaryShardBatchAllocator {

        @Override
        @SuppressWarnings("unchecked")
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShardsBatch> fetchData(
            Set<ShardRouting> shardsEligibleForFetch,
            Set<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            ShardRouting shardRouting = shardsEligibleForFetch.iterator().hasNext() ? shardsEligibleForFetch.iterator().next() : null;
            shardRouting = shardRouting == null && inEligibleShards.iterator().hasNext()
                ? inEligibleShards.iterator().next()
                : shardRouting;
            if (shardRouting == null) {
                return new AsyncShardFetch.FetchResult<>(null, Collections.emptyMap());
            }
            String batchId = getBatchId(shardRouting, shardRouting.primary());
            if (batchId == null) {
                logger.debug("Shard {} has no batch id", shardRouting);
                throw new IllegalStateException("Shard " + shardRouting + " has no batch id. Shard should batched before fetching");
            }

            if (batchIdToStartedShardBatch.containsKey(batchId) == false) {
                logger.debug("Batch {} has no started shard batch", batchId);
                throw new IllegalStateException("Batch " + batchId + " has no started shard batch");
            }

            ShardsBatch shardsBatch = batchIdToStartedShardBatch.get(batchId);
            // remove in eligible shards which allocator is not responsible for
            inEligibleShards.forEach(ShardsBatchGatewayAllocator.this::safelyRemoveShardFromBatch);

            if (shardsBatch.getBatchedShards().isEmpty() && shardsEligibleForFetch.isEmpty()) {
                logger.debug("Batch {} is empty", batchId);
                return new AsyncShardFetch.FetchResult<>(null, Collections.emptyMap());
            }

            Map<ShardId, Set<String>> shardToIgnoreNodes = new HashMap<>();

            for (ShardId shardId : shardsBatch.asyncBatch.shardAttributesMap.keySet()) {
                shardToIgnoreNodes.put(shardId, allocation.getIgnoreNodes(shardId));
            }
            AsyncShardFetch<? extends BaseNodeResponse> asyncFetcher = shardsBatch.getAsyncFetcher();
            AsyncShardFetch.FetchResult<? extends BaseNodeResponse> shardBatchState = asyncFetcher.fetchData(
                allocation.nodes(),
                shardToIgnoreNodes
            );

            if (shardBatchState.hasData()) {
                shardBatchState.processAllocation(allocation);
            }
            return (AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedBatchShards.NodeGatewayStartedShardsBatch>) shardBatchState;
        }

    }

    class InternalReplicaBatchShardAllocator extends ReplicaShardBatchAllocator {
        @Override
        @SuppressWarnings("unchecked")
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch> fetchData(
            Set<ShardRouting> shardsEligibleForFetch,
            Set<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            // get batch id for anyone given shard. We are assuming all shards will have same batch Id
            ShardRouting shardRouting = shardsEligibleForFetch.iterator().hasNext() ? shardsEligibleForFetch.iterator().next() : null;
            shardRouting = shardRouting == null && inEligibleShards.iterator().hasNext()
                ? inEligibleShards.iterator().next()
                : shardRouting;
            if (shardRouting == null) {
                return new AsyncShardFetch.FetchResult<>(null, Collections.emptyMap());
            }
            String batchId = getBatchId(shardRouting, shardRouting.primary());
            if (batchId == null) {
                logger.debug("Shard {} has no batch id", shardRouting);
                throw new IllegalStateException("Shard " + shardRouting + " has no batch id. Shard should batched before fetching");
            }

            if (batchIdToStoreShardBatch.containsKey(batchId) == false) {
                logger.debug("Batch {} has no store shard batch", batchId);
                throw new IllegalStateException("Batch " + batchId + " has no shard store batch");
            }

            ShardsBatch shardsBatch = batchIdToStoreShardBatch.get(batchId);
            // remove in eligible shards which allocator is not responsible for
            inEligibleShards.forEach(ShardsBatchGatewayAllocator.this::safelyRemoveShardFromBatch);

            if (shardsBatch.getBatchedShards().isEmpty() && shardsEligibleForFetch.isEmpty()) {
                logger.debug("Batch {} is empty", batchId);
                return new AsyncShardFetch.FetchResult<>(null, Collections.emptyMap());
            }
            Map<ShardId, Set<String>> shardToIgnoreNodes = new HashMap<>();
            for (ShardId shardId : shardsBatch.asyncBatch.shardAttributesMap.keySet()) {
                shardToIgnoreNodes.put(shardId, allocation.getIgnoreNodes(shardId));
            }
            AsyncShardFetch<? extends BaseNodeResponse> asyncFetcher = shardsBatch.getAsyncFetcher();
            AsyncShardFetch.FetchResult<? extends BaseNodeResponse> shardBatchStores = asyncFetcher.fetchData(
                allocation.nodes(),
                shardToIgnoreNodes
            );
            if (shardBatchStores.hasData()) {
                shardBatchStores.processAllocation(allocation);
            }
            return (AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch>) shardBatchStores;
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            String batchId = getBatchId(shard, shard.primary());
            return batchId != null;
        }
    }

    /**
     * Holds information about a batch of shards to be allocated.
     * Async fetcher is used to fetch the data for the batch.
     *
     * Visible for testing
     */
    public class ShardsBatch {
        private final String batchId;
        private final boolean primary;

        private final AsyncShardFetch<? extends BaseNodeResponse> asyncBatch;

        private final Map<ShardId, ShardEntry> batchInfo;

        public ShardsBatch(String batchId, Map<ShardId, ShardEntry> shardsWithInfo, boolean primary) {
            this.batchId = batchId;
            this.batchInfo = new HashMap<>(shardsWithInfo);
            // create a ShardId -> customDataPath map for async fetch
            Map<ShardId, ShardAttributes> shardIdsMap = batchInfo.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getShardAttributes()));
            this.primary = primary;
            if (primary) {
                asyncBatch = new InternalBatchAsyncFetch<>(logger, "batch_shards_started", shardIdsMap, batchStartedAction, batchId);
            } else {
                asyncBatch = new InternalBatchAsyncFetch<>(logger, "batch_shards_started", shardIdsMap, batchStoreAction, batchId);

            }
        }

        private void removeFromBatch(ShardRouting shard) {
            batchInfo.remove(shard.shardId());
            asyncBatch.shardAttributesMap.remove(shard.shardId());
            // assert that fetcher and shards are the same as batched shards
            assert batchInfo.size() == asyncBatch.shardAttributesMap.size() : "Shards size is not equal to fetcher size";
        }

        public Set<ShardRouting> getBatchedShardRoutings() {
            return batchInfo.values().stream().map(ShardEntry::getShardRouting).collect(Collectors.toSet());
        }

        public Set<ShardId> getBatchedShards() {
            return batchInfo.keySet();
        }

        public String getBatchId() {
            return batchId;
        }

        public AsyncShardFetch<? extends BaseNodeResponse> getAsyncFetcher() {
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
    private class ShardEntry {

        private final ShardAttributes shardAttributes;

        public ShardEntry setShardRouting(ShardRouting shardRouting) {
            this.shardRouting = shardRouting;
            return this;
        }

        private ShardRouting shardRouting;

        public ShardEntry(ShardAttributes shardAttributes, ShardRouting shardRouting) {
            this.shardAttributes = shardAttributes;
            this.shardRouting = shardRouting;
        }

        public ShardRouting getShardRouting() {
            return shardRouting;
        }

        public ShardAttributes getShardAttributes() {
            return shardAttributes;
        }
    }

    public int getNumberOfStartedShardBatches() {
        return batchIdToStoreShardBatch.size();
    }

    public int getNumberOfStoreShardBatches() {
        return batchIdToStoreShardBatch.size();
    }
}
