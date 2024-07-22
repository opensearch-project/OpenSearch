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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BatchRunnableExecutor;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.TimeoutAwareRunnable;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper.GatewayStartedShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.store.ShardAttributes;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataHelper;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata;

import java.util.ArrayList;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    private static final String PRIMARY_BATCH_ALLOCATOR_TIMEOUT_SETTING_KEY =
        "cluster.routing.allocation.shards_batch_gateway_allocator.primary_allocator_timeout";
    private static final String REPLICA_BATCH_ALLOCATOR_TIMEOUT_SETTING_KEY =
        "cluster.routing.allocation.shards_batch_gateway_allocator.replica_allocator_timeout";

    private TimeValue primaryShardsBatchGatewayAllocatorTimeout;
    private TimeValue replicaShardsBatchGatewayAllocatorTimeout;

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

    public static final Setting<TimeValue> PRIMARY_BATCH_ALLOCATOR_TIMEOUT_SETTING = Setting.timeSetting(
        PRIMARY_BATCH_ALLOCATOR_TIMEOUT_SETTING_KEY,
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> REPLICA_BATCH_ALLOCATOR_TIMEOUT_SETTING = Setting.timeSetting(
        REPLICA_BATCH_ALLOCATOR_TIMEOUT_SETTING_KEY,
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final RerouteService rerouteService;
    private final PrimaryShardBatchAllocator primaryShardBatchAllocator;
    private final ReplicaShardBatchAllocator replicaShardBatchAllocator;
    private Set<String> lastSeenEphemeralIds = Collections.emptySet();

    // visible for testing
    protected final ConcurrentMap<String, ShardsBatch> batchIdToStartedShardBatch = ConcurrentCollections.newConcurrentMap();

    // visible for testing
    protected final ConcurrentMap<String, ShardsBatch> batchIdToStoreShardBatch = ConcurrentCollections.newConcurrentMap();
    private final TransportNodesListGatewayStartedShardsBatch batchStartedAction;
    private final TransportNodesListShardStoreMetadataBatch batchStoreAction;

    @Inject
    public ShardsBatchGatewayAllocator(
        RerouteService rerouteService,
        TransportNodesListGatewayStartedShardsBatch batchStartedAction,
        TransportNodesListShardStoreMetadataBatch batchStoreAction,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        this.rerouteService = rerouteService;
        this.primaryShardBatchAllocator = new InternalPrimaryBatchShardAllocator();
        this.replicaShardBatchAllocator = new InternalReplicaBatchShardAllocator();
        this.batchStartedAction = batchStartedAction;
        this.batchStoreAction = batchStoreAction;
        this.maxBatchSize = GATEWAY_ALLOCATOR_BATCH_SIZE.get(settings);
        this.primaryShardsBatchGatewayAllocatorTimeout = PRIMARY_BATCH_ALLOCATOR_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(PRIMARY_BATCH_ALLOCATOR_TIMEOUT_SETTING, this::setPrimaryBatchAllocatorTimeout);
        this.replicaShardsBatchGatewayAllocatorTimeout = REPLICA_BATCH_ALLOCATOR_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REPLICA_BATCH_ALLOCATOR_TIMEOUT_SETTING, this::setReplicaBatchAllocatorTimeout);
    }

    @Override
    public void cleanCaches() {
        Stream.of(batchIdToStartedShardBatch, batchIdToStoreShardBatch).forEach(b -> {
            Releasables.close(b.values().stream().map(shardsBatch -> shardsBatch.asyncBatch).collect(Collectors.toList()));
            b.clear();
        });
    }

    // for tests
    protected ShardsBatchGatewayAllocator() {
        this(DEFAULT_SHARD_BATCH_SIZE);
    }

    protected ShardsBatchGatewayAllocator(long batchSize) {
        this.rerouteService = null;
        this.batchStartedAction = null;
        this.primaryShardBatchAllocator = null;
        this.batchStoreAction = null;
        this.replicaShardBatchAllocator = null;
        this.maxBatchSize = batchSize;
        this.primaryShardsBatchGatewayAllocatorTimeout = null;
        this.replicaShardsBatchGatewayAllocatorTimeout = null;
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
        List<List<ShardRouting>> storedShardBatches = batchIdToStoreShardBatch.values()
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
    public BatchRunnableExecutor allocateAllUnassignedShards(final RoutingAllocation allocation, boolean primary) {

        assert primaryShardBatchAllocator != null;
        assert replicaShardBatchAllocator != null;
        return innerAllocateUnassignedBatch(allocation, primaryShardBatchAllocator, replicaShardBatchAllocator, primary);
    }

    protected BatchRunnableExecutor innerAllocateUnassignedBatch(
        RoutingAllocation allocation,
        PrimaryShardBatchAllocator primaryBatchShardAllocator,
        ReplicaShardBatchAllocator replicaBatchShardAllocator,
        boolean primary
    ) {
        // create batches for unassigned shards
        Set<String> batchesToAssign = createAndUpdateBatches(allocation, primary);
        if (batchesToAssign.isEmpty()) {
            return null;
        }
        List<TimeoutAwareRunnable> runnables = new ArrayList<>();
        if (primary) {
            batchIdToStartedShardBatch.values()
                .stream()
                .filter(batch -> batchesToAssign.contains(batch.batchId))
                .forEach(shardsBatch -> runnables.add(new TimeoutAwareRunnable() {
                    @Override
                    public void onTimeout() {
                        primaryBatchShardAllocator.allocateUnassignedBatchOnTimeout(
                            shardsBatch.getBatchedShardRoutings(),
                            allocation,
                            true
                        );
                    }

                    @Override
                    public void run() {
                        primaryBatchShardAllocator.allocateUnassignedBatch(shardsBatch.getBatchedShardRoutings(), allocation);
                    }
                }));
            return new BatchRunnableExecutor(runnables, () -> primaryShardsBatchGatewayAllocatorTimeout);
        } else {
            batchIdToStoreShardBatch.values()
                .stream()
                .filter(batch -> batchesToAssign.contains(batch.batchId))
                .forEach(batch -> runnables.add(new TimeoutAwareRunnable() {
                    @Override
                    public void onTimeout() {
                        replicaBatchShardAllocator.allocateUnassignedBatchOnTimeout(batch.getBatchedShardRoutings(), allocation, false);
                    }

                    @Override
                    public void run() {
                        replicaBatchShardAllocator.allocateUnassignedBatch(batch.getBatchedShardRoutings(), allocation);
                    }
                }));
            return new BatchRunnableExecutor(runnables, () -> replicaShardsBatchGatewayAllocatorTimeout);
        }
    }

    // visible for testing
    protected Set<String> createAndUpdateBatches(RoutingAllocation allocation, boolean primary) {
        Set<String> batchesToBeAssigned = new HashSet<>();
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        ConcurrentMap<String, ShardsBatch> currentBatches = primary ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
        // get all batched shards
        Map<ShardId, String> currentBatchedShards = new HashMap<>();
        for (Map.Entry<String, ShardsBatch> batchEntry : currentBatches.entrySet()) {
            batchEntry.getValue().getBatchedShards().forEach(shardId -> currentBatchedShards.put(shardId, batchEntry.getKey()));
        }

        Map<ShardId, ShardRouting> newShardsToBatch = new HashMap<>();
        Set<ShardId> batchedShardsToAssign = Sets.newHashSet();
        // add all unassigned shards to the batch if they are not already in a batch
        unassigned.forEach(shardRouting -> {
            if ((currentBatchedShards.containsKey(shardRouting.shardId()) == false) && (shardRouting.primary() == primary)) {
                assert shardRouting.unassigned();
                newShardsToBatch.put(shardRouting.shardId(), shardRouting);
            }
            // if shard is already batched update to latest shardRouting information in the batches
            // Replica shard assignment can be cancelled if we get a better match. These ShardRouting objects also
            // store other information like relocating node, targetRelocatingShard etc. And it can be updated after
            // batches are created. If we don't update the ShardRouting object, stale data would be passed from the
            // batch. This stale data can end up creating a same decision which has already been taken, and we'll see
            // failure in executeDecision of BaseGatewayShardAllocator. Previous non-batch mode flow also used to
            // pass ShardRouting object directly from unassignedIterator, so we're following the same behaviour.
            else if (shardRouting.primary() == primary) {
                String batchId = currentBatchedShards.get(shardRouting.shardId());
                batchesToBeAssigned.add(batchId);
                currentBatches.get(batchId).batchInfo.get(shardRouting.shardId()).setShardRouting(shardRouting);
                batchedShardsToAssign.add(shardRouting.shardId());
            }
        });

        allocation.routingNodes().forEach(routingNode -> routingNode.getInitializingShards().forEach(shardRouting -> {
            if (currentBatchedShards.containsKey(shardRouting.shardId()) && shardRouting.primary() == primary) {
                batchedShardsToAssign.add(shardRouting.shardId());
                // Set updated shard routing in batch if it already exists
                String batchId = currentBatchedShards.get(shardRouting.shardId());
                currentBatches.get(batchId).batchInfo.get(shardRouting.shardId()).setShardRouting(shardRouting);
            }
        }));

        refreshShardBatches(currentBatches, batchedShardsToAssign, primary);

        Iterator<ShardRouting> iterator = newShardsToBatch.values().iterator();
        assert maxBatchSize > 0 : "Shards batch size must be greater than 0";

        long batchSize = maxBatchSize;
        Map<ShardId, ShardEntry> perBatchShards = new HashMap<>();
        while (iterator.hasNext()) {
            ShardRouting currentShard = iterator.next();
            ShardEntry shardEntry = new ShardEntry(
                new ShardAttributes(
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(currentShard.index()).getSettings())
                ),
                currentShard
            );
            perBatchShards.put(currentShard.shardId(), shardEntry);
            batchSize--;
            iterator.remove();
            // add to batch if batch size full or last shard in unassigned list
            if (batchSize == 0 || iterator.hasNext() == false) {
                String batchUUId = UUIDs.base64UUID();
                ShardsBatch shardsBatch = new ShardsBatch(batchUUId, perBatchShards, primary);
                // add the batch to list of current batches
                addBatch(shardsBatch, primary);
                batchesToBeAssigned.add(batchUUId);
                perBatchShards.clear();
                batchSize = maxBatchSize;
            }
        }
        return batchesToBeAssigned;
    }

    private void refreshShardBatches(
        ConcurrentMap<String, ShardsBatch> currentBatches,
        Set<ShardId> batchedShardsToAssign,
        boolean primary
    ) {
        // cleanup shard from batches if they are not present in unassigned list from allocation object. This is
        // needed as AllocationService.reroute can also be called directly by API flows for example DeleteIndices.
        // So, as part of calling reroute, those shards will be removed from allocation object. It'll handle the
        // scenarios where shards can be removed from unassigned list without "start" or "failed" event.
        for (Map.Entry<String, ShardsBatch> batchEntry : currentBatches.entrySet()) {
            Iterator<ShardId> shardIdIterator = batchEntry.getValue().getBatchedShards().iterator();
            while (shardIdIterator.hasNext()) {
                ShardId shardId = shardIdIterator.next();
                if (batchedShardsToAssign.contains(shardId) == false) {
                    shardIdIterator.remove();
                    batchEntry.getValue().clearShardFromCache(shardId);
                }
            }
            ConcurrentMap<String, ShardsBatch> batches = primary ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
            deleteBatchIfEmpty(batches, batchEntry.getValue().getBatchId());
        }
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
     * @param primary from which batch shard needs to be removed
     */
    protected void safelyRemoveShardFromBatch(ShardRouting shardRouting, boolean primary) {
        String batchId = primary ? getBatchId(shardRouting, true) : getBatchId(shardRouting, false);
        if (batchId == null) {
            logger.debug("Shard[{}] is not batched", shardRouting);
            return;
        }
        ConcurrentMap<String, ShardsBatch> batches = primary ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
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
        safelyRemoveShardFromBatch(shardRouting, true);
        safelyRemoveShardFromBatch(shardRouting, false);
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
            batchIdToStoreShardBatch.values().forEach(batch -> clearCacheForBatchPrimary(batch, allocation));

            // recalc to also (lazily) clear out old nodes.
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    private static void clearCacheForBatchPrimary(ShardsBatch batch, RoutingAllocation allocation) {
        // We need to clear the cache for the primary shard to ensure we do not cancel recoveries based on excessively
        // stale data. We do this by clearing the cache of nodes for all the active primaries of replicas in the current batch.
        // Although this flow can be optimized by only clearing the cache for the primary shard but currently
        // when we want to fetch data we do for complete node, for doing this a new fetch flow will also handle just
        // fetching the data for a single shard on the node and fill that up in our cache
        // Opened issue #13352 - to track the improvement
        List<ShardRouting> primaries = batch.getBatchedShards()
            .stream()
            .map(allocation.routingNodes()::activePrimary)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        AsyncShardBatchFetch<? extends BaseNodeResponse, ?> fetch = batch.getAsyncFetcher();
        primaries.forEach(shardRouting -> fetch.clearCacheForNode(shardRouting.currentNodeId()));
    }

    private boolean hasNewNodes(DiscoveryNodes nodes) {
        for (final DiscoveryNode node : nodes.getDataNodes().values()) {
            if (lastSeenEphemeralIds.contains(node.getEphemeralId()) == false) {
                return true;
            }
        }
        return false;
    }

    class InternalBatchAsyncFetch<T extends BaseNodeResponse, V> extends AsyncShardBatchFetch<T, V> {
        InternalBatchAsyncFetch(
            Logger logger,
            String type,
            Map<ShardId, ShardAttributes> map,
            AsyncShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
            String batchUUId,
            Class<V> clazz,
            V emptyShardResponse,
            Predicate<V> emptyShardResponsePredicate,
            ShardBatchResponseFactory<T, V> responseFactory
        ) {
            super(logger, type, map, action, batchUUId, clazz, emptyShardResponse, emptyShardResponsePredicate, responseFactory);
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
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> fetchData(
            List<ShardRouting> eligibleShards,
            List<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            return (AsyncShardFetch.FetchResult<
                TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch>) fetchDataAndCleanIneligibleShards(
                    eligibleShards,
                    inEligibleShards,
                    allocation
                );
        }

    }

    class InternalReplicaBatchShardAllocator extends ReplicaShardBatchAllocator {
        @Override
        @SuppressWarnings("unchecked")
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch> fetchData(
            List<ShardRouting> eligibleShards,
            List<ShardRouting> inEligibleShards,
            RoutingAllocation allocation
        ) {
            return (AsyncShardFetch.FetchResult<
                TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch>) fetchDataAndCleanIneligibleShards(
                    eligibleShards,
                    inEligibleShards,
                    allocation
                );
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            /**
             * This function is to check if asyncFetch has happened before for this shard batch, or is ongoing.
             * It should return false if there has never been a fetch for this batch.
             * This function is currently only used in the case of replica shards when all deciders returned NO/THROTTLE, and explain mode is ON.
             * Allocation explain and manual reroute APIs try to append shard store information (matching bytes) to the allocation decision.
             * However, these APIs do not want to trigger a new asyncFetch for these ineligible shards, unless the data from nodes is already there.
             * This function is used to see if a fetch has happened to decide if it is possible to append shard store info without a new async fetch.
             *
             * In order to check if a fetch has ever happened, we check 2 things:
             * 1. If the shard batch cache is empty, we know that fetch has never happened so we return false.
             * 2. If we see that the list of nodes to fetch from is empty, we know that all nodes have data or are ongoing a fetch. So we return true.
             * 3. Otherwise we return false.
             *
             * see {@link AsyncShardFetchCache#findNodesToFetch()}
             */
            String batchId = getBatchId(shard, shard.primary());
            logger.trace("Checking if fetching done for batch id {}", batchId);
            ShardsBatch shardsBatch = shard.primary() ? batchIdToStartedShardBatch.get(batchId) : batchIdToStoreShardBatch.get(batchId);

            // if fetchData has never been called, the per node cache will be empty and have no nodes
            // this is because cache.fillShardCacheWithDataNodes(nodes) initialises this map and is called in AsyncShardFetch.fetchData
            if (shardsBatch.getAsyncFetcher().hasEmptyCache()) {
                logger.trace("Batch cache is empty for batch {} ", batchId);
                return false;
            }
            // this check below is to make sure we already have all the data and that we wouldn't create a new async fetchData call
            return shardsBatch.getAsyncFetcher().getCache().findNodesToFetch().isEmpty();
        }
    }

    AsyncShardFetch.FetchResult<? extends BaseNodeResponse> fetchDataAndCleanIneligibleShards(
        List<ShardRouting> eligibleShards,
        List<ShardRouting> inEligibleShards,
        RoutingAllocation allocation
    ) {
        // get batch id for anyone given shard. We are assuming all shards will have same batchId
        ShardRouting shardRouting = eligibleShards.iterator().hasNext() ? eligibleShards.iterator().next() : null;
        shardRouting = shardRouting == null && inEligibleShards.iterator().hasNext() ? inEligibleShards.iterator().next() : shardRouting;
        if (shardRouting == null) {
            return new AsyncShardFetch.FetchResult<>(null, Collections.emptyMap());
        }
        String batchId = getBatchId(shardRouting, shardRouting.primary());
        if (batchId == null) {
            logger.debug("Shard {} has no batch id", shardRouting);
            throw new IllegalStateException("Shard " + shardRouting + " has no batch id. Shard should batched before fetching");
        }
        ConcurrentMap<String, ShardsBatch> batches = shardRouting.primary() ? batchIdToStartedShardBatch : batchIdToStoreShardBatch;
        if (batches.containsKey(batchId) == false) {
            logger.debug("Batch {} has no shards batch", batchId);
            throw new IllegalStateException("Batch " + batchId + " has no shards batch");
        }

        ShardsBatch shardsBatch = batches.get(batchId);
        // remove in eligible shards which allocator is not responsible for
        inEligibleShards.forEach(sr -> safelyRemoveShardFromBatch(sr, sr.primary()));

        if (shardsBatch.getBatchedShards().isEmpty() && eligibleShards.isEmpty()) {
            logger.debug("Batch {} is empty", batchId);
            return new AsyncShardFetch.FetchResult<>(null, Collections.emptyMap());
        }
        Map<ShardId, Set<String>> shardToIgnoreNodes = new HashMap<>();
        for (ShardId shardId : shardsBatch.asyncBatch.shardAttributesMap.keySet()) {
            shardToIgnoreNodes.put(shardId, allocation.getIgnoreNodes(shardId));
        }
        AsyncShardBatchFetch<? extends BaseNodeResponse, ?> asyncFetcher = shardsBatch.getAsyncFetcher();
        AsyncShardFetch.FetchResult<? extends BaseNodeResponse> fetchResult = asyncFetcher.fetchData(
            allocation.nodes(),
            shardToIgnoreNodes
        );
        if (fetchResult.hasData()) {
            fetchResult.processAllocation(allocation);
        }

        return fetchResult;
    }

    /**
     * Holds information about a batch of shards to be allocated.
     * Async fetcher is used to fetch the data for the batch.
     * <p>
     * Visible for testing
     */
    public class ShardsBatch {
        private final String batchId;
        private final boolean primary;

        private final InternalBatchAsyncFetch<? extends BaseNodeResponse, ?> asyncBatch;

        private final Map<ShardId, ShardEntry> batchInfo;

        public ShardsBatch(String batchId, Map<ShardId, ShardEntry> shardsWithInfo, boolean primary) {
            this.batchId = batchId;
            this.batchInfo = new HashMap<>(shardsWithInfo);
            // create a ShardId -> customDataPath map for async fetch
            Map<ShardId, ShardAttributes> shardIdsMap = batchInfo.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getShardAttributes()));
            this.primary = primary;
            if (this.primary) {
                asyncBatch = new InternalBatchAsyncFetch<>(
                    logger,
                    "batch_shards_started",
                    shardIdsMap,
                    batchStartedAction,
                    batchId,
                    GatewayStartedShard.class,
                    new GatewayStartedShard(null, false, null, null),
                    GatewayStartedShard::isEmpty,
                    new ShardBatchResponseFactory<>(true)
                );
            } else {
                asyncBatch = new InternalBatchAsyncFetch<>(
                    logger,
                    "batch_shards_store",
                    shardIdsMap,
                    batchStoreAction,
                    batchId,
                    NodeStoreFilesMetadata.class,
                    new NodeStoreFilesMetadata(new StoreFilesMetadata(null, Store.MetadataSnapshot.EMPTY, Collections.emptyList()), null),
                    NodeStoreFilesMetadata::isEmpty,
                    new ShardBatchResponseFactory<>(false)
                );
            }
        }

        protected void removeShard(ShardId shardId) {
            this.batchInfo.remove(shardId);
        }

        private TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata buildEmptyReplicaShardResponse() {
            return new TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata(
                new TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata(
                    null,
                    Store.MetadataSnapshot.EMPTY,
                    Collections.emptyList()
                ),
                null
            );
        }

        private void removeFromBatch(ShardRouting shard) {
            removeShard(shard.shardId());
            clearShardFromCache(shard.shardId());
            // assert that fetcher and shards are the same as batched shards
            assert batchInfo.size() == asyncBatch.shardAttributesMap.size() : "Shards size is not equal to fetcher size";
        }

        private void clearShardFromCache(ShardId shardId) {
            asyncBatch.clearShard(shardId);
        }

        public List<ShardRouting> getBatchedShardRoutings() {
            return batchInfo.values().stream().map(ShardEntry::getShardRouting).collect(Collectors.toList());
        }

        public Set<ShardId> getBatchedShards() {
            return batchInfo.keySet();
        }

        public String getBatchId() {
            return batchId;
        }

        public AsyncShardBatchFetch<? extends BaseNodeResponse, ?> getAsyncFetcher() {
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
            if (o instanceof ShardsBatch == false) {
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
    static class ShardEntry {

        private final ShardAttributes shardAttributes;

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

        public ShardEntry setShardRouting(ShardRouting shardRouting) {
            this.shardRouting = shardRouting;
            return this;
        }
    }

    public int getNumberOfStartedShardBatches() {
        return batchIdToStartedShardBatch.size();
    }

    public int getNumberOfStoreShardBatches() {
        return batchIdToStoreShardBatch.size();
    }

    private void setPrimaryBatchAllocatorTimeout(TimeValue primaryShardsBatchGatewayAllocatorTimeout) {
        this.primaryShardsBatchGatewayAllocatorTimeout = primaryShardsBatchGatewayAllocatorTimeout;
    }

    private void setReplicaBatchAllocatorTimeout(TimeValue replicaShardsBatchGatewayAllocatorTimeout) {
        this.replicaShardsBatchGatewayAllocatorTimeout = replicaShardsBatchGatewayAllocatorTimeout;
    }
}
