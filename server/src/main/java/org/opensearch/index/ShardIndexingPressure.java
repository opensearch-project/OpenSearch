/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.stats.ShardIndexingPressureStats;
import org.opensearch.index.stats.IndexingPressurePerShardStats;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Shard Indexing Pressure is the uber level class similar to IndexingPressure.
 * The methods of this class will be invoked from Transport Action to start the memory accounting and as a response
 * it provides Releasable which will remove those memory accounting values or perform necessary actions once the request
 * completes.
 *
 * This class will be responsible for
 * 1. Memory Accounting at shard level.
 * 2. Memory Accounting at Node level. The tracking happens in the same variables defined in IndexingPressure to support
 * consistency even after feature toggle.
 * 3. Instantiating new tracker objects for new shards and moving the shard tracker object to cold store from hot when
 * the respective criteria meet via {@link ShardIndexingPressureStore}
 * 4. Calling methods of {@link ShardIndexingPressureMemoryManager} to evaluate if a request can be process successfully
 * and can increase the memory limits for a shard under certain scenarios
 */
public class ShardIndexingPressure {

    public static final String SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY = "shard_indexing_pressure_enabled";
    private final Logger logger = LogManager.getLogger(getClass());

    private final IndexingPressure indexingPressure;
    private static ClusterService clusterService;
    private final ShardIndexingPressureSettings shardIndexingPressureSettings;
    private final ShardIndexingPressureMemoryManager memoryManager;
    private final ShardIndexingPressureStore shardIndexingPressureStore;

    ShardIndexingPressure(IndexingPressure indexingPressure, ClusterService clusterService, Settings settings) {
        shardIndexingPressureSettings = new ShardIndexingPressureSettings(clusterService.getClusterSettings(), settings,
                indexingPressure.getPrimaryAndCoordinatingLimits());
        ShardIndexingPressure.clusterService = clusterService;
        this.indexingPressure = indexingPressure;
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings, clusterSettings, settings);
        this.shardIndexingPressureStore = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
    }

    public Releasable markCoordinatingOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if(0 == bytes) { return () -> {}; }

        long requestStartTime = System.currentTimeMillis();
        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);
        long nodeCombinedBytes = indexingPressure.addAndGetCurrentCombinedCoordinatingAndPrimaryBytes(bytes);
        long nodeReplicaBytes = indexingPressure.getCurrentReplicaBytes();
        long nodeTotalBytes = nodeCombinedBytes + nodeReplicaBytes;
        long shardCombinedBytes = tracker.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);

        boolean shardLevelLimitBreached = false;
        if (!forceExecution) {
            boolean nodeLevelLimitBreached = memoryManager.isCoordinatingNodeLimitBreached(tracker, nodeTotalBytes);
            if (!nodeLevelLimitBreached) {
                shardLevelLimitBreached = memoryManager.isCoordinatingShardLimitBreached(tracker, requestStartTime,
                        shardIndexingPressureStore.getShardIndexingPressureHotStore(), nodeTotalBytes);
            }
            boolean shouldRejectRequest = nodeLevelLimitBreached || (shardLevelLimitBreached && shardIndexingPressureSettings.isShardIndexingPressureEnforced());

            if (shouldRejectRequest) {
                long nodeBytesWithoutOperation = nodeCombinedBytes - bytes;
                long nodeTotalBytesWithoutOperation = nodeTotalBytes - bytes;
                long shardBytesWithoutOperation = shardCombinedBytes - bytes;

                indexingPressure.addAndGetCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
                indexingPressure.getAndIncrementCoordinatingRejections();
                tracker.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
                tracker.coordinatingRejections.getAndIncrement();

                throw new OpenSearchRejectedExecutionException("rejected execution of coordinating operation [" +
                        "shard_detail=[" + shardId.getIndexName() + "][" + shardId.id() + "][C], " +
                        "shard_coordinating_and_primary_bytes=" + shardBytesWithoutOperation + ", " +
                        "shard_operation_bytes=" + bytes + ", " +
                        "shard_max_coordinating_and_primary_bytes=" + tracker.primaryAndCoordinatingLimits + "] OR [" +
                        "node_coordinating_and_primary_bytes=" + nodeBytesWithoutOperation + ", " +
                        "node_replica_bytes=" + nodeReplicaBytes + ", " +
                        "node_all_bytes=" + nodeTotalBytesWithoutOperation + ", " +
                        "node_operation_bytes=" + bytes + ", " +
                        "node_max_coordinating_and_primary_bytes=" + this.indexingPressure.getPrimaryAndCoordinatingLimits() + "]", false);
            }
        }
        indexingPressure.addAndGetCurrentCoordinatingBytes(bytes);
        indexingPressure.addAndGetTotalCombinedCoordinatingAndPrimaryBytes(bytes);
        indexingPressure.addAndGetTotalCoordinatingBytes(bytes);
        tracker.currentCoordinatingBytes.getAndAdd(bytes);
        tracker.coordinatingCount.incrementAndGet();
        tracker.totalOutstandingCoordinatingRequests.incrementAndGet();

        // In shadow mode if request was intended to rejected; it should only contribute to accounting limits and
        // should not influence dynamic parameters such as throughput
        if (shardLevelLimitBreached) {
            return () -> {
                indexingPressure.addAndGetCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
                indexingPressure.addAndGetCurrentCoordinatingBytes(-bytes);
                tracker.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
                tracker.currentCoordinatingBytes.addAndGet(-bytes);
                tracker.totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
                tracker.totalCoordinatingBytes.getAndAdd(bytes);

                memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
                shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
            };
        }

        return () -> {
            long requestEndTime = System.currentTimeMillis();
            long requestLatency = requestEndTime - requestStartTime;

            indexingPressure.addAndGetCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
            indexingPressure.addAndGetCurrentCoordinatingBytes(-bytes);
            tracker.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
            tracker.currentCoordinatingBytes.addAndGet(-bytes);
            tracker.coordinatingTimeInMillis.addAndGet(requestLatency);
            tracker.totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
            tracker.totalCoordinatingBytes.getAndAdd(bytes);
            tracker.lastSuccessfulCoordinatingRequestTimestamp.set(requestEndTime);
            tracker.totalOutstandingCoordinatingRequests.set(0);

            if(requestLatency > 0) {
                double requestThroughput = (double) bytes / requestLatency;
                tracker.coordinatingThroughputMovingQueue.offer(requestThroughput);
                if (tracker.coordinatingThroughputMovingQueue.size() > shardIndexingPressureSettings.getRequestSizeWindow()) {
                    double front = tracker.coordinatingThroughputMovingQueue.poll();
                    double movingAverage =
                        calculateMovingAverage(tracker.coordinatingThroughputMovingAverage.get(), front, requestThroughput, shardIndexingPressureSettings.getRequestSizeWindow());
                    tracker.coordinatingThroughputMovingAverage.set(Double.doubleToLongBits(movingAverage));
                 } else {
                    double movingAverage = (double) tracker.totalCoordinatingBytes.get() / tracker.coordinatingTimeInMillis.get();
                    tracker.coordinatingThroughputMovingAverage.set(Double.doubleToLongBits(movingAverage));
                }
            }
            memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
            shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
        };
    }

    public Releasable markPrimaryOperationLocalToCoordinatingNodeStarted(ShardId shardId, long bytes) {
        if(bytes == 0) { return () -> {}; }

        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);

        indexingPressure.addAndGetCurrentPrimaryBytes(bytes);
        indexingPressure.addAndGetTotalPrimaryBytes(bytes);
        tracker.currentPrimaryBytes.getAndAdd(bytes);
        tracker.totalPrimaryBytes.getAndAdd(bytes);

        return () -> {
            indexingPressure.addAndGetCurrentPrimaryBytes(-bytes);
            tracker.currentPrimaryBytes.addAndGet(-bytes);
        };
    }

    public Releasable markPrimaryOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if(0 == bytes) { return () -> {}; }

        long requestStartTime = System.currentTimeMillis();
        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);
        long nodeCombinedBytes = indexingPressure.addAndGetCurrentCombinedCoordinatingAndPrimaryBytes(bytes);
        long nodeReplicaBytes = indexingPressure.getCurrentReplicaBytes();
        long nodeTotalBytes = nodeCombinedBytes + nodeReplicaBytes;
        long shardCombinedBytes = tracker.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);

        boolean shardLevelLimitBreached = false;
        if (!forceExecution) {
            boolean nodeLevelLimitBreached = memoryManager.isPrimaryNodeLimitBreached(tracker, nodeTotalBytes);
            if (!nodeLevelLimitBreached) {
                shardLevelLimitBreached = memoryManager.isPrimaryShardLimitBreached(tracker, requestStartTime,
                        shardIndexingPressureStore.getShardIndexingPressureHotStore(), nodeTotalBytes);
            }
            boolean shouldRejectRequest = nodeLevelLimitBreached || (shardLevelLimitBreached && shardIndexingPressureSettings.isShardIndexingPressureEnforced());

            if (shouldRejectRequest) {
                long nodeBytesWithoutOperation = nodeCombinedBytes - bytes;
                long nodeTotalBytesWithoutOperation = nodeTotalBytes - bytes;
                long shardBytesWithoutOperation = shardCombinedBytes - bytes;

                indexingPressure.addAndGetCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
                indexingPressure.getAndIncrementPrimaryRejections();
                tracker.currentCombinedCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
                tracker.primaryRejections.getAndIncrement();

                throw new OpenSearchRejectedExecutionException("rejected execution of primary operation [" +
                        "shard_detail=[" + shardId.getIndexName() + "][" + shardId.id() + "][P], " +
                        "shard_coordinating_and_primary_bytes=" + shardBytesWithoutOperation + ", " +
                        "shard_operation_bytes=" + bytes + ", " +
                        "shard_max_coordinating_and_primary_bytes=" + tracker.primaryAndCoordinatingLimits + "] OR [" +
                        "node_coordinating_and_primary_bytes=" + nodeBytesWithoutOperation + ", " +
                        "node_replica_bytes=" + nodeReplicaBytes + ", " +
                        "node_all_bytes=" + nodeTotalBytesWithoutOperation + ", " +
                        "node_operation_bytes=" + bytes + ", " +
                        "node_max_coordinating_and_primary_bytes=" + this.indexingPressure.getPrimaryAndCoordinatingLimits() + "]", false);
            }
        }
        indexingPressure.addAndGetCurrentPrimaryBytes(bytes);
        indexingPressure.addAndGetTotalCombinedCoordinatingAndPrimaryBytes(bytes);
        indexingPressure.addAndGetTotalPrimaryBytes(bytes);
        tracker.currentPrimaryBytes.getAndAdd(bytes);
        tracker.primaryCount.incrementAndGet();
        tracker.totalOutstandingPrimaryRequests.incrementAndGet();

        // In shadow mode if request was intended to rejected; it should only contribute to accounting limits and
        // should not influence dynamic parameters such as throughput
        if (shardLevelLimitBreached) {
            return () -> {
                indexingPressure.addAndGetCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
                indexingPressure.addAndGetCurrentPrimaryBytes(-bytes);
                tracker.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
                tracker.currentPrimaryBytes.addAndGet(-bytes);
                tracker.totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
                tracker.totalPrimaryBytes.getAndAdd(bytes);

                memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
                shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
            };
        }

        return () -> {
            long requestEndTime = System.currentTimeMillis();
            long requestLatency = requestEndTime - requestStartTime;

            indexingPressure.addAndGetCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
            indexingPressure.addAndGetCurrentPrimaryBytes(-bytes);
            tracker.currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
            tracker.currentPrimaryBytes.addAndGet(-bytes);
            tracker.primaryTimeInMillis.addAndGet(requestLatency);
            tracker.totalCombinedCoordinatingAndPrimaryBytes.getAndAdd(bytes);
            tracker.totalPrimaryBytes.getAndAdd(bytes);
            tracker.lastSuccessfulPrimaryRequestTimestamp.set(requestEndTime);
            tracker.totalOutstandingPrimaryRequests.set(0);

            if(requestLatency > 0) {
                double requestThroughput = (double)bytes / requestLatency;
                tracker.primaryThroughputMovingQueue.offer(requestThroughput);
                if(tracker.primaryThroughputMovingQueue.size() > shardIndexingPressureSettings.getRequestSizeWindow()) {
                    double front = tracker.primaryThroughputMovingQueue.poll();
                    double movingAverage =
                        calculateMovingAverage(tracker.primaryThroughputMovingAverage.get(), front, requestThroughput, shardIndexingPressureSettings.getRequestSizeWindow());
                    tracker.primaryThroughputMovingAverage.set(Double.doubleToLongBits(movingAverage));
                } else {
                    double movingAverage = (double) tracker.totalPrimaryBytes.get() / tracker.primaryTimeInMillis.get();
                    tracker.primaryThroughputMovingAverage.set(Double.doubleToLongBits(movingAverage));
                }
            }
            memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
            shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
        };
    }

    public Releasable markReplicaOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if(0 == bytes) { return () -> {}; }

        long requestStartTime = System.currentTimeMillis();
        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);
        long nodeReplicaBytes = indexingPressure.addAndGetCurrentReplicaBytes(bytes);
        long shardReplicaBytes = tracker.currentReplicaBytes.addAndGet(bytes);

        boolean shardLevelLimitBreached = false;
        if (!forceExecution) {
            boolean nodeLevelLimitBreached = memoryManager.isReplicaNodeLimitBreached(tracker, nodeReplicaBytes);
            if (!nodeLevelLimitBreached) {
                shardLevelLimitBreached = memoryManager.isReplicaShardLimitBreached(tracker, requestStartTime,
                        shardIndexingPressureStore.getShardIndexingPressureHotStore(), nodeReplicaBytes);
            }
            boolean shouldRejectRequest = nodeLevelLimitBreached || (shardLevelLimitBreached && shardIndexingPressureSettings.isShardIndexingPressureEnforced());

            if (shouldRejectRequest) {
                long nodeReplicaBytesWithoutOperation = nodeReplicaBytes - bytes;
                long shardReplicaBytesWithoutOperation = shardReplicaBytes - bytes;

                indexingPressure.addAndGetCurrentReplicaBytes(-bytes);
                indexingPressure.getAndIncrementReplicaRejections();
                tracker.currentReplicaBytes.getAndAdd(-bytes);
                tracker.replicaRejections.getAndIncrement();

                throw new OpenSearchRejectedExecutionException("rejected execution of replica operation [" +
                        "shard_detail=[" + shardId.getIndexName() + "][" + shardId.id() + "][R], " +
                        "shard_replica_bytes=" + shardReplicaBytesWithoutOperation + ", " +
                        "operation_bytes=" + bytes + ", " +
                        "max_coordinating_and_primary_bytes=" + tracker.replicaLimits + "] OR [" +
                        "replica_bytes=" + nodeReplicaBytesWithoutOperation + ", " +
                        "operation_bytes=" + bytes + ", " +
                        "max_coordinating_and_primary_bytes=" + this.indexingPressure.getReplicaLimits() + "]", false);
            }
        }
        indexingPressure.addAndGetTotalReplicaBytes(bytes);
        tracker.replicaCount.incrementAndGet();
        tracker.totalOutstandingReplicaRequests.incrementAndGet();

        // In shadow-mode if request was intended to rejected; it should only contribute to accounting limits and
        // should not influence dynamic parameters such as throughput
        if (shardLevelLimitBreached) {
            return () -> {
                indexingPressure.addAndGetCurrentReplicaBytes(-bytes);
                tracker.currentReplicaBytes.addAndGet(-bytes);
                tracker.totalReplicaBytes.getAndAdd(bytes);

                memoryManager.decreaseShardReplicaLimits(tracker);
                shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
            };
        }

        return () -> {
            long requestEndTime = System.currentTimeMillis();
            long requestLatency = requestEndTime - requestStartTime;

            indexingPressure.addAndGetCurrentReplicaBytes(-bytes);
            tracker.currentReplicaBytes.addAndGet(-bytes);
            tracker.replicaTimeInMillis.addAndGet(requestLatency);
            tracker.totalReplicaBytes.getAndAdd(bytes);
            tracker.lastSuccessfulReplicaRequestTimestamp.set(requestEndTime);
            tracker.totalOutstandingReplicaRequests.set(0);

            if(requestLatency > 0) {
                double requestThroughput = (double) bytes / requestLatency;
                tracker.replicaThroughputMovingQueue.offer(requestThroughput);
                if (tracker.replicaThroughputMovingQueue.size() > shardIndexingPressureSettings.getRequestSizeWindow()) {
                    double front = tracker.replicaThroughputMovingQueue.poll();
                    double movingAverage =
                        calculateMovingAverage(tracker.replicaThroughputMovingAverage.get(), front, requestThroughput, shardIndexingPressureSettings.getRequestSizeWindow());
                    tracker.replicaThroughputMovingAverage.set(Double.doubleToLongBits(movingAverage));
                } else {
                    double movingAverage = (double) tracker.totalReplicaBytes.get() / tracker.replicaTimeInMillis.get();
                    tracker.replicaThroughputMovingAverage.set(Double.doubleToLongBits(movingAverage));
                }
            }
            memoryManager.decreaseShardReplicaLimits(tracker);
            shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
        };
    }

    private double calculateMovingAverage(long currentAverage, double frontValue, double currentValue, int count) {
        if(count > 0) {
            return ((Double.longBitsToDouble(currentAverage) * count) + currentValue - frontValue) / count;
        } else {
            return currentValue;
        }
    }

    public ShardIndexingPressureStats stats(CommonStatsFlags statsFlags) {

        if (statsFlags.includeOnlyTopIndexingPressureMetrics()) {
            return topStats();
        } else {
            ShardIndexingPressureStats allStats = stats();
            if (statsFlags.includeAllShardIndexingPressureTrackers()) {
                allStats.addAll(coldStats());
            }
            return allStats;
        }
    }

    ShardIndexingPressureStats stats() {
        Map<Long, IndexingPressurePerShardStats> statsPerShard = new HashMap<>();
        boolean isEnforcedMode = shardIndexingPressureSettings.isShardIndexingPressureEnforced();

        for (Map.Entry<Long, ShardIndexingPressureTracker> shardEntry :
                this.shardIndexingPressureStore.getShardIndexingPressureHotStore().entrySet()) {
            IndexingPressurePerShardStats shardStats = new IndexingPressurePerShardStats(shardEntry.getValue(),
                    isEnforcedMode);
            statsPerShard.put(shardEntry.getKey(), shardStats);
        }
        return new ShardIndexingPressureStats(statsPerShard, memoryManager.totalNodeLimitsBreachedRejections.get(),
            memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get(),
            memoryManager.totalThroughputDegradationLimitsBreachedRejections.get(), shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
                isEnforcedMode);
    }

    ShardIndexingPressureStats coldStats() {
        Map<Long, IndexingPressurePerShardStats> statsPerShard = new HashMap<>();
        boolean isEnforcedMode = shardIndexingPressureSettings.isShardIndexingPressureEnforced();

        for (Map.Entry<Long, ShardIndexingPressureTracker> shardEntry :
                this.shardIndexingPressureStore.getShardIndexingPressureColdStore().entrySet()) {
            IndexingPressurePerShardStats shardStats = new IndexingPressurePerShardStats(shardEntry.getValue(),
                    isEnforcedMode);
            statsPerShard.put(shardEntry.getKey(), shardStats);
        }
        return new ShardIndexingPressureStats(statsPerShard, memoryManager.totalNodeLimitsBreachedRejections.get(),
            memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get(),
            memoryManager.totalThroughputDegradationLimitsBreachedRejections.get(), shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
                isEnforcedMode);
    }

    ShardIndexingPressureStats topStats() {
        return new ShardIndexingPressureStats(Collections.emptyMap(), memoryManager.totalNodeLimitsBreachedRejections.get(),
                memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get(),
                memoryManager.totalThroughputDegradationLimitsBreachedRejections.get(), shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
                shardIndexingPressureSettings.isShardIndexingPressureEnforced());
    }

    ShardIndexingPressureTracker getShardIndexingPressureTracker(ShardId shardId) {
        return shardIndexingPressureStore.getShardIndexingPressureTracker(shardId);
    }

    public static boolean isShardIndexingPressureAttributeEnabled() {
        Iterator<DiscoveryNode> nodes = clusterService.state().getNodes().getNodes().valuesIt();
        while (nodes.hasNext()) {
            if (!Boolean.parseBoolean(nodes.next().getAttributes().get(SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY))) {
                return false;
            }
        }
        return true;
    }

    public boolean isShardIndexingPressureEnabled() {
        return shardIndexingPressureSettings.isShardIndexingPressureEnabled();
    }
}
