/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
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
import java.util.Map;

/**
 * Shard Indexing Pressure is the uber level class derived from IndexingPressure.
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
public class ShardIndexingPressure extends IndexingPressure {

    private final Logger logger = LogManager.getLogger(getClass());

    private final ShardIndexingPressureSettings shardIndexingPressureSettings;
    private final ShardIndexingPressureMemoryManager memoryManager;
    private final ShardIndexingPressureStore shardIndexingPressureStore;

    ShardIndexingPressure(Settings settings, ClusterService clusterService) {
        super(settings);
        shardIndexingPressureSettings = new ShardIndexingPressureSettings(clusterService, settings, primaryAndCoordinatingLimits);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings, clusterSettings, settings);
        this.shardIndexingPressureStore = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);
    }

    public Releasable markCoordinatingOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if(0 == bytes) { return () -> {}; }

        long requestStartTime = System.currentTimeMillis();
        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);
        long nodeCombinedBytes = currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long nodeReplicaBytes = currentReplicaBytes.get();
        long nodeTotalBytes = nodeCombinedBytes + nodeReplicaBytes;
        long shardCombinedBytes = tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(bytes);

        boolean shardLevelLimitBreached = false;
        if (forceExecution == false) {
            boolean nodeLevelLimitBreached = memoryManager.isCoordinatingNodeLimitBreached(tracker, nodeTotalBytes);
            if (nodeLevelLimitBreached == false) {
                shardLevelLimitBreached = memoryManager.isCoordinatingShardLimitBreached(tracker, requestStartTime,
                        shardIndexingPressureStore.getShardIndexingPressureHotStore(), nodeTotalBytes);
            }
            boolean shouldRejectRequest = nodeLevelLimitBreached ||
                (shardLevelLimitBreached && shardIndexingPressureSettings.isShardIndexingPressureEnforced());

            if (shouldRejectRequest) {
                long nodeBytesWithoutOperation = nodeCombinedBytes - bytes;
                long nodeTotalBytesWithoutOperation = nodeTotalBytes - bytes;
                long shardBytesWithoutOperation = shardCombinedBytes - bytes;

                currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
                coordinatingRejections.getAndIncrement();
                tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().getAndAdd(-bytes);
                tracker.rejection().getCoordinatingRejections().getAndIncrement();

                throw new OpenSearchRejectedExecutionException("rejected execution of coordinating operation [" +
                        "shard_detail=[" + shardId.getIndexName() + "][" + shardId.id() + "][C], " +
                        "shard_coordinating_and_primary_bytes=" + shardBytesWithoutOperation + ", " +
                        "shard_operation_bytes=" + bytes + ", " +
                        "shard_max_coordinating_and_primary_bytes=" + tracker.getPrimaryAndCoordinatingLimits() + "] OR [" +
                        "node_coordinating_and_primary_bytes=" + nodeBytesWithoutOperation + ", " +
                        "node_replica_bytes=" + nodeReplicaBytes + ", " +
                        "node_all_bytes=" + nodeTotalBytesWithoutOperation + ", " +
                        "node_operation_bytes=" + bytes + ", " +
                        "node_max_coordinating_and_primary_bytes=" + primaryAndCoordinatingLimits + "]", false);
            }
        }
        currentCoordinatingBytes.addAndGet(bytes);
        totalCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        totalCoordinatingBytes.addAndGet(bytes);
        tracker.memory().getCurrentCoordinatingBytes().getAndAdd(bytes);
        tracker.count().getCoordinatingCount().incrementAndGet();
        tracker.outstandingRequest().getTotalOutstandingCoordinatingRequests().incrementAndGet();

        // In shadow mode if request was intended to rejected; it should only contribute to accounting limits and
        // should not influence dynamic parameters such as throughput
        if (shardLevelLimitBreached) {
            return () -> {
                currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
                currentCoordinatingBytes.addAndGet(-bytes);
                tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(-bytes);
                tracker.memory().getCurrentCoordinatingBytes().addAndGet(-bytes);
                tracker.memory().getTotalCombinedCoordinatingAndPrimaryBytes().getAndAdd(bytes);
                tracker.memory().getTotalCoordinatingBytes().getAndAdd(bytes);

                memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
                shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
            };
        }

        return () -> {
            long requestEndTime = System.currentTimeMillis();
            long requestLatency = requestEndTime - requestStartTime;

            currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
            currentCoordinatingBytes.addAndGet(-bytes);
            tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(-bytes);
            tracker.memory().getCurrentCoordinatingBytes().addAndGet(-bytes);
            tracker.latency().getCoordinatingTimeInMillis().addAndGet(requestLatency);
            tracker.memory().getTotalCombinedCoordinatingAndPrimaryBytes().getAndAdd(bytes);
            tracker.memory().getTotalCoordinatingBytes().getAndAdd(bytes);
            tracker.timeStamp().getLastSuccessfulCoordinatingRequestTimestamp().set(requestEndTime);
            tracker.outstandingRequest().getTotalOutstandingCoordinatingRequests().set(0);

            if(requestLatency > 0) {
                double requestThroughput = (double) bytes / requestLatency;
                tracker.throughput().getCoordinatingThroughputMovingQueue().offer(requestThroughput);
                if (tracker.throughput().getCoordinatingThroughputMovingQueue().size() >
                    shardIndexingPressureSettings.getRequestSizeWindow()) {
                    double front = tracker.throughput().getCoordinatingThroughputMovingQueue().poll();
                    double movingAverage = calculateMovingAverage(tracker.throughput().getCoordinatingThroughputMovingAverage().get(),
                        front, requestThroughput, shardIndexingPressureSettings.getRequestSizeWindow());
                    tracker.throughput().getCoordinatingThroughputMovingAverage().set(Double.doubleToLongBits(movingAverage));
                 } else {
                    double movingAverage = (double) tracker.memory().getTotalCoordinatingBytes().get() /
                        tracker.latency().getCoordinatingTimeInMillis().get();
                    tracker.throughput().getCoordinatingThroughputMovingAverage().set(Double.doubleToLongBits(movingAverage));
                }
            }
            memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
            shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
        };
    }

    public Releasable markPrimaryOperationLocalToCoordinatingNodeStarted(ShardId shardId, long bytes) {
        if(bytes == 0) { return () -> {}; }

        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);

        currentPrimaryBytes.addAndGet(bytes);
        totalPrimaryBytes.addAndGet(bytes);
        tracker.memory().getCurrentPrimaryBytes().getAndAdd(bytes);
        tracker.memory().getTotalPrimaryBytes().getAndAdd(bytes);

        return () -> {
            currentPrimaryBytes.addAndGet(-bytes);
            tracker.memory().getCurrentPrimaryBytes().addAndGet(-bytes);
        };
    }

    public Releasable markPrimaryOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if(0 == bytes) { return () -> {}; }

        long requestStartTime = System.currentTimeMillis();
        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);
        long nodeCombinedBytes = currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long nodeReplicaBytes = currentReplicaBytes.get();
        long nodeTotalBytes = nodeCombinedBytes + nodeReplicaBytes;
        long shardCombinedBytes = tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(bytes);

        boolean shardLevelLimitBreached = false;
        if (forceExecution == false) {
            boolean nodeLevelLimitBreached = memoryManager.isPrimaryNodeLimitBreached(tracker, nodeTotalBytes);
            if (nodeLevelLimitBreached == false) {
                shardLevelLimitBreached = memoryManager.isPrimaryShardLimitBreached(tracker, requestStartTime,
                        shardIndexingPressureStore.getShardIndexingPressureHotStore(), nodeTotalBytes);
            }
            boolean shouldRejectRequest = nodeLevelLimitBreached ||
                (shardLevelLimitBreached && shardIndexingPressureSettings.isShardIndexingPressureEnforced());

            if (shouldRejectRequest) {
                long nodeBytesWithoutOperation = nodeCombinedBytes - bytes;
                long nodeTotalBytesWithoutOperation = nodeTotalBytes - bytes;
                long shardBytesWithoutOperation = shardCombinedBytes - bytes;

                currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
                primaryRejections.getAndIncrement();
                tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().getAndAdd(-bytes);
                tracker.rejection().getPrimaryRejections().getAndIncrement();

                throw new OpenSearchRejectedExecutionException("rejected execution of primary operation [" +
                        "shard_detail=[" + shardId.getIndexName() + "][" + shardId.id() + "][P], " +
                        "shard_coordinating_and_primary_bytes=" + shardBytesWithoutOperation + ", " +
                        "shard_operation_bytes=" + bytes + ", " +
                        "shard_max_coordinating_and_primary_bytes=" + tracker.getPrimaryAndCoordinatingLimits() + "] OR [" +
                        "node_coordinating_and_primary_bytes=" + nodeBytesWithoutOperation + ", " +
                        "node_replica_bytes=" + nodeReplicaBytes + ", " +
                        "node_all_bytes=" + nodeTotalBytesWithoutOperation + ", " +
                        "node_operation_bytes=" + bytes + ", " +
                        "node_max_coordinating_and_primary_bytes=" + this.primaryAndCoordinatingLimits + "]", false);
            }
        }
        currentPrimaryBytes.addAndGet(bytes);
        totalCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        totalPrimaryBytes.addAndGet(bytes);
        tracker.memory().getCurrentPrimaryBytes().getAndAdd(bytes);
        tracker.count().getPrimaryCount().incrementAndGet();
        tracker.outstandingRequest().getTotalOutstandingPrimaryRequests().incrementAndGet();

        // In shadow mode if request was intended to rejected; it should only contribute to accounting limits and
        // should not influence dynamic parameters such as throughput
        if (shardLevelLimitBreached) {
            return () -> {
                currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
                currentPrimaryBytes.addAndGet(-bytes);
                tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(-bytes);
                tracker.memory().getCurrentPrimaryBytes().addAndGet(-bytes);
                tracker.memory().getTotalCombinedCoordinatingAndPrimaryBytes().getAndAdd(bytes);
                tracker.memory().getTotalPrimaryBytes().getAndAdd(bytes);

                memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
                shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
            };
        }

        return () -> {
            long requestEndTime = System.currentTimeMillis();
            long requestLatency = requestEndTime - requestStartTime;

            currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
            currentPrimaryBytes.addAndGet(-bytes);
            tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().addAndGet(-bytes);
            tracker.memory().getCurrentPrimaryBytes().addAndGet(-bytes);
            tracker.latency().getPrimaryTimeInMillis().addAndGet(requestLatency);
            tracker.memory().getTotalCombinedCoordinatingAndPrimaryBytes().getAndAdd(bytes);
            tracker.memory().getTotalPrimaryBytes().getAndAdd(bytes);
            tracker.timeStamp().getLastSuccessfulPrimaryRequestTimestamp().set(requestEndTime);
            tracker.outstandingRequest().getTotalOutstandingPrimaryRequests().set(0);

            if(requestLatency > 0) {
                double requestThroughput = (double)bytes / requestLatency;
                tracker.throughput().getPrimaryThroughputMovingQueue().offer(requestThroughput);
                if(tracker.throughput().getPrimaryThroughputMovingQueue().size() > shardIndexingPressureSettings.getRequestSizeWindow()) {
                    double front = tracker.throughput().getPrimaryThroughputMovingQueue().poll();
                    double movingAverage = calculateMovingAverage(tracker.throughput().getPrimaryThroughputMovingAverage().get(), front,
                        requestThroughput, shardIndexingPressureSettings.getRequestSizeWindow());
                    tracker.throughput().getPrimaryThroughputMovingAverage().set(Double.doubleToLongBits(movingAverage));
                } else {
                    double movingAverage = (double) tracker.memory().getTotalPrimaryBytes().get() /
                        tracker.latency().getPrimaryTimeInMillis().get();
                    tracker.throughput().getPrimaryThroughputMovingAverage().set(Double.doubleToLongBits(movingAverage));
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
        long nodeReplicaBytes = currentReplicaBytes.addAndGet(bytes);
        long shardReplicaBytes = tracker.memory().getCurrentReplicaBytes().addAndGet(bytes);

        boolean shardLevelLimitBreached = false;
        if (forceExecution == false) {
            boolean nodeLevelLimitBreached = memoryManager.isReplicaNodeLimitBreached(tracker, nodeReplicaBytes);
            if (nodeLevelLimitBreached == false) {
                shardLevelLimitBreached = memoryManager.isReplicaShardLimitBreached(tracker, requestStartTime,
                        shardIndexingPressureStore.getShardIndexingPressureHotStore(), nodeReplicaBytes);
            }
            boolean shouldRejectRequest = nodeLevelLimitBreached ||
                (shardLevelLimitBreached && shardIndexingPressureSettings.isShardIndexingPressureEnforced());

            if (shouldRejectRequest) {
                long nodeReplicaBytesWithoutOperation = nodeReplicaBytes - bytes;
                long shardReplicaBytesWithoutOperation = shardReplicaBytes - bytes;

                currentReplicaBytes.addAndGet(-bytes);
                replicaRejections.getAndIncrement();
                tracker.memory().getCurrentReplicaBytes().getAndAdd(-bytes);
                tracker.rejection().getReplicaRejections().getAndIncrement();

                throw new OpenSearchRejectedExecutionException("rejected execution of replica operation [" +
                        "shard_detail=[" + shardId.getIndexName() + "][" + shardId.id() + "][R], " +
                        "shard_replica_bytes=" + shardReplicaBytesWithoutOperation + ", " +
                        "operation_bytes=" + bytes + ", " +
                        "max_coordinating_and_primary_bytes=" + tracker.getReplicaLimits() + "] OR [" +
                        "replica_bytes=" + nodeReplicaBytesWithoutOperation + ", " +
                        "operation_bytes=" + bytes + ", " +
                        "max_coordinating_and_primary_bytes=" + this.replicaLimits + "]", false);
            }
        }
        totalReplicaBytes.addAndGet(bytes);
        tracker.count().getReplicaCount().incrementAndGet();
        tracker.outstandingRequest().getTotalOutstandingReplicaRequests().incrementAndGet();

        // In shadow-mode if request was intended to rejected; it should only contribute to accounting limits and
        // should not influence dynamic parameters such as throughput
        if (shardLevelLimitBreached) {
            return () -> {
                currentReplicaBytes.addAndGet(-bytes);
                tracker.memory().getCurrentReplicaBytes().addAndGet(-bytes);
                tracker.memory().getTotalReplicaBytes().getAndAdd(bytes);

                memoryManager.decreaseShardReplicaLimits(tracker);
                shardIndexingPressureStore.tryIndexingPressureTrackerCleanup(tracker);
            };
        }

        return () -> {
            long requestEndTime = System.currentTimeMillis();
            long requestLatency = requestEndTime - requestStartTime;

            currentReplicaBytes.addAndGet(-bytes);
            tracker.memory().getCurrentReplicaBytes().addAndGet(-bytes);
            tracker.latency().getReplicaTimeInMillis().addAndGet(requestLatency);
            tracker.memory().getTotalReplicaBytes().getAndAdd(bytes);
            tracker.timeStamp().getLastSuccessfulReplicaRequestTimestamp().set(requestEndTime);
            tracker.outstandingRequest().getTotalOutstandingReplicaRequests().set(0);

            if(requestLatency > 0) {
                double requestThroughput = (double) bytes / requestLatency;
                tracker.throughput().getReplicaThroughputMovingQueue().offer(requestThroughput);
                if (tracker.throughput().getReplicaThroughputMovingQueue().size() > shardIndexingPressureSettings.getRequestSizeWindow()) {
                    double front = tracker.throughput().getReplicaThroughputMovingQueue().poll();
                    double movingAverage = calculateMovingAverage(tracker.throughput().getReplicaThroughputMovingAverage().get(), front,
                        requestThroughput, shardIndexingPressureSettings.getRequestSizeWindow());
                    tracker.throughput().getReplicaThroughputMovingAverage().set(Double.doubleToLongBits(movingAverage));
                } else {
                    double movingAverage = (double) tracker.memory().getTotalReplicaBytes().get() /
                        tracker.latency().getReplicaTimeInMillis().get();
                    tracker.throughput().getReplicaThroughputMovingAverage().set(Double.doubleToLongBits(movingAverage));
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

    public ShardIndexingPressureStats shardStats(CommonStatsFlags statsFlags) {

        if (statsFlags.includeOnlyTopIndexingPressureMetrics()) {
            return topStats();
        } else {
            ShardIndexingPressureStats allStats = shardStats();
            if (statsFlags.includeAllShardIndexingPressureTrackers()) {
                allStats.addAll(coldStats());
            }
            return allStats;
        }
    }

    ShardIndexingPressureStats shardStats() {
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
            memoryManager.totalThroughputDegradationLimitsBreachedRejections.get(),
            shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
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
            memoryManager.totalThroughputDegradationLimitsBreachedRejections.get(),
            shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
            isEnforcedMode);
    }

    ShardIndexingPressureStats topStats() {
        return new ShardIndexingPressureStats(Collections.emptyMap(), memoryManager.totalNodeLimitsBreachedRejections.get(),
            memoryManager.totalLastSuccessfulRequestLimitsBreachedRejections.get(),
            memoryManager.totalThroughputDegradationLimitsBreachedRejections.get(),
            shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
            shardIndexingPressureSettings.isShardIndexingPressureEnforced());
    }

    ShardIndexingPressureTracker getShardIndexingPressureTracker(ShardId shardId) {
        return shardIndexingPressureStore.getShardIndexingPressureTracker(shardId);
    }

    public boolean isShardIndexingPressureEnabled() {
        return shardIndexingPressureSettings.isShardIndexingPressureEnabled();
    }
}
