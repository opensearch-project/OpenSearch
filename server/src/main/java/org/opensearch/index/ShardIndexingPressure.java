/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.ShardIndexingPressureTracker.CommonOperationTracker;
import org.opensearch.index.ShardIndexingPressureTracker.OperationTracker;
import org.opensearch.index.ShardIndexingPressureTracker.PerformanceTracker;
import org.opensearch.index.ShardIndexingPressureTracker.RejectionTracker;
import org.opensearch.index.ShardIndexingPressureTracker.StatsTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.stats.ShardIndexingPressureStats;
import org.opensearch.index.stats.IndexingPressurePerShardStats;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shard Indexing Pressure is a framework level artefact build on top of IndexingPressure to track incoming indexing request, per shard.
 * The interfaces provided by this class will be used by Transport Action layers to start accounting for an incoming request.
 * Interfaces returns Releasable which when triggered will release the acquired accounting tokens values and also
 * perform necessary actions such as throughput evaluation once the request completes.
 * Consumers of these interfaces are expected to trigger close on releasable, reliably for consistency.
 *
 * Overall ShardIndexingPressure provides:
 * 1. Memory Accounting at shard level. This can be enabled/disabled based on dynamic setting.
 * 2. Memory Accounting at Node level. Tracking is done using the IndexingPressure artefacts to support feature seamless toggling.
 * 3. Interfaces to access the statistics for shard trackers.
 *
 * @opensearch.internal
 */
public class ShardIndexingPressure extends IndexingPressure {

    private static final Logger logger = LogManager.getLogger(ShardIndexingPressure.class);
    private final ShardIndexingPressureSettings shardIndexingPressureSettings;
    private final ShardIndexingPressureMemoryManager memoryManager;

    ShardIndexingPressure(Settings settings, ClusterService clusterService) {
        super(settings);
        shardIndexingPressureSettings = new ShardIndexingPressureSettings(clusterService, settings, primaryAndCoordinatingLimits);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.memoryManager = new ShardIndexingPressureMemoryManager(shardIndexingPressureSettings, clusterSettings, settings);
    }

    public Releasable markCoordinatingOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if (0 == bytes) {
            return () -> {};
        }

        long requestStartTime = System.nanoTime();
        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);
        long nodeCombinedBytes = currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long nodeReplicaBytes = currentReplicaBytes.get();
        long nodeTotalBytes = nodeCombinedBytes + nodeReplicaBytes;
        long shardCombinedBytes = tracker.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(bytes);

        boolean shardLevelLimitBreached = false;
        if (forceExecution == false) {
            boolean nodeLevelLimitBreached = memoryManager.isCoordinatingNodeLimitBreached(tracker, nodeTotalBytes);
            if (nodeLevelLimitBreached == false) {
                shardLevelLimitBreached = memoryManager.isCoordinatingShardLimitBreached(tracker, nodeTotalBytes, requestStartTime);
            }

            if (shouldRejectRequest(nodeLevelLimitBreached, shardLevelLimitBreached)) {
                coordinatingRejections.getAndIncrement();
                currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
                tracker.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
                rejectShardRequest(
                    tracker,
                    bytes,
                    nodeTotalBytes,
                    shardCombinedBytes,
                    tracker.getCoordinatingOperationTracker().getRejectionTracker(),
                    "coordinating"
                );
            }
        }
        currentCoordinatingBytes.addAndGet(bytes);
        totalCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        totalCoordinatingBytes.addAndGet(bytes);

        StatsTracker statsTracker = tracker.getCoordinatingOperationTracker().getStatsTracker();
        statsTracker.incrementCurrentBytes(bytes);
        markShardOperationStarted(statsTracker, tracker.getCoordinatingOperationTracker().getPerformanceTracker());
        boolean isShadowModeBreach = shardLevelLimitBreached;

        return wrapReleasable(() -> {
            currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
            currentCoordinatingBytes.addAndGet(-bytes);
            markShardOperationComplete(
                bytes,
                requestStartTime,
                isShadowModeBreach,
                tracker.getCoordinatingOperationTracker(),
                tracker.getCommonOperationTracker()
            );
            memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
            tryReleaseTracker(tracker);
        });
    }

    public Releasable markPrimaryOperationLocalToCoordinatingNodeStarted(ShardId shardId, long bytes) {
        if (bytes == 0) {
            return () -> {};
        }

        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);

        currentPrimaryBytes.addAndGet(bytes);
        totalPrimaryBytes.addAndGet(bytes);
        tracker.getPrimaryOperationTracker().getStatsTracker().incrementCurrentBytes(bytes);
        tracker.getPrimaryOperationTracker().getStatsTracker().incrementTotalBytes(bytes);

        return wrapReleasable(() -> {
            currentPrimaryBytes.addAndGet(-bytes);
            tracker.getPrimaryOperationTracker().getStatsTracker().incrementCurrentBytes(-bytes);
        });
    }

    public Releasable markPrimaryOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if (0 == bytes) {
            return () -> {};
        }

        long requestStartTime = System.nanoTime();
        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);
        long nodeCombinedBytes = currentCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long nodeReplicaBytes = currentReplicaBytes.get();
        long nodeTotalBytes = nodeCombinedBytes + nodeReplicaBytes;
        long shardCombinedBytes = tracker.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(bytes);

        boolean shardLevelLimitBreached = false;
        if (forceExecution == false) {
            boolean nodeLevelLimitBreached = memoryManager.isPrimaryNodeLimitBreached(tracker, nodeTotalBytes);
            if (nodeLevelLimitBreached == false) {
                shardLevelLimitBreached = memoryManager.isPrimaryShardLimitBreached(tracker, nodeTotalBytes, requestStartTime);
            }

            if (shouldRejectRequest(nodeLevelLimitBreached, shardLevelLimitBreached)) {
                primaryRejections.getAndIncrement();
                currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
                tracker.getCommonOperationTracker().incrementCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
                rejectShardRequest(
                    tracker,
                    bytes,
                    nodeTotalBytes,
                    shardCombinedBytes,
                    tracker.getPrimaryOperationTracker().getRejectionTracker(),
                    "primary"
                );
            }
        }
        currentPrimaryBytes.addAndGet(bytes);
        totalCombinedCoordinatingAndPrimaryBytes.addAndGet(bytes);
        totalPrimaryBytes.addAndGet(bytes);

        StatsTracker statsTracker = tracker.getPrimaryOperationTracker().getStatsTracker();
        statsTracker.incrementCurrentBytes(bytes);
        markShardOperationStarted(statsTracker, tracker.getPrimaryOperationTracker().getPerformanceTracker());
        boolean isShadowModeBreach = shardLevelLimitBreached;

        return wrapReleasable(() -> {
            currentCombinedCoordinatingAndPrimaryBytes.addAndGet(-bytes);
            currentPrimaryBytes.addAndGet(-bytes);
            markShardOperationComplete(
                bytes,
                requestStartTime,
                isShadowModeBreach,
                tracker.getPrimaryOperationTracker(),
                tracker.getCommonOperationTracker()
            );
            memoryManager.decreaseShardPrimaryAndCoordinatingLimits(tracker);
            tryReleaseTracker(tracker);
        });
    }

    public Releasable markReplicaOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if (0 == bytes) {
            return () -> {};
        }

        long requestStartTime = System.nanoTime();
        ShardIndexingPressureTracker tracker = getShardIndexingPressureTracker(shardId);
        long nodeReplicaBytes = currentReplicaBytes.addAndGet(bytes);
        long shardReplicaBytes = tracker.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(bytes);

        boolean shardLevelLimitBreached = false;
        if (forceExecution == false) {
            boolean nodeLevelLimitBreached = memoryManager.isReplicaNodeLimitBreached(tracker, nodeReplicaBytes);
            if (nodeLevelLimitBreached == false) {
                shardLevelLimitBreached = memoryManager.isReplicaShardLimitBreached(tracker, nodeReplicaBytes, requestStartTime);
            }

            if (shouldRejectRequest(nodeLevelLimitBreached, shardLevelLimitBreached)) {
                replicaRejections.getAndIncrement();
                currentReplicaBytes.addAndGet(-bytes);
                tracker.getReplicaOperationTracker().getStatsTracker().incrementCurrentBytes(-bytes);
                rejectShardRequest(
                    tracker,
                    bytes,
                    nodeReplicaBytes,
                    shardReplicaBytes,
                    tracker.getReplicaOperationTracker().getRejectionTracker(),
                    "replica"
                );
            }
        }
        totalReplicaBytes.addAndGet(bytes);

        StatsTracker statsTracker = tracker.getReplicaOperationTracker().getStatsTracker();
        markShardOperationStarted(statsTracker, tracker.getReplicaOperationTracker().getPerformanceTracker());
        boolean isShadowModeBreach = shardLevelLimitBreached;

        return wrapReleasable(() -> {
            currentReplicaBytes.addAndGet(-bytes);
            markShardOperationComplete(bytes, requestStartTime, isShadowModeBreach, tracker.getReplicaOperationTracker());
            memoryManager.decreaseShardReplicaLimits(tracker);
            tryReleaseTracker(tracker);
        });
    }

    private static Releasable wrapReleasable(Releasable releasable) {
        final AtomicBoolean called = new AtomicBoolean();
        return () -> {
            if (called.compareAndSet(false, true)) {
                releasable.close();
            } else {
                logger.error("ShardIndexingPressure Release is called twice", new IllegalStateException("Releasable is called twice"));
                assert false : "ShardIndexingPressure Release is called twice";
            }
        };
    }

    private boolean shouldRejectRequest(boolean nodeLevelLimitBreached, boolean shardLevelLimitBreached) {
        return nodeLevelLimitBreached || (shardLevelLimitBreached && shardIndexingPressureSettings.isShardIndexingPressureEnforced());
    }

    private void markShardOperationStarted(StatsTracker statsTracker, PerformanceTracker performanceTracker) {
        statsTracker.incrementRequestCount();
        performanceTracker.incrementTotalOutstandingRequests();
    }

    private void adjustPerformanceUponCompletion(
        long bytes,
        long requestStartTime,
        StatsTracker statsTracker,
        PerformanceTracker performanceTracker
    ) {
        long requestEndTime = System.nanoTime();
        long requestLatency = TimeUnit.NANOSECONDS.toMillis(requestEndTime - requestStartTime);

        performanceTracker.addLatencyInMillis(requestLatency);
        performanceTracker.updateLastSuccessfulRequestTimestamp(requestEndTime);
        performanceTracker.resetTotalOutstandingRequests();

        if (requestLatency > 0) {
            calculateRequestThroughput(bytes, requestLatency, performanceTracker, statsTracker);
        }
    }

    private void calculateRequestThroughput(
        long bytes,
        long requestLatency,
        PerformanceTracker performanceTracker,
        StatsTracker statsTracker
    ) {
        double requestThroughput = (double) bytes / requestLatency;
        performanceTracker.addNewThroughout(requestThroughput);
        if (performanceTracker.getThroughputMovingQueueSize() > shardIndexingPressureSettings.getRequestSizeWindow()) {
            double front = performanceTracker.getFirstThroughput();
            double movingAverage = memoryManager.calculateMovingAverage(
                performanceTracker.getThroughputMovingAverage(),
                front,
                requestThroughput,
                shardIndexingPressureSettings.getRequestSizeWindow()
            );
            performanceTracker.updateThroughputMovingAverage(Double.doubleToLongBits(movingAverage));
        } else {
            double movingAverage = (double) statsTracker.getTotalBytes() / performanceTracker.getLatencyInMillis();
            performanceTracker.updateThroughputMovingAverage(Double.doubleToLongBits(movingAverage));
        }
    }

    private void markShardOperationComplete(
        long bytes,
        long requestStartTime,
        boolean isShadowModeBreach,
        OperationTracker operationTracker,
        CommonOperationTracker commonOperationTracker
    ) {
        commonOperationTracker.incrementCurrentCombinedCoordinatingAndPrimaryBytes(-bytes);
        commonOperationTracker.incrementTotalCombinedCoordinatingAndPrimaryBytes(bytes);
        markShardOperationComplete(bytes, requestStartTime, isShadowModeBreach, operationTracker);
    }

    private void markShardOperationComplete(
        long bytes,
        long requestStartTime,
        boolean isShadowModeBreach,
        OperationTracker operationTracker
    ) {

        StatsTracker statsTracker = operationTracker.getStatsTracker();
        statsTracker.incrementCurrentBytes(-bytes);
        statsTracker.incrementTotalBytes(bytes);

        // In shadow mode if request was intended to be rejected, we do not account it for dynamic rejection parameters
        if (isShadowModeBreach == false) {
            adjustPerformanceUponCompletion(bytes, requestStartTime, statsTracker, operationTracker.getPerformanceTracker());
        }
    }

    private void tryReleaseTracker(ShardIndexingPressureTracker tracker) {
        memoryManager.tryTrackerCleanupFromHotStore(
            tracker,
            () -> (tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes() == 0
                && tracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes() == 0)
        );
    }

    private void rejectShardRequest(
        ShardIndexingPressureTracker tracker,
        long bytes,
        long nodeTotalBytes,
        long shardTotalBytes,
        RejectionTracker rejectionTracker,
        String operationType
    ) {
        long nodeBytesWithoutOperation = nodeTotalBytes - bytes;
        long shardBytesWithoutOperation = shardTotalBytes - bytes;
        ShardId shardId = tracker.getShardId();

        rejectionTracker.incrementTotalRejections();
        throw new OpenSearchRejectedExecutionException(
            "rejected execution of "
                + operationType
                + " operation ["
                + "shard_detail=["
                + shardId.getIndexName()
                + "]["
                + shardId.id()
                + "], "
                + "shard_total_bytes="
                + shardBytesWithoutOperation
                + ", "
                + "shard_operation_bytes="
                + bytes
                + ", "
                + "shard_max_coordinating_and_primary_bytes="
                + tracker.getPrimaryAndCoordinatingLimits()
                + ", "
                + "shard_max_replica_bytes="
                + tracker.getReplicaLimits()
                + "] OR ["
                + "node_total_bytes="
                + nodeBytesWithoutOperation
                + ", "
                + "node_operation_bytes="
                + bytes
                + ", "
                + "node_max_coordinating_and_primary_bytes="
                + primaryAndCoordinatingLimits
                + ", "
                + "node_max_replica_bytes="
                + replicaLimits
                + "]",
            false
        );
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
        Map<ShardId, IndexingPressurePerShardStats> statsPerShard = new HashMap<>();
        boolean isEnforcedMode = shardIndexingPressureSettings.isShardIndexingPressureEnforced();

        for (Map.Entry<ShardId, ShardIndexingPressureTracker> shardEntry : memoryManager.getShardIndexingPressureHotStore().entrySet()) {
            IndexingPressurePerShardStats shardStats = new IndexingPressurePerShardStats(shardEntry.getValue(), isEnforcedMode);
            statsPerShard.put(shardEntry.getKey(), shardStats);
        }
        return new ShardIndexingPressureStats(
            statsPerShard,
            memoryManager.getTotalNodeLimitsBreachedRejections(),
            memoryManager.getTotalLastSuccessfulRequestLimitsBreachedRejections(),
            memoryManager.getTotalThroughputDegradationLimitsBreachedRejections(),
            shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
            isEnforcedMode
        );
    }

    ShardIndexingPressureStats coldStats() {
        Map<ShardId, IndexingPressurePerShardStats> statsPerShard = new HashMap<>();
        boolean isEnforcedMode = shardIndexingPressureSettings.isShardIndexingPressureEnforced();

        for (Map.Entry<ShardId, ShardIndexingPressureTracker> shardEntry : memoryManager.getShardIndexingPressureColdStore().entrySet()) {
            IndexingPressurePerShardStats shardStats = new IndexingPressurePerShardStats(shardEntry.getValue(), isEnforcedMode);
            statsPerShard.put(shardEntry.getKey(), shardStats);
        }
        return new ShardIndexingPressureStats(
            statsPerShard,
            memoryManager.getTotalNodeLimitsBreachedRejections(),
            memoryManager.getTotalLastSuccessfulRequestLimitsBreachedRejections(),
            memoryManager.getTotalThroughputDegradationLimitsBreachedRejections(),
            shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
            isEnforcedMode
        );
    }

    ShardIndexingPressureStats topStats() {
        return new ShardIndexingPressureStats(
            Collections.emptyMap(),
            memoryManager.getTotalNodeLimitsBreachedRejections(),
            memoryManager.getTotalLastSuccessfulRequestLimitsBreachedRejections(),
            memoryManager.getTotalThroughputDegradationLimitsBreachedRejections(),
            shardIndexingPressureSettings.isShardIndexingPressureEnabled(),
            shardIndexingPressureSettings.isShardIndexingPressureEnforced()
        );
    }

    ShardIndexingPressureTracker getShardIndexingPressureTracker(ShardId shardId) {
        return memoryManager.getShardIndexingPressureTracker(shardId);
    }

    public boolean isShardIndexingPressureEnabled() {
        return shardIndexingPressureSettings.isShardIndexingPressureEnabled();
    }
}
