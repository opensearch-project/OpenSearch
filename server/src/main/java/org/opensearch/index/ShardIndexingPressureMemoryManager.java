/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.ShardIndexingPressureTracker.OperationTracker;
import org.opensearch.index.ShardIndexingPressureTracker.PerformanceTracker;
import org.opensearch.index.ShardIndexingPressureTracker.RejectionTracker;
import org.opensearch.index.ShardIndexingPressureTracker.StatsTracker;
import org.opensearch.core.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/**
 * The Shard Indexing Pressure Memory Manager is the construct responsible for increasing and decreasing the allocated shard limit
 * based on incoming requests. A shard limits defines the maximum memory that a shard can occupy in the heap for request objects.
 *
 * Based on the overall memory utilization on the node, and current traffic needs shard limits will be modified:
 *
 * 1. If the limits assigned to a shard is breached (Primary Parameter) while the node level overall occupancy across all shards
 * is not greater than primary_parameter.node.soft_limit, MemoryManager will increase the shard limits without any deeper evaluation.
 * 2. If the limits assigned to the shard is breached(Primary Parameter) and the node level overall occupancy across all shards
 * is greater than primary_parameter.node.soft_limit, then MemoryManager will evaluate deeper parameters for shards to identify any
 * issues, such as throughput degradation (Secondary Parameter - 1) and time since last request was successful (Secondary Parameter - 2).
 * This helps identify detect any duress state with the shard, requesting more memory.
 *
 * Secondary Parameters covered above:
 * 1. ThroughputDegradationLimitsBreached - When the moving window throughput average has increased by a factor compared to
 * the historical throughput average. If the factor by which it has increased is greater than the degradation limit threshold, this
 * parameter is considered to be breached.
 * 2. LastSuccessfulRequestDurationLimitsBreached - When the time since the last successful request completed is greater than the max
 * timeout threshold value, while there a number of outstanding requests greater than the max outstanding requests then this parameter
 * is considered to be breached.
 *
 * MemoryManager attempts to increase of decrease the shard limits in case the shard utilization goes below operating_factor.lower or
 * goes above operating_factor.upper of current shard limits. MemoryManager attempts to update the new shard limit such that the new value
 * remains withing the operating_factor.optimal range of current shard utilization.
 *
 * @opensearch.internal
 */
public class ShardIndexingPressureMemoryManager {
    private static final Logger logger = LogManager.getLogger(ShardIndexingPressureMemoryManager.class);

    /**
     * Shard operating factor can be evaluated using currentShardBytes/shardLimits. Outcome of this expression is categorized as
     * lower, optimal and upper boundary, and appropriate action is taken once the below defined threshold values are breached.
     */
    public static final Setting<Double> LOWER_OPERATING_FACTOR = Setting.doubleSetting(
        "shard_indexing_pressure.operating_factor.lower",
        0.75d,
        0.0d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Double> OPTIMAL_OPERATING_FACTOR = Setting.doubleSetting(
        "shard_indexing_pressure.operating_factor.optimal",
        0.85d,
        0.0d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Double> UPPER_OPERATING_FACTOR = Setting.doubleSetting(
        "shard_indexing_pressure.operating_factor.upper",
        0.95d,
        0.0d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This determines the max time elapsed since any request was processed successfully. Appropriate action is taken
     * once the below below defined threshold value is breached.
     */
    public static final Setting<TimeValue> SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT = Setting.positiveTimeSetting(
        "shard_indexing_pressure.secondary_parameter.successful_request.elapsed_timeout",
        TimeValue.timeValueMillis(300000),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This determines the max outstanding request that are yet to be processed successfully. Appropriate
     * action is taken once the below defined threshold value is breached.
     */
    public static final Setting<Integer> MAX_OUTSTANDING_REQUESTS = Setting.intSetting(
        "shard_indexing_pressure.secondary_parameter.successful_request.max_outstanding_requests",
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Degradation for a shard can be evaluated using average throughput of last N requests,
     * where N being {@link ShardIndexingPressureSettings#REQUEST_SIZE_WINDOW}, divided by lifetime average throughput.
     * Appropriate action is taken once the outcome of above expression breaches the below defined threshold value is breached.
     */
    public static final Setting<Double> THROUGHPUT_DEGRADATION_LIMITS = Setting.doubleSetting(
        "shard_indexing_pressure.secondary_parameter.throughput.degradation_factor",
        5.0d,
        1.0d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The node level soft limit determines when the secondary parameters for shard is to be evaluated for degradation.
     */
    public static final Setting<Double> NODE_SOFT_LIMIT = Setting.doubleSetting(
        "shard_indexing_pressure.primary_parameter.node.soft_limit",
        0.7d,
        0.0d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final AtomicLong totalNodeLimitsBreachedRejections = new AtomicLong();
    private final AtomicLong totalLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
    private final AtomicLong totalThroughputDegradationLimitsBreachedRejections = new AtomicLong();

    private final ShardIndexingPressureSettings shardIndexingPressureSettings;
    private final ShardIndexingPressureStore shardIndexingPressureStore;

    private volatile double lowerOperatingFactor;
    private volatile double optimalOperatingFactor;
    private volatile double upperOperatingFactor;

    private volatile TimeValue successfulRequestElapsedTimeout;
    private volatile int maxOutstandingRequests;

    private volatile double primaryAndCoordinatingThroughputDegradationLimits;
    private volatile double replicaThroughputDegradationLimits;

    private volatile double nodeSoftLimit;

    public ShardIndexingPressureMemoryManager(
        ShardIndexingPressureSettings shardIndexingPressureSettings,
        ClusterSettings clusterSettings,
        Settings settings
    ) {
        this.shardIndexingPressureSettings = shardIndexingPressureSettings;
        this.shardIndexingPressureStore = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);

        this.lowerOperatingFactor = LOWER_OPERATING_FACTOR.get(settings).doubleValue();
        clusterSettings.addSettingsUpdateConsumer(LOWER_OPERATING_FACTOR, this::setLowerOperatingFactor);

        this.optimalOperatingFactor = OPTIMAL_OPERATING_FACTOR.get(settings).doubleValue();
        clusterSettings.addSettingsUpdateConsumer(OPTIMAL_OPERATING_FACTOR, this::setOptimalOperatingFactor);

        this.upperOperatingFactor = UPPER_OPERATING_FACTOR.get(settings).doubleValue();
        clusterSettings.addSettingsUpdateConsumer(UPPER_OPERATING_FACTOR, this::setUpperOperatingFactor);

        this.successfulRequestElapsedTimeout = SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT, this::setSuccessfulRequestElapsedTimeout);

        this.maxOutstandingRequests = MAX_OUTSTANDING_REQUESTS.get(settings).intValue();
        clusterSettings.addSettingsUpdateConsumer(MAX_OUTSTANDING_REQUESTS, this::setMaxOutstandingRequests);

        this.primaryAndCoordinatingThroughputDegradationLimits = THROUGHPUT_DEGRADATION_LIMITS.get(settings).doubleValue();
        this.replicaThroughputDegradationLimits = this.primaryAndCoordinatingThroughputDegradationLimits * 1.5;
        clusterSettings.addSettingsUpdateConsumer(THROUGHPUT_DEGRADATION_LIMITS, this::setThroughputDegradationLimits);

        this.nodeSoftLimit = NODE_SOFT_LIMIT.get(settings).doubleValue();
        clusterSettings.addSettingsUpdateConsumer(NODE_SOFT_LIMIT, this::setNodeSoftLimit);
    }

    /**
     * Checks if the node level memory threshold is breached for coordinating operations.
     */
    boolean isCoordinatingNodeLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes) {
        if (nodeTotalBytes > this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) {
            logger.debug(
                "Node limits breached for coordinating operation [node_total_bytes={} , " + "node_primary_and_coordinating_limits={}]",
                nodeTotalBytes,
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()
            );
            incrementNodeLimitBreachedRejectionCount(tracker.getCoordinatingOperationTracker().getRejectionTracker());
            return true;
        }
        return false;
    }

    /**
     * Checks if the shard level memory threshold is breached for coordinating operations.
     */
    boolean isCoordinatingShardLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes, long requestStartTime) {
        // Shard memory limits is breached when the current utilization is greater than operating_factor.upper limit.
        long shardCombinedBytes = tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes();
        long shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits();
        boolean shardMemoryLimitsBreached = ((double) shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.upperOperatingFactor;

        if (shardMemoryLimitsBreached) {
            BooleanSupplier increaseShardLimitSupplier = () -> increaseShardLimits(
                tracker.getShardId(),
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits(),
                () -> tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes(),
                tracker::getPrimaryAndCoordinatingLimits,
                ShardIndexingPressureTracker::getPrimaryAndCoordinatingLimits,
                tracker::compareAndSetPrimaryAndCoordinatingLimits
            );

            return onShardLimitBreached(
                nodeTotalBytes,
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits(),
                requestStartTime,
                tracker.getCoordinatingOperationTracker(),
                increaseShardLimitSupplier
            );
        } else {
            return false;
        }
    }

    /**
     * Checks if the node level memory threshold is breached for primary operations.
     */
    boolean isPrimaryNodeLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes) {
        if (nodeTotalBytes > this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) {
            logger.debug(
                "Node limits breached for primary operation [node_total_bytes={}, " + "node_primary_and_coordinating_limits={}]",
                nodeTotalBytes,
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()
            );
            incrementNodeLimitBreachedRejectionCount(tracker.getPrimaryOperationTracker().getRejectionTracker());
            return true;
        }
        return false;
    }

    /**
     * Checks if the shard level memory threshold is breached for primary operations.
     */
    boolean isPrimaryShardLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes, long requestStartTime) {
        // Shard memory limits is breached when the current utilization is greater than operating_factor.upper limit.
        long shardCombinedBytes = tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes();
        long shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits();
        boolean shardMemoryLimitsBreached = ((double) shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.upperOperatingFactor;

        if (shardMemoryLimitsBreached) {
            BooleanSupplier increaseShardLimitSupplier = () -> increaseShardLimits(
                tracker.getShardId(),
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits(),
                () -> tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes(),
                tracker::getPrimaryAndCoordinatingLimits,
                ShardIndexingPressureTracker::getPrimaryAndCoordinatingLimits,
                tracker::compareAndSetPrimaryAndCoordinatingLimits
            );

            return onShardLimitBreached(
                nodeTotalBytes,
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits(),
                requestStartTime,
                tracker.getPrimaryOperationTracker(),
                increaseShardLimitSupplier
            );
        } else {
            return false;
        }
    }

    /**
     * Checks if the node level memory threshold is breached for replica operations.
     */
    boolean isReplicaNodeLimitBreached(ShardIndexingPressureTracker tracker, long nodeReplicaBytes) {
        if (nodeReplicaBytes > this.shardIndexingPressureSettings.getNodeReplicaLimits()) {
            logger.debug(
                "Node limits breached for replica operation [node_replica_bytes={} , " + "node_replica_limits={}]",
                nodeReplicaBytes,
                this.shardIndexingPressureSettings.getNodeReplicaLimits()
            );
            incrementNodeLimitBreachedRejectionCount(tracker.getReplicaOperationTracker().getRejectionTracker());
            return true;
        }
        return false;
    }

    /**
     * Checks if the shard level memory threshold is breached for replica operations.
     */
    boolean isReplicaShardLimitBreached(ShardIndexingPressureTracker tracker, long nodeReplicaBytes, long requestStartTime) {
        // Shard memory limits is breached when the current utilization is greater than operating_factor.upper limit.
        long shardReplicaBytes = tracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes();
        long shardReplicaLimits = tracker.getReplicaLimits();
        final boolean shardMemoryLimitsBreached = ((double) shardReplicaBytes / shardReplicaLimits) > this.upperOperatingFactor;

        if (shardMemoryLimitsBreached) {
            BooleanSupplier increaseShardLimitSupplier = () -> increaseShardLimits(
                tracker.getShardId(),
                this.shardIndexingPressureSettings.getNodeReplicaLimits(),
                () -> tracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes(),
                tracker::getReplicaLimits,
                ShardIndexingPressureTracker::getReplicaLimits,
                tracker::compareAndSetReplicaLimits
            );

            return onShardLimitBreached(
                nodeReplicaBytes,
                this.shardIndexingPressureSettings.getNodeReplicaLimits(),
                requestStartTime,
                tracker.getReplicaOperationTracker(),
                increaseShardLimitSupplier
            );
        } else {
            return false;
        }
    }

    void decreaseShardPrimaryAndCoordinatingLimits(ShardIndexingPressureTracker tracker) {
        decreaseShardLimits(
            tracker.getShardId(),
            () -> tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes(),
            tracker::getPrimaryAndCoordinatingLimits,
            tracker::compareAndSetPrimaryAndCoordinatingLimits,
            shardIndexingPressureSettings.getShardPrimaryAndCoordinatingBaseLimits()
        );
    }

    void decreaseShardReplicaLimits(ShardIndexingPressureTracker tracker) {
        decreaseShardLimits(
            tracker.getShardId(),
            () -> tracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes(),
            tracker::getReplicaLimits,
            tracker::compareAndSetReplicaLimits,
            shardIndexingPressureSettings.getShardReplicaBaseLimits()
        );
    }

    ShardIndexingPressureTracker getShardIndexingPressureTracker(ShardId shardId) {
        return shardIndexingPressureStore.getShardIndexingPressureTracker(shardId);
    }

    Map<ShardId, ShardIndexingPressureTracker> getShardIndexingPressureHotStore() {
        return shardIndexingPressureStore.getShardIndexingPressureHotStore();
    }

    Map<ShardId, ShardIndexingPressureTracker> getShardIndexingPressureColdStore() {
        return shardIndexingPressureStore.getShardIndexingPressureColdStore();
    }

    void tryTrackerCleanupFromHotStore(ShardIndexingPressureTracker tracker, BooleanSupplier condition) {
        shardIndexingPressureStore.tryTrackerCleanupFromHotStore(tracker, condition);
    }

    double calculateMovingAverage(long currentAverage, double frontValue, double currentValue, int count) {
        if (count > 0) {
            return ((Double.longBitsToDouble(currentAverage) * count) + currentValue - frontValue) / count;
        } else {
            return currentValue;
        }
    }

    long getTotalNodeLimitsBreachedRejections() {
        return totalNodeLimitsBreachedRejections.get();
    }

    long getTotalLastSuccessfulRequestLimitsBreachedRejections() {
        return totalLastSuccessfulRequestLimitsBreachedRejections.get();
    }

    long getTotalThroughputDegradationLimitsBreachedRejections() {
        return totalThroughputDegradationLimitsBreachedRejections.get();
    }

    /**
     * Verifies and returns true if the shard limit is hard-breached i.e. shard limit cannot be increased further. Otherwise
     * increases the shard limit and returns false.
     */
    private boolean onShardLimitBreached(
        long nodeTotalBytes,
        long nodeLimit,
        long requestStartTime,
        OperationTracker operationTracker,
        BooleanSupplier increaseShardLimitSupplier
    ) {

        // Secondary Parameters (i.e. LastSuccessfulRequestDuration and Throughput) is taken into consideration when
        // the current node utilization is greater than primary_parameter.node.soft_limit of total node limits.
        if (((double) nodeTotalBytes / nodeLimit) < this.nodeSoftLimit) {
            boolean isShardLimitsIncreased = increaseShardLimitSupplier.getAsBoolean();
            if (isShardLimitsIncreased == false) {
                incrementNodeLimitBreachedRejectionCount(operationTracker.getRejectionTracker());
            }
            return !isShardLimitsIncreased;
        } else {
            boolean shardLastSuccessfulRequestDurationLimitsBreached = evaluateLastSuccessfulRequestDurationLimitsBreached(
                operationTracker.getPerformanceTracker(),
                requestStartTime
            );

            if (shardLastSuccessfulRequestDurationLimitsBreached) {
                operationTracker.getRejectionTracker().incrementLastSuccessfulRequestLimitsBreachedRejections();
                this.totalLastSuccessfulRequestLimitsBreachedRejections.incrementAndGet();
                return true;
            }

            boolean shardThroughputDegradationLimitsBreached = evaluateThroughputDegradationLimitsBreached(
                operationTracker.getPerformanceTracker(),
                operationTracker.getStatsTracker(),
                primaryAndCoordinatingThroughputDegradationLimits
            );

            if (shardThroughputDegradationLimitsBreached) {
                operationTracker.getRejectionTracker().incrementThroughputDegradationLimitsBreachedRejections();
                this.totalThroughputDegradationLimitsBreachedRejections.incrementAndGet();
                return true;
            }

            boolean isShardLimitsIncreased = increaseShardLimitSupplier.getAsBoolean();
            if (isShardLimitsIncreased == false) {
                incrementNodeLimitBreachedRejectionCount(operationTracker.getRejectionTracker());
            }
            return !isShardLimitsIncreased;
        }
    }

    private boolean increaseShardLimits(
        ShardId shardId,
        long nodeLimit,
        LongSupplier shardCurrentBytesSupplier,
        LongSupplier shardLimitSupplier,
        ToLongFunction<ShardIndexingPressureTracker> getShardLimitFunction,
        BiPredicate<Long, Long> updateShardLimitPredicate
    ) {
        long currentShardLimit;
        long newShardLimit;
        do {
            currentShardLimit = shardLimitSupplier.getAsLong();
            long shardCurrentBytes = shardCurrentBytesSupplier.getAsLong();

            if (((double) shardCurrentBytes / currentShardLimit) > this.upperOperatingFactor) {
                newShardLimit = (long) (shardCurrentBytes / this.optimalOperatingFactor);
                long totalShardLimitsExceptCurrentShard = this.shardIndexingPressureStore.getShardIndexingPressureHotStore()
                    .entrySet()
                    .stream()
                    .filter(entry -> (shardId != entry.getKey()))
                    .map(Map.Entry::getValue)
                    .mapToLong(getShardLimitFunction)
                    .sum();

                if (totalShardLimitsExceptCurrentShard + newShardLimit > nodeLimit) {
                    logger.debug(
                        "Failed To Increase Shard Limit [shard_detail=[{}][{}}], "
                            + "shard_current_limit_bytes={}, "
                            + "total_shard_limits_bytes_except_current_shard={}, "
                            + "expected_shard_limits_bytes={}]",
                        shardId.getIndexName(),
                        shardId.id(),
                        currentShardLimit,
                        totalShardLimitsExceptCurrentShard,
                        newShardLimit
                    );
                    return false;
                }
            } else {
                return true;
            }
        } while (!updateShardLimitPredicate.test(currentShardLimit, newShardLimit));

        logger.debug(
            "Increased Shard Limit [" + "shard_detail=[{}][{}], old_shard_limit_bytes={}, " + "new_shard_limit_bytes={}]",
            shardId.getIndexName(),
            shardId.id(),
            currentShardLimit,
            newShardLimit
        );
        return true;
    }

    private void decreaseShardLimits(
        ShardId shardId,
        LongSupplier shardCurrentBytesSupplier,
        LongSupplier shardLimitSupplier,
        BiPredicate<Long, Long> updateShardLimitPredicate,
        long shardBaseLimit
    ) {

        long currentShardLimit;
        long newShardLimit;
        do {
            currentShardLimit = shardLimitSupplier.getAsLong();
            long shardCurrentBytes = shardCurrentBytesSupplier.getAsLong();
            newShardLimit = Math.max((long) (shardCurrentBytes / this.optimalOperatingFactor), shardBaseLimit);

            if (((double) shardCurrentBytes / currentShardLimit) > this.lowerOperatingFactor) {
                logger.debug(
                    "Shard Limits Already Decreased ["
                        + "shard_detail=[{}][{}], "
                        + "current_shard_limit_bytes={}, "
                        + "expected_shard_limit_bytes={}]",
                    shardId.getIndexName(),
                    shardId.id(),
                    currentShardLimit,
                    newShardLimit
                );
                return;
            }
        } while (!updateShardLimitPredicate.test(currentShardLimit, newShardLimit));

        logger.debug(
            "Decreased Shard Limit [shard_detail=[{}][{}], " + "old_shard_limit_bytes={}, new_shard_limit_bytes={}]",
            shardId.getIndexName(),
            shardId.id(),
            currentShardLimit,
            newShardLimit
        );
    }

    /**
     * This evaluation returns true if throughput of last N request divided by the total lifetime requests throughput is greater than
     * the degradation limits threshold.
     */
    private boolean evaluateThroughputDegradationLimitsBreached(
        PerformanceTracker performanceTracker,
        StatsTracker statsTracker,
        double degradationLimits
    ) {
        double throughputMovingAverage = Double.longBitsToDouble(performanceTracker.getThroughputMovingAverage());
        long throughputMovingQueueSize = performanceTracker.getThroughputMovingQueueSize();
        double throughputHistoricalAverage = (double) statsTracker.getTotalBytes() / performanceTracker.getLatencyInMillis();
        return throughputMovingAverage > 0
            && throughputMovingQueueSize >= this.shardIndexingPressureSettings.getRequestSizeWindow()
            && throughputHistoricalAverage / throughputMovingAverage > degradationLimits;
    }

    /**
     * This evaluation returns true if the difference in the current timestamp and last successful request timestamp is greater than
     * the successful request elapsed-timeout threshold, and the total number of outstanding requests is greater than
     * the maximum outstanding request-count threshold.
     */
    private boolean evaluateLastSuccessfulRequestDurationLimitsBreached(PerformanceTracker performanceTracker, long requestStartTime) {
        return (performanceTracker.getLastSuccessfulRequestTimestamp() > 0)
            && (requestStartTime - performanceTracker.getLastSuccessfulRequestTimestamp()) > this.successfulRequestElapsedTimeout.nanos()
            && performanceTracker.getTotalOutstandingRequests() > this.maxOutstandingRequests;
    }

    private void setLowerOperatingFactor(double lowerOperatingFactor) {
        this.lowerOperatingFactor = lowerOperatingFactor;
    }

    private void setOptimalOperatingFactor(double optimalOperatingFactor) {
        this.optimalOperatingFactor = optimalOperatingFactor;
    }

    private void setUpperOperatingFactor(double upperOperatingFactor) {
        this.upperOperatingFactor = upperOperatingFactor;
    }

    private void setSuccessfulRequestElapsedTimeout(TimeValue successfulRequestElapsedTimeout) {
        this.successfulRequestElapsedTimeout = successfulRequestElapsedTimeout;
    }

    private void setMaxOutstandingRequests(int maxOutstandingRequests) {
        this.maxOutstandingRequests = maxOutstandingRequests;
    }

    private void setThroughputDegradationLimits(double throughputDegradationLimits) {
        this.primaryAndCoordinatingThroughputDegradationLimits = throughputDegradationLimits;
        this.replicaThroughputDegradationLimits = this.primaryAndCoordinatingThroughputDegradationLimits * 1.5;
    }

    private void setNodeSoftLimit(double nodeSoftLimit) {
        this.nodeSoftLimit = nodeSoftLimit;
    }

    private void incrementNodeLimitBreachedRejectionCount(RejectionTracker rejectionTracker) {
        rejectionTracker.incrementNodeLimitsBreachedRejections();
        this.totalNodeLimitsBreachedRejections.incrementAndGet();
    }
}
