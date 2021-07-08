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
import org.opensearch.index.ShardIndexingPressureTracker.PerformanceTracker;
import org.opensearch.index.ShardIndexingPressureTracker.StatsTracker;
import org.opensearch.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
 */
public class ShardIndexingPressureMemoryManager {
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Operating factor can be evaluated using currentShardBytes/shardLimits. Outcome of this expression is categorized as
     * lower, optimal and upper boundary, and appropriate action is taken once they breach the value mentioned below.
     */
    public static final Setting<Double> LOWER_OPERATING_FACTOR =
        Setting.doubleSetting("shard_indexing_pressure.operating_factor.lower", 0.75d, 0.0d,
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<Double> OPTIMAL_OPERATING_FACTOR =
        Setting.doubleSetting("shard_indexing_pressure.operating_factor.optimal", 0.85d, 0.0d,
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<Double> UPPER_OPERATING_FACTOR =
        Setting.doubleSetting("shard_indexing_pressure.operating_factor.upper", 0.95d, 0.0d,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * This is the max time that can be elapsed after any request is processed successfully. Appropriate action is taken
     * once the below mentioned value is breached.
     */
    public static final Setting<Integer> SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT =
        Setting.intSetting("shard_indexing_pressure.secondary_parameter.successful_request.elapsed_timeout", 300000,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * This is the max outstanding request that are present after any request is processed successfully. Appropriate
     * action is taken once the below mentioned value is breached.
     */
    public static final Setting<Integer> MAX_OUTSTANDING_REQUESTS =
        Setting.intSetting("shard_indexing_pressure.secondary_parameter.successful_request.max_outstanding_requests",
            100, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Degradation limits can be evaluated using average throughput last N requests
     * and N being {@link ShardIndexingPressureSettings#REQUEST_SIZE_WINDOW} divided by lifetime average throughput.
     * Appropriate action is taken once the outcome of above expression breaches the below mentioned factor
     */
    public static final Setting<Double> THROUGHPUT_DEGRADATION_LIMITS =
        Setting.doubleSetting("shard_indexing_pressure.secondary_parameter.throughput.degradation_factor", 5.0d, 1.0d,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * The secondary parameter accounting factor tells when the secondary parameter is considered. i.e. If the current
     * node level memory utilization divided by the node limits is greater than 70% then appropriate action is taken.
     */
    public static final Setting<Double> NODE_SOFT_LIMIT =
        Setting.doubleSetting("shard_indexing_pressure.primary_parameter.node.soft_limit", 0.7d, 0.0d,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public final AtomicLong totalNodeLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong totalLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong totalThroughputDegradationLimitsBreachedRejections = new AtomicLong();

    private final ShardIndexingPressureSettings shardIndexingPressureSettings;
    private final ShardIndexingPressureStore shardIndexingPressureStore;

    private volatile double lowerOperatingFactor;
    private volatile double optimalOperatingFactor;
    private volatile double upperOperatingFactor;

    private volatile int successfulRequestElapsedTimeout;
    private volatile int maxOutstandingRequests;

    private volatile double primaryAndCoordinatingThroughputDegradationLimits;
    private volatile double replicaThroughputDegradationLimits;

    private volatile double nodeSoftLimit;

    public ShardIndexingPressureMemoryManager(ShardIndexingPressureSettings shardIndexingPressureSettings,
                                              ClusterSettings clusterSettings, Settings settings) {
        this.shardIndexingPressureSettings = shardIndexingPressureSettings;
        this.shardIndexingPressureStore = new ShardIndexingPressureStore(shardIndexingPressureSettings, clusterSettings, settings);

        this.lowerOperatingFactor = LOWER_OPERATING_FACTOR.get(settings).doubleValue();
        clusterSettings.addSettingsUpdateConsumer(LOWER_OPERATING_FACTOR, this::setLowerOperatingFactor);

        this.optimalOperatingFactor = OPTIMAL_OPERATING_FACTOR.get(settings).doubleValue();
        clusterSettings.addSettingsUpdateConsumer(OPTIMAL_OPERATING_FACTOR, this::setOptimalOperatingFactor);

        this.upperOperatingFactor = UPPER_OPERATING_FACTOR.get(settings).doubleValue();
        clusterSettings.addSettingsUpdateConsumer(UPPER_OPERATING_FACTOR, this::setUpperOperatingFactor);

        this.successfulRequestElapsedTimeout = SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.get(settings).intValue();
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
     * Checks if the node level memory threshold is breached for primary operations.
     */
    boolean isPrimaryNodeLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes) {

        if(nodeTotalBytes > this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) {
            logger.debug("Node limits breached for primary operation [node_total_bytes={}, " +
                    "node_primary_and_coordinating_limits={}]", nodeTotalBytes,
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits());
            tracker.getPrimaryOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
            totalNodeLimitsBreachedRejections.incrementAndGet();

            return true;
        }
        return false;
    }

    /**
     * Checks if the shard level memory threshold is breached for primary operations.
     */
    boolean isPrimaryShardLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes, long requestStartTime) {

        // Memory limits is breached when the current utilization is greater than operating_factor.upper of total shard limits.
        long shardCombinedBytes = tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes();
        long shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits();
        boolean shardMemoryLimitsBreached = ((double)shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.upperOperatingFactor;

        if(shardMemoryLimitsBreached) {
            // Secondary Parameters (i.e. LastSuccessfulRequestDuration and Throughput) is taken into consideration when
            // the current node utilization is greater than primary_parameter.node.soft_limit of total node limits.
            if(((double)nodeTotalBytes / this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) < this.nodeSoftLimit) {
                boolean isShardLimitsIncreased = this.increaseShardPrimaryAndCoordinatingLimits(tracker);
                if(isShardLimitsIncreased == false) {
                    tracker.getPrimaryOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }
                return !isShardLimitsIncreased;
            } else {
                boolean shardLastSuccessfulRequestDurationLimitsBreached =
                    this.evaluateLastSuccessfulRequestDurationLimitsBreached(tracker.getPrimaryOperationTracker().getPerformanceTracker(),
                        requestStartTime);

                if(shardLastSuccessfulRequestDurationLimitsBreached) {
                    tracker.getPrimaryOperationTracker().getRejectionTracker()
                        .incrementLastSuccessfulRequestLimitsBreachedRejections();
                    totalLastSuccessfulRequestLimitsBreachedRejections.incrementAndGet();
                    return true;
                }

                boolean shardThroughputDegradationLimitsBreached =
                    this.evaluateThroughputDegradationLimitsBreached(tracker.getPrimaryOperationTracker().getPerformanceTracker(),
                        tracker.getPrimaryOperationTracker().getStatsTracker(),
                        primaryAndCoordinatingThroughputDegradationLimits);

                if (shardThroughputDegradationLimitsBreached) {
                    tracker.getPrimaryOperationTracker().getRejectionTracker()
                        .incrementThroughputDegradationLimitsBreachedRejections();
                    totalThroughputDegradationLimitsBreachedRejections.incrementAndGet();
                    return true;
                }

                boolean isShardLimitsIncreased = this.increaseShardPrimaryAndCoordinatingLimits(tracker);
                if(isShardLimitsIncreased == false) {
                    tracker.getPrimaryOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }

                return !isShardLimitsIncreased;
            }
        } else {
            return false;
        }
    }

    boolean isCoordinatingNodeLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes) {

        //Checks if the node level threshold is breached.
        if(nodeTotalBytes > this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) {
            logger.debug("Node limits breached for coordinating operation [node_total_bytes={} , " +
                    "node_primary_and_coordinating_limits={}]", nodeTotalBytes,
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits());
            tracker.getCoordinatingOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
            totalNodeLimitsBreachedRejections.incrementAndGet();

            return true;
        }
        return false;
    }

    boolean isCoordinatingShardLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes, long requestStartTime) {

        //Shard memory limit is breached when the current utilization is greater than operating_factor.upper of total shard limits.
        long shardCombinedBytes = tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes();
        long shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits();
        boolean shardMemoryLimitsBreached = ((double)shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.upperOperatingFactor;

        if(shardMemoryLimitsBreached) {
            /*
            Secondary Parameters(i.e. LastSuccessfulRequestDuration and Throughput) is taken into consideration when
            the current node utilization is greater than primary_parameter.node.soft_limit of total node limits.
             */
            if(((double)nodeTotalBytes / this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) < this.nodeSoftLimit) {
                boolean isShardLimitsIncreased = this.increaseShardPrimaryAndCoordinatingLimits(tracker);
                if(isShardLimitsIncreased == false) {
                    tracker.getCoordinatingOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }

                return !isShardLimitsIncreased;
            } else {
                boolean shardLastSuccessfulRequestDurationLimitsBreached =
                    this.evaluateLastSuccessfulRequestDurationLimitsBreached(tracker.getCoordinatingOperationTracker()
                        .getPerformanceTracker(), requestStartTime);

                if(shardLastSuccessfulRequestDurationLimitsBreached) {
                    tracker.getCoordinatingOperationTracker().getRejectionTracker()
                        .incrementLastSuccessfulRequestLimitsBreachedRejections();
                    totalLastSuccessfulRequestLimitsBreachedRejections.incrementAndGet();
                    return true;
                }

                boolean shardThroughputDegradationLimitsBreached =
                    this.evaluateThroughputDegradationLimitsBreached(tracker.getCoordinatingOperationTracker().getPerformanceTracker(),
                        tracker.getCoordinatingOperationTracker().getStatsTracker(),
                        primaryAndCoordinatingThroughputDegradationLimits);

                if (shardThroughputDegradationLimitsBreached) {
                    tracker.getCoordinatingOperationTracker().getRejectionTracker()
                        .incrementThroughputDegradationLimitsBreachedRejections();
                    totalThroughputDegradationLimitsBreachedRejections.incrementAndGet();
                    return true;
                }

                boolean isShardLimitsIncreased =
                    this.increaseShardPrimaryAndCoordinatingLimits(tracker);
                if(isShardLimitsIncreased == false) {
                    tracker.getCoordinatingOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }

                return !isShardLimitsIncreased;
            }
        } else {
            return false;
        }
    }

    boolean isReplicaNodeLimitBreached(ShardIndexingPressureTracker tracker, long nodeReplicaBytes) {

        //Checks if the node level threshold is breached.
        if(nodeReplicaBytes > this.shardIndexingPressureSettings.getNodeReplicaLimits()) {
            logger.debug("Node limits breached for replica operation [node_replica_bytes={} , " +
                "node_replica_limits={}]", nodeReplicaBytes, this.shardIndexingPressureSettings.getNodeReplicaLimits());
            tracker.getReplicaOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
            totalNodeLimitsBreachedRejections.incrementAndGet();

            return true;
        }
        return false;
    }

    boolean isReplicaShardLimitBreached(ShardIndexingPressureTracker tracker, long nodeReplicaBytes, long requestStartTime) {

        //Memory limits is breached when the current utilization is greater than operating_factor.upper of total shard limits.
        long shardReplicaBytes = tracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes();
        long shardReplicaLimits = tracker.getReplicaLimits();
        final boolean shardMemoryLimitsBreached =
            ((double)shardReplicaBytes / shardReplicaLimits) > this.upperOperatingFactor;

        if(shardMemoryLimitsBreached) {
            /*
            Secondary Parameters(i.e. LastSuccessfulRequestDuration and Throughput) is taken into consideration when
            the current node utilization is greater than primary_parameter.node.soft_limit of total node limits.
             */
            if(((double)nodeReplicaBytes / this.shardIndexingPressureSettings.getNodeReplicaLimits()) < this.nodeSoftLimit)  {
                boolean isShardLimitsIncreased = this.increaseShardReplicaLimits(tracker);
                if(isShardLimitsIncreased == false) {
                    tracker.getReplicaOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }

                return !isShardLimitsIncreased;
            } else {
                boolean shardLastSuccessfulRequestDurationLimitsBreached =
                    this.evaluateLastSuccessfulRequestDurationLimitsBreached(tracker.getReplicaOperationTracker().getPerformanceTracker(),
                        requestStartTime);

                if(shardLastSuccessfulRequestDurationLimitsBreached) {
                    tracker.getReplicaOperationTracker().getRejectionTracker().incrementLastSuccessfulRequestLimitsBreachedRejections();
                    totalLastSuccessfulRequestLimitsBreachedRejections.incrementAndGet();
                    return  true;
                }

                boolean shardThroughputDegradationLimitsBreached =
                    this.evaluateThroughputDegradationLimitsBreached(tracker.getReplicaOperationTracker().getPerformanceTracker(),
                        tracker.getReplicaOperationTracker().getStatsTracker(),
                        replicaThroughputDegradationLimits);

                if (shardThroughputDegradationLimitsBreached) {
                    tracker.getReplicaOperationTracker().getRejectionTracker().incrementThroughputDegradationLimitsBreachedRejections();
                    totalThroughputDegradationLimitsBreachedRejections.incrementAndGet();
                    return true;
                }

                boolean isShardLimitsIncreased = this.increaseShardReplicaLimits(tracker);
                if(isShardLimitsIncreased == false) {
                    tracker.getReplicaOperationTracker().getRejectionTracker().incrementNodeLimitsBreachedRejections();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }

                return !isShardLimitsIncreased;
            }
        } else {
            return false;
        }
    }

    private boolean increaseShardPrimaryAndCoordinatingLimits(ShardIndexingPressureTracker tracker) {
        long shardPrimaryAndCoordinatingLimits;
        long newShardPrimaryAndCoordinatingLimits;
        do {
            shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits();
            long shardCombinedBytes = tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes();
            newShardPrimaryAndCoordinatingLimits = (long)(shardCombinedBytes / this.optimalOperatingFactor);

            long totalPrimaryAndCoordinatingLimitsExceptCurrentShard = shardIndexingPressureStore.getShardIndexingPressureHotStore()
                .entrySet().stream()
                .filter(entry -> (tracker.getShardId() != entry.getKey()))
                .map(Map.Entry::getValue)
                .mapToLong(ShardIndexingPressureTracker::getPrimaryAndCoordinatingLimits).sum();

            if(((double)shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.upperOperatingFactor) {
                if (totalPrimaryAndCoordinatingLimitsExceptCurrentShard + newShardPrimaryAndCoordinatingLimits >
                    this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) {
                    logger.debug("Failed to increase the Primary And Coordinating Limits [shard_detail=[{}][{}}], " +
                            "shard_max_primary_and_coordinating_bytes={}, " +
                            "total_max_primary_and_coordinating_bytes_except_current_shard={}, " +
                            "expected_shard_max_primary_and_coordinating_bytes={}, node_max_coordinating_and_primary_bytes={}]",
                        tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardPrimaryAndCoordinatingLimits,
                        totalPrimaryAndCoordinatingLimitsExceptCurrentShard, newShardPrimaryAndCoordinatingLimits,
                        this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits());
                    return false;
                }
            } else {
                return true;
            }
        } while(!tracker.compareAndSetPrimaryAndCoordinatingLimits(shardPrimaryAndCoordinatingLimits,
            newShardPrimaryAndCoordinatingLimits));

        logger.debug("Increased the Primary And Coordinating Limits [" +
                "shard_detail=[{}][{}], old_shard_max_primary_and_coordinating_bytes={}, " +
                "new_shard_max_primary_and_coordinating_bytes={}]",
            tracker.getShardId().getIndexName(), tracker.getShardId().id(),
            shardPrimaryAndCoordinatingLimits, newShardPrimaryAndCoordinatingLimits);
        return true;
    }

    void decreaseShardPrimaryAndCoordinatingLimits(ShardIndexingPressureTracker tracker) {
        long shardPrimaryAndCoordinatingLimits;
        long newShardPrimaryAndCoordinatingLimits;
        do {
            shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits();
            long shardCombinedBytes = tracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes();
            newShardPrimaryAndCoordinatingLimits = Math.max((long) (shardCombinedBytes / this.optimalOperatingFactor),
                this.shardIndexingPressureSettings.getShardPrimaryAndCoordinatingBaseLimits());

            if (((double)shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.lowerOperatingFactor) {
                logger.debug("Primary And Coordinating Limits Already Decreased [" +
                        "shard_detail=[{}][{}], " + "shard_max_primary_and_coordinating_bytes={}, " +
                        "expected_shard_max_primary_and_coordinating_bytes={}]",
                    tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardPrimaryAndCoordinatingLimits,
                    newShardPrimaryAndCoordinatingLimits);
                return;
            }
        } while(!tracker.compareAndSetPrimaryAndCoordinatingLimits(shardPrimaryAndCoordinatingLimits,
            newShardPrimaryAndCoordinatingLimits));

        logger.debug("Decreased the Primary And Coordinating Limits [shard_detail=[{}][{}], " +
                "shard_max_primary_and_coordinating_bytes={}, new_shard_max_primary_and_coordinating_bytes={}]",
            tracker.getShardId().getIndexName(), tracker.getShardId().id(),
            shardPrimaryAndCoordinatingLimits, newShardPrimaryAndCoordinatingLimits);
    }

    private boolean increaseShardReplicaLimits(ShardIndexingPressureTracker tracker) {
        long shardReplicaLimits;
        long newShardReplicaLimits;
        do {
            shardReplicaLimits = tracker.getReplicaLimits();
            long shardReplicaBytes = tracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes();
            newShardReplicaLimits = (long)(shardReplicaBytes / this.optimalOperatingFactor);

            long totalReplicaLimitsExceptCurrentShard = shardIndexingPressureStore.getShardIndexingPressureHotStore()
                .entrySet().stream()
                .filter(entry -> (tracker.getShardId() != entry.getKey()))
                .map(Map.Entry::getValue)
                .mapToLong(ShardIndexingPressureTracker::getReplicaLimits).sum();

            if(((double)shardReplicaBytes / shardReplicaLimits) > this.upperOperatingFactor) {
                if (totalReplicaLimitsExceptCurrentShard + newShardReplicaLimits >
                    this.shardIndexingPressureSettings.getNodeReplicaLimits()) {
                    logger.debug("Failed to increase the Replica Limits [shard_detail=[{}][{}], " +
                            "shard_max_replica_bytes={}, total_max_replica_except_current_shard={}}, " +
                            "expected_shard_max_replica_bytes={}, node_max_replica_bytes={}]",
                        tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardReplicaLimits,
                        totalReplicaLimitsExceptCurrentShard, newShardReplicaLimits,
                        this.shardIndexingPressureSettings.getNodeReplicaLimits());
                    return false;
                }
            } else {
                return true;
            }
        } while(!tracker.compareAndSetReplicaLimits(shardReplicaLimits, newShardReplicaLimits));

        logger.debug("Increased the Replica Limits [shard_detail=[{}][{}], " +
                "old_shard_max_replica_bytes={}, new_expected_shard_max_replica_bytes={}]",
            tracker.getShardId().getIndexName(), tracker.getShardId().id(),
            shardReplicaLimits, newShardReplicaLimits);

        return true;
    }

    void decreaseShardReplicaLimits(ShardIndexingPressureTracker tracker) {

        long shardReplicaLimits;
        long newShardReplicaLimits;
        do {
            shardReplicaLimits = tracker.getReplicaLimits();
            long shardReplicaBytes = tracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes();
            newShardReplicaLimits = Math.max((long) (shardReplicaBytes / this.optimalOperatingFactor),
                this.shardIndexingPressureSettings.getShardReplicaBaseLimits());

            if (((double)shardReplicaBytes / shardReplicaLimits) > this.lowerOperatingFactor) {
                logger.debug("Replica Limits Already Increased [shard_detail=[{}][{}], " +
                    "shard_max_replica_bytes={}, expected_shard_max_replica_bytes={}]",
                tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardReplicaLimits,
                newShardReplicaLimits);
                return;
            }
        } while(!tracker.compareAndSetReplicaLimits(shardReplicaLimits, newShardReplicaLimits));

        logger.debug("Decreased the Replica Limits [shard_detail=[{}}][{}}], " +
                "shard_max_replica_bytes={}, expected_shard_max_replica_bytes={}]",
            tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardReplicaLimits,
            newShardReplicaLimits);
    }

    ShardIndexingPressureTracker getShardIndexingPressureTracker(ShardId shardId) {
        return shardIndexingPressureStore.getShardIndexingPressureTracker(shardId);
    }

    Map<ShardId, ShardIndexingPressureTracker> getShardIndexingPressureHotStore() {
        return shardIndexingPressureStore.getShardIndexingPressureHotStore();
    }

    /**
     * Throughput of last N request divided by the total lifetime requests throughput is greater than the acceptable
     * degradation limits then we say this parameter has breached the threshold.
     */
    private boolean  evaluateThroughputDegradationLimitsBreached(PerformanceTracker performanceTracker, StatsTracker statsTracker,
                                                                 double degradationLimits) {
        double throughputMovingAverage =  Double.longBitsToDouble(performanceTracker.getThroughputMovingAverage());
        long throughputMovingQueueSize = performanceTracker.getThroughputMovingQueueSize();
        double throughputHistoricalAverage = (double)statsTracker.getTotalBytes() / performanceTracker.getLatencyInMillis();
        return throughputMovingAverage > 0 && throughputMovingQueueSize >= shardIndexingPressureSettings.getRequestSizeWindow() &&
            throughputHistoricalAverage / throughputMovingAverage > degradationLimits;
    }

    /**
     * The difference in the current timestamp and last successful request timestamp is greater than
     * successful request elapsed timeout value and the total number of outstanding requests is greater than
     * the maximum outstanding request count value then we say this parameter has breached the threshold.
     */
    private boolean evaluateLastSuccessfulRequestDurationLimitsBreached(PerformanceTracker performanceTracker, long requestStartTime) {
        return (performanceTracker.getLastSuccessfulRequestTimestamp() > 0) &&
            (((requestStartTime - performanceTracker.getLastSuccessfulRequestTimestamp()) > this.successfulRequestElapsedTimeout &&
                performanceTracker.getTotalOutstandingRequests() > this.maxOutstandingRequests));
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

    private void setSuccessfulRequestElapsedTimeout(int successfulRequestElapsedTimeout) {
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
}
