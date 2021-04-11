/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Shard Indexing Pressure Memory Manager is the class which will be responsible for increasing and decreasing the
 * limits given to a shard in a thread safe manner. The limits is the maximum space that a shard can occupy in the heap
 * and the values will be modified in certain scenarios.
 *
 * 1. If the limits assigned to the shard is breached(Primary Parameter) and the node level occupancy of all shards
 * is not greater than primary_parameter.node.soft_limit, we will be increasing the shard limits without any further evaluation.
 * 2. If the limits assigned to the shard is breached(Primary Parameter) and the node level occupancy of all the shards
 * is greater than primary_parameter.node.soft_limit is when we will evaluate certain parameters like
 * throughput degradation(Secondary Parameter) and last successful request elapsed timeout(Secondary Parameter) to evaluate if the limits
 * for the shard needs to be modified or not.
 *
 * Secondary Parameters
 * 1. ThroughputDegradationLimitsBreached - When the moving window throughput average has increased by some factor than
 * the historical throughput average. If the factor by which it has increased is greater than the degradation limit this
 * parameter is said to be breached.
 * 2. LastSuccessfulRequestDurationLimitsBreached - When the difference between last successful request timestamp and
 * current request timestamp is greater than the max timeout value and the number of outstanding requests is greater
 * than the max outstanding requests then this parameter is said to be breached.
 *
 * Note : Every time we try to increase of decrease the shard limits. In case the shard utilization goes below operating_factor.lower or
 * goes above operating_factor.upper of current shard limits then we try to set the new shard limit to be operating_factor.optimal of
 * current shard utilization.
 *
 */
public class ShardIndexingPressureMemoryManager {
    private final Logger logger = LogManager.getLogger(getClass());

    /*
    Operating factor can be evaluated using currentShardBytes/shardLimits. Outcome of this expression is categorized as
    lower, optimal and upper and appropriate action is taken once they breach the value mentioned below.
     */
    public static final Setting<Double> LOWER_OPERATING_FACTOR =
        Setting.doubleSetting("shard_indexing_pressure.operating_factor.lower", 0.75d, 0.0d, Property.NodeScope, Property.Dynamic);
    public static final Setting<Double> OPTIMAL_OPERATING_FACTOR =
        Setting.doubleSetting("shard_indexing_pressure.operating_factor.optimal", 0.85d, 0.0d, Property.NodeScope, Property.Dynamic);
    public static final Setting<Double> UPPER_OPERATING_FACTOR =
        Setting.doubleSetting("shard_indexing_pressure.operating_factor.upper", 0.95d, 0.0d, Property.NodeScope, Property.Dynamic);

    /*
    This is the max time that can be elapsed after any request is processed successfully. Appropriate action is taken
    once the below mentioned value is breached.
     */
    public static final Setting<Integer> SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT =
        Setting.intSetting("shard_indexing_pressure.secondary_parameter.successful_request.elapsed_timeout", 300000,
            Property.NodeScope, Property.Dynamic);

    /*
    This is the max outstanding request that are present after any request is processed successfully. Appropriate
    action is taken once the below mentioned value is breached.
     */
    public static final Setting<Integer> MAX_OUTSTANDING_REQUESTS =
        Setting.intSetting("shard_indexing_pressure.secondary_parameter.successful_request.max_outstanding_requests",
            100, Property.NodeScope, Property.Dynamic);

    /*
    Degradation limits can be evaluated using average throughput last N requests
    and N being {@link ShardIndexingPressure#WINDOW_SIZE} divided by lifetime average throughput.
    Appropriate action is taken once the outcome of above expression breaches the below mentioned factor
     */
    public static final Setting<Double> THROUGHPUT_DEGRADATION_LIMITS =
        Setting.doubleSetting("shard_indexing_pressure.secondary_parameter.throughput.degradation_factor", 5.0d, 1.0d,
            Property.NodeScope, Property.Dynamic);

    /*
    The secondary parameter accounting factor tells when the secondary parameter is considered. i.e. If the current
    node level memory utilization divided by the node limits is greater than 70% then appropriate action is taken.
     */
    public static final Setting<Double> NODE_SOFT_LIMIT =
        Setting.doubleSetting("shard_indexing_pressure.primary_parameter.node.soft_limit", 0.7d, 0.0d,
            Property.NodeScope, Property.Dynamic);

    public final AtomicLong totalNodeLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong totalLastSuccessfulRequestLimitsBreachedRejections = new AtomicLong();
    public final AtomicLong totalThroughputDegradationLimitsBreachedRejections = new AtomicLong();

    private final ShardIndexingPressureSettings shardIndexingPressureSettings;

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

    boolean isPrimaryNodeLimitBreached(ShardIndexingPressureTracker tracker, long nodeTotalBytes) {

        //Checks if the node level threshold is breached.
        if(nodeTotalBytes > this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) {
            logger.debug("Node limits breached for primary operation [node_total_bytes={}, " +
                    "node_primary_and_coordinating_limits={}]", nodeTotalBytes,
                this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits());
            tracker.rejection().getPrimaryNodeLimitsBreachedRejections().incrementAndGet();
            totalNodeLimitsBreachedRejections.incrementAndGet();

            return true;
        }
        return false;
    }

    boolean isPrimaryShardLimitBreached(ShardIndexingPressureTracker tracker, long requestStartTime,
                                        Map<Long, ShardIndexingPressureTracker> shardIndexingPressureStore, long nodeTotalBytes) {

        /* Memory limits is breached when the current utilization is greater than operating_factor.upper of total shard limits. */
        long shardCombinedBytes = tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().get();
        long shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits().get();
        boolean shardMemoryLimitsBreached =
            ((double)shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.upperOperatingFactor;

        if(shardMemoryLimitsBreached) {
            /*
            Secondary Parameters(i.e. LastSuccessfulRequestDuration and Throughput) is taken into consideration when
            the current node utilization is greater than primary_parameter.node.soft_limit of total node limits.
             */
            if(((double)nodeTotalBytes / this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) < this.nodeSoftLimit) {
                boolean isShardLimitsIncreased =
                    this.increaseShardPrimaryAndCoordinatingLimits(tracker, shardIndexingPressureStore);
                if(isShardLimitsIncreased == false) {
                    tracker.rejection().getPrimaryNodeLimitsBreachedRejections().incrementAndGet();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }

                return !isShardLimitsIncreased;
            } else {
                boolean shardLastSuccessfulRequestDurationLimitsBreached =
                    this.evaluateLastSuccessfulRequestDurationLimitsBreached(tracker.timeStamp().getLastSuccessfulPrimaryRequestTimestamp()
                            .get(), requestStartTime, tracker.outstandingRequest().getTotalOutstandingPrimaryRequests().get());

                boolean shardThroughputDegradationLimitsBreached =
                    this.evaluateThroughputDegradationLimitsBreached(
                        Double.longBitsToDouble(tracker.throughput().getPrimaryThroughputMovingAverage().get()),
                        tracker.memory().getTotalPrimaryBytes().get(), tracker.latency().getPrimaryTimeInMillis().get(),
                        tracker.throughput().getPrimaryThroughputMovingQueue().size(), primaryAndCoordinatingThroughputDegradationLimits);

                if(shardLastSuccessfulRequestDurationLimitsBreached || shardThroughputDegradationLimitsBreached) {
                    if(shardLastSuccessfulRequestDurationLimitsBreached) {
                        tracker.rejection().getPrimaryLastSuccessfulRequestLimitsBreachedRejections().incrementAndGet();
                        totalLastSuccessfulRequestLimitsBreachedRejections.incrementAndGet();
                    } else {
                        tracker.rejection().getPrimaryThroughputDegradationLimitsBreachedRejections().incrementAndGet();
                        totalThroughputDegradationLimitsBreachedRejections.incrementAndGet();
                    }

                    return true;
                } else {
                    boolean isShardLimitsIncreased =
                        this.increaseShardPrimaryAndCoordinatingLimits(tracker, shardIndexingPressureStore);
                    if(isShardLimitsIncreased == false) {
                        tracker.rejection().getPrimaryNodeLimitsBreachedRejections().incrementAndGet();
                        totalNodeLimitsBreachedRejections.incrementAndGet();
                    }

                    return !isShardLimitsIncreased;
                }
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
            tracker.rejection().getCoordinatingNodeLimitsBreachedRejections().incrementAndGet();
            totalNodeLimitsBreachedRejections.incrementAndGet();

            return true;
        }
        return false;
    }

    boolean isCoordinatingShardLimitBreached(ShardIndexingPressureTracker tracker, long requestStartTime,
                                             Map<Long, ShardIndexingPressureTracker> shardIndexingPressureStore, long nodeTotalBytes) {

        //Shard memory limit is breached when the current utilization is greater than operating_factor.upper of total shard limits.
        long shardCombinedBytes = tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().get();
        long shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits().get();
        boolean shardMemoryLimitsBreached =
            ((double)shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.upperOperatingFactor;

        if(shardMemoryLimitsBreached) {
            /*
            Secondary Parameters(i.e. LastSuccessfulRequestDuration and Throughput) is taken into consideration when
            the current node utilization is greater than primary_parameter.node.soft_limit of total node limits.
             */
            if(((double)nodeTotalBytes / this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) < this.nodeSoftLimit) {
                boolean isShardLimitsIncreased =
                    this.increaseShardPrimaryAndCoordinatingLimits(tracker, shardIndexingPressureStore);
                if(isShardLimitsIncreased == false) {
                    tracker.rejection().getCoordinatingNodeLimitsBreachedRejections().incrementAndGet();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }

                return !isShardLimitsIncreased;
            } else {
                boolean shardLastSuccessfulRequestDurationLimitsBreached =
                    this.evaluateLastSuccessfulRequestDurationLimitsBreached(
                        tracker.timeStamp().getLastSuccessfulCoordinatingRequestTimestamp().get(), requestStartTime,
                        tracker.outstandingRequest().getTotalOutstandingCoordinatingRequests().get());

                boolean shardThroughputDegradationLimitsBreached =
                    this.evaluateThroughputDegradationLimitsBreached(
                        Double.longBitsToDouble(tracker.throughput().getCoordinatingThroughputMovingAverage().get()),
                        tracker.memory().getTotalCoordinatingBytes().get(), tracker.latency().getCoordinatingTimeInMillis().get(),
                        tracker.throughput().getCoordinatingThroughputMovingQueue().size(),
                        primaryAndCoordinatingThroughputDegradationLimits);

                if (shardLastSuccessfulRequestDurationLimitsBreached || shardThroughputDegradationLimitsBreached) {
                    if(shardLastSuccessfulRequestDurationLimitsBreached) {
                        tracker.rejection().getCoordinatingLastSuccessfulRequestLimitsBreachedRejections().incrementAndGet();
                        totalLastSuccessfulRequestLimitsBreachedRejections.incrementAndGet();
                    } else {
                        tracker.rejection().getCoordinatingThroughputDegradationLimitsBreachedRejections().incrementAndGet();
                        totalThroughputDegradationLimitsBreachedRejections.incrementAndGet();
                    }

                    return true;
                } else {
                    boolean isShardLimitsIncreased =
                        this.increaseShardPrimaryAndCoordinatingLimits(tracker, shardIndexingPressureStore);
                    if(isShardLimitsIncreased == false) {
                        tracker.rejection().getCoordinatingNodeLimitsBreachedRejections().incrementAndGet();
                        totalNodeLimitsBreachedRejections.incrementAndGet();
                    }

                    return !isShardLimitsIncreased;
                }
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
            tracker.rejection().getReplicaNodeLimitsBreachedRejections().incrementAndGet();
            totalNodeLimitsBreachedRejections.incrementAndGet();

            return true;
        }
        return false;
    }

    boolean isReplicaShardLimitBreached(ShardIndexingPressureTracker tracker, long requestStartTime,
                                        Map<Long, ShardIndexingPressureTracker> shardIndexingPressureStore, long nodeReplicaBytes) {

        //Memory limits is breached when the current utilization is greater than operating_factor.upper of total shard limits.
        long shardReplicaBytes = tracker.memory().getCurrentReplicaBytes().get();
        long shardReplicaLimits = tracker.getReplicaLimits().get();
        final boolean shardMemoryLimitsBreached =
            ((double)shardReplicaBytes / shardReplicaLimits) > this.upperOperatingFactor;

        if(shardMemoryLimitsBreached) {
            /*
            Secondary Parameters(i.e. LastSuccessfulRequestDuration and Throughput) is taken into consideration when
            the current node utilization is greater than primary_parameter.node.soft_limit of total node limits.
             */
            if(((double)nodeReplicaBytes / this.shardIndexingPressureSettings.getNodeReplicaLimits()) < this.nodeSoftLimit)  {
                boolean isShardLimitsIncreased =
                    this.increaseShardReplicaLimits(tracker, shardIndexingPressureStore);
                if(isShardLimitsIncreased == false) {
                    tracker.rejection().getReplicaNodeLimitsBreachedRejections().incrementAndGet();
                    totalNodeLimitsBreachedRejections.incrementAndGet();
                }

                return !isShardLimitsIncreased;
            } else {
                boolean shardLastSuccessfulRequestDurationLimitsBreached =
                    this.evaluateLastSuccessfulRequestDurationLimitsBreached(
                        tracker.timeStamp().getLastSuccessfulReplicaRequestTimestamp().get(),
                        requestStartTime, tracker.outstandingRequest().getTotalOutstandingReplicaRequests().get());

                boolean shardThroughputDegradationLimitsBreached =
                    this.evaluateThroughputDegradationLimitsBreached(
                        Double.longBitsToDouble(tracker.throughput().getReplicaThroughputMovingAverage().get()),
                        tracker.memory().getTotalReplicaBytes().get(), tracker.latency().getReplicaTimeInMillis().get(),
                        tracker.throughput().getReplicaThroughputMovingQueue().size(), replicaThroughputDegradationLimits);

                if (shardLastSuccessfulRequestDurationLimitsBreached || shardThroughputDegradationLimitsBreached) {
                    if(shardLastSuccessfulRequestDurationLimitsBreached) {
                        tracker.rejection().getReplicaLastSuccessfulRequestLimitsBreachedRejections().incrementAndGet();
                        totalLastSuccessfulRequestLimitsBreachedRejections.incrementAndGet();
                    } else {
                        tracker.rejection().getReplicaThroughputDegradationLimitsBreachedRejections().incrementAndGet();
                        totalThroughputDegradationLimitsBreachedRejections.incrementAndGet();
                    }

                    return true;
                } else {
                    boolean isShardLimitsIncreased =
                        this.increaseShardReplicaLimits(tracker, shardIndexingPressureStore);
                    if(isShardLimitsIncreased == false) {
                        tracker.rejection().getReplicaNodeLimitsBreachedRejections().incrementAndGet();
                        totalNodeLimitsBreachedRejections.incrementAndGet();
                    }

                    return !isShardLimitsIncreased;
                }
            }
        } else {
            return false;
        }
    }

    private boolean increaseShardPrimaryAndCoordinatingLimits(ShardIndexingPressureTracker tracker,
                                                              Map<Long, ShardIndexingPressureTracker> shardIndexingPressureStore) {
        long shardPrimaryAndCoordinatingLimits;
        long expectedShardPrimaryAndCoordinatingLimits;
        do {
            shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits().get();
            long shardCombinedBytes = tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().get();
            expectedShardPrimaryAndCoordinatingLimits = (long)(shardCombinedBytes / this.optimalOperatingFactor);

            long totalPrimaryAndCoordinatingLimitsExceptCurrentShard = shardIndexingPressureStore.entrySet().stream()
                .filter(entry -> !(tracker.getShardId().hashCode() == entry.getKey()))
                .map(Map.Entry::getValue)
                .mapToLong(entry -> entry.getPrimaryAndCoordinatingLimits().get()).sum();

            if(((double)shardCombinedBytes / shardPrimaryAndCoordinatingLimits) > this.upperOperatingFactor) {
                if (totalPrimaryAndCoordinatingLimitsExceptCurrentShard + expectedShardPrimaryAndCoordinatingLimits <
                    this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits()) {
                    logger.debug("Increasing the Primary And Coordinating Limits [" +
                            "shard_detail=[{}][{}], shard_max_primary_and_coordinating_bytes={}, " +
                            "expected_shard_max_primary_and_coordinating_bytes={}]",
                        tracker.getShardId().getIndexName(), tracker.getShardId().id(),
                        shardPrimaryAndCoordinatingLimits, expectedShardPrimaryAndCoordinatingLimits);
                } else {
                    logger.debug("Failed to increase the Primary And Coordinating Limits [shard_detail=[{}][{}}], " +
                            "shard_max_primary_and_coordinating_bytes={}, " +
                            "total_max_primary_and_coordinating_bytes_except_current_shard={}, " +
                            "expected_shard_max_primary_and_coordinating_bytes={}, node_max_coordinating_and_primary_bytes={}]",
                        tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardPrimaryAndCoordinatingLimits,
                        totalPrimaryAndCoordinatingLimitsExceptCurrentShard, expectedShardPrimaryAndCoordinatingLimits,
                        this.shardIndexingPressureSettings.getNodePrimaryAndCoordinatingLimits());
                    return false;
                }
            } else {
                return true;
            }
        } while(!tracker.getPrimaryAndCoordinatingLimits().compareAndSet(shardPrimaryAndCoordinatingLimits,
            expectedShardPrimaryAndCoordinatingLimits));
        return true;
    }

    void decreaseShardPrimaryAndCoordinatingLimits(ShardIndexingPressureTracker tracker) {
        long shardPrimaryAndCoordinatingLimits;
        long expectedShardPrimaryAndCoordinatingLimits;
        do {
            shardPrimaryAndCoordinatingLimits = tracker.getPrimaryAndCoordinatingLimits().get();
            long shardCombinedBytes = tracker.memory().getCurrentCombinedCoordinatingAndPrimaryBytes().get();
            expectedShardPrimaryAndCoordinatingLimits = Math.max((long) (shardCombinedBytes / this.optimalOperatingFactor),
                this.shardIndexingPressureSettings.getShardPrimaryAndCoordinatingBaseLimits());

            if (((double)shardCombinedBytes / shardPrimaryAndCoordinatingLimits) < this.lowerOperatingFactor) {
                logger.debug("Decreasing the Primary And Coordinating Limits [shard_detail=[{}][{}], " +
                    "shard_max_primary_and_coordinating_bytes={}, expected_shard_max_primary_and_coordinating_bytes={}]",
                    tracker.getShardId().getIndexName(), tracker.getShardId().id(),
                    shardPrimaryAndCoordinatingLimits, expectedShardPrimaryAndCoordinatingLimits);
            } else {
                logger.debug("Primary And Coordinating Limits Already Increased [" +
                    "shard_detail=[{}][{}], " + "shard_max_primary_and_coordinating_bytes={}, " +
                    "expected_shard_max_primary_and_coordinating_bytes={}]",
                    tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardPrimaryAndCoordinatingLimits,
                    expectedShardPrimaryAndCoordinatingLimits);
                return;
            }
        } while(!tracker.getPrimaryAndCoordinatingLimits().compareAndSet(shardPrimaryAndCoordinatingLimits,
            expectedShardPrimaryAndCoordinatingLimits));
    }

    private boolean increaseShardReplicaLimits(ShardIndexingPressureTracker tracker,
                                               Map<Long, ShardIndexingPressureTracker> shardIndexingPressureStore) {
        long shardReplicaLimits;
        long expectedShardReplicaLimits;
        do {
            shardReplicaLimits = tracker.getReplicaLimits().get();
            long shardReplicaBytes = tracker.memory().getCurrentReplicaBytes().get();
            expectedShardReplicaLimits = (long)(shardReplicaBytes / this.optimalOperatingFactor);

            long totalReplicaLimitsExceptCurrentShard = shardIndexingPressureStore.entrySet().stream()
                .filter(entry -> !(tracker.getShardId().hashCode() == entry.getKey()))
                .map(Map.Entry::getValue)
                .mapToLong(entry -> entry.getReplicaLimits().get()).sum();

            if(((double)shardReplicaBytes / shardReplicaLimits) > this.upperOperatingFactor) {
                if (totalReplicaLimitsExceptCurrentShard + expectedShardReplicaLimits <
                    this.shardIndexingPressureSettings.getNodeReplicaLimits()) {
                    logger.debug("Increasing the Replica Limits [shard_detail=[{}][{}], " +
                        "shard_max_replica_bytes={}, expected_shard_max_replica_bytes={}]",
                        tracker.getShardId().getIndexName(), tracker.getShardId().id(),
                        shardReplicaLimits, expectedShardReplicaLimits);
                } else {
                    logger.debug("Failed to increase the Replica Limits [shard_detail=[{}][{}], " +
                        "shard_max_replica_bytes={}, total_max_replica_except_current_shard={}}, " +
                        "expected_shard_max_replica_bytes={}, node_max_replica_bytes={}]",
                        tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardReplicaLimits,
                        totalReplicaLimitsExceptCurrentShard, expectedShardReplicaLimits,
                        this.shardIndexingPressureSettings.getNodeReplicaLimits());
                    return false;
                }
            } else {
                return true;
            }
        } while(!tracker.getReplicaLimits().compareAndSet(shardReplicaLimits, expectedShardReplicaLimits));
        return true;
    }

    void decreaseShardReplicaLimits(ShardIndexingPressureTracker tracker) {

        long shardReplicaLimits;
        long expectedShardReplicaLimits;
        do {
            shardReplicaLimits = tracker.getReplicaLimits().get();
            long shardReplicaBytes = tracker.memory().getCurrentReplicaBytes().get();
            expectedShardReplicaLimits = Math.max((long) (shardReplicaBytes / this.optimalOperatingFactor),
                this.shardIndexingPressureSettings.getShardReplicaBaseLimits());

            if (((double)shardReplicaBytes / shardReplicaLimits) < this.lowerOperatingFactor) {
                logger.debug("Decreasing the Replica Limits [shard_detail=[{}}][{}}], " +
                    "shard_max_replica_bytes={}, expected_shard_max_replica_bytes={}]",
                    tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardReplicaLimits,
                    expectedShardReplicaLimits);
            } else {
                logger.debug("Replica Limits Already Increased [shard_detail=[{}][{}], " +
                    "shard_max_replica_bytes={}, expected_shard_max_replica_bytes={}]",
                    tracker.getShardId().getIndexName(), tracker.getShardId().id(), shardReplicaLimits,
                    expectedShardReplicaLimits);
                return;
            }
        } while(!tracker.getReplicaLimits().compareAndSet(shardReplicaLimits, expectedShardReplicaLimits));
    }

    /**
     * Throughput of last N request divided by the total lifetime requests throughput is greater than the acceptable
     * degradation limits then we say this parameter has breached the threshold.
     */
    private boolean  evaluateThroughputDegradationLimitsBreached(double throughputMovingAverage,
                                                                long totalBytes, long totalLatency,
                                                                long queueSize, double degradationLimits) {
        double throughputHistoricalAverage = (double)totalBytes / totalLatency;
        return throughputMovingAverage > 0 && queueSize >= shardIndexingPressureSettings.getRequestSizeWindow()
            && throughputHistoricalAverage / throughputMovingAverage > degradationLimits;
    }

    /**
     * The difference in the current timestamp and last successful request timestamp is greater than
     * successful request elapsed timeout value and the total number of outstanding requests is greater than
     * the maximum outstanding request count value then we say this parameter has breached the threshold.
     */
    private boolean evaluateLastSuccessfulRequestDurationLimitsBreached(long lastSuccessfulRequestTimestamp,
                                                                        long requestStartTime,
                                                                        long totalOutstandingRequests) {
        return (lastSuccessfulRequestTimestamp > 0) &&
            (((requestStartTime - lastSuccessfulRequestTimestamp) > this.successfulRequestElapsedTimeout
                    && totalOutstandingRequests > this.maxOutstandingRequests));
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
