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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IntroSorter;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardMovementStrategy;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationConstraints;
import org.opensearch.cluster.routing.allocation.ConstraintTypes;
import org.opensearch.cluster.routing.allocation.MoveDecision;
import org.opensearch.cluster.routing.allocation.RebalanceConstraints;
import org.opensearch.cluster.routing.allocation.RebalanceParameter;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.ShardAllocationDecision;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.action.shard.ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID;
import static org.opensearch.cluster.routing.allocation.ConstraintTypes.INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID;

/**
 * The {@link BalancedShardsAllocator} re-balances the nodes allocations
 * within a cluster based on a {@link WeightFunction}. The clusters balance is defined by four parameters which can be set
 * in the cluster update API that allows changes in real-time:
 * <ul><li><code>cluster.routing.allocation.balance.shard</code> - The <b>shard balance</b> defines the weight factor
 * for shards allocated on a {@link RoutingNode}</li>
 * <li><code>cluster.routing.allocation.balance.index</code> - The <b>index balance</b> defines a factor to the number
 * of {@link org.opensearch.cluster.routing.ShardRouting}s per index allocated on a specific node</li>
 * <li><code>cluster.routing.allocation.balance.threshold</code> - A <b>threshold</b> to set the minimal optimization
 * value of operations that should be performed</li>
 * <li><code>cluster.routing.allocation.balance.prefer_primary</code> - Defines whether primary shard balance is desired</li>
 * <li><code>cluster.routing.allocation.balance.disk_usage</code> - The <b>disk-usage balance</b> defines a factor
 * applied to per-shard byte size so rebalancing can be biased by actual disk footprint rather than purely
 * by shard count. Defaults to {@code 0.0f} (disabled). See
 * {@link #DISK_USAGE_BALANCE_FACTOR_SETTING} for calibration guidance.</li>
 * </ul>
 * <p>
 * These parameters are combined in a {@link WeightFunction} that allows calculation of node weights which
 * are used to re-balance shards based on global as well as per-index factors.
 *
 * @opensearch.internal
 */
public class BalancedShardsAllocator implements ShardsAllocator {

    private static final Logger logger = LogManager.getLogger(BalancedShardsAllocator.class);
    public static final TimeValue MIN_ALLOCATOR_TIMEOUT = TimeValue.timeValueSeconds(20);

    public static final Setting<Float> INDEX_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.index",
        0.55f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> SHARD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.shard",
        0.45f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    /**
     * Balance factor for the disk-usage component of the shard-balancer weight function.
     *
     * <p>The disk-usage term in the weight function is normalized by the cluster's average
     * per-node disk usage, so it is a dimensionless ratio (a node at 2&times; cluster-average
     * contributes {@code 1.0}). This is <em>different</em> from the count-based terms
     * {@link #SHARD_BALANCE_FACTOR_SETTING} and {@link #INDEX_BALANCE_FACTOR_SETTING}, which
     * are raw shard-count deviations (a node with 5 shards above average contributes {@code 5.0}).
     * The three factors are therefore <em>not</em> directly comparable: to give the disk term
     * a magnitude similar to the shard term on a cluster where typical skew is {@code K} shards,
     * this factor needs to be roughly {@code K} times larger than
     * {@link #SHARD_BALANCE_FACTOR_SETTING}. Operators should tune empirically against
     * {@link #THRESHOLD_SETTING} (default {@code 1.0}) to observe rebalancing behavior.
     *
     * <p>Achievable byte skew is bounded by shard granularity: when shards are large relative
     * to node capacity or are heterogeneous in size, the allocator converges to a local
     * optimum dictated by indivisible shard boundaries regardless of this factor.
     *
     * <p>The default is {@code 0.0f}, which disables disk-usage balancing entirely and keeps
     * the factor zero-overhead on the reroute hot path: when the factor is {@code 0.0f} the
     * allocator uses only the count and index terms and skips per-shard byte bookkeeping.
     */
    public static final Setting<Float> DISK_USAGE_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.disk_usage",
        0.0f,
        0.0f,
        Float.MAX_VALUE,
        new Setting.Validator<Float>() {
            @Override
            public void validate(Float value) {
                if (value != null && Float.isFinite(value) == false) {
                    throw new IllegalArgumentException(
                        "Illegal value for [cluster.routing.allocation.balance.disk_usage]: must be a finite number, got [" + value + "]"
                    );
                }
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Selects the scale on which the shard, per-index, and disk-usage balance terms are
     * expressed in the weight function.
     *
     * <ul>
     *   <li>{@link BalanceMode#COUNT} (default): shard and per-index terms are raw
     *       shard-count deltas from the cluster average; the disk-usage term is a ratio.
     *       Preserves historical behavior and {@link #THRESHOLD_SETTING} semantics.</li>
     *   <li>{@link BalanceMode#RATIO}: all three terms are relative deviations from the
     *       per-axis cluster average:
     *       <ul>
     *         <li>{@code weight_shard(n)  = (n.numShards    - avg)      / max(1, avg)}</li>
     *         <li>{@code weight_index(n,i)= (n.numShards(i) - avg(i))   / max(1, avg(i))}</li>
     *         <li>{@code weight_disk(n)   = (n.bytes        - avgBytes) / max(1, avgBytes)}</li>
     *       </ul>
     *       All three balance factors ({@link #SHARD_BALANCE_FACTOR_SETTING},
     *       {@link #INDEX_BALANCE_FACTOR_SETTING}, {@link #DISK_USAGE_BALANCE_FACTOR_SETTING})
     *       then operate on the same dimensionless scale.</li>
     * </ul>
     *
     * <p><b>Interaction with {@link #THRESHOLD_SETTING}.</b> In {@code count} mode,
     * {@code threshold} is interpreted as a shard-count delta (default {@code 1.0} meaning
     * "do not bother unless the imbalance is at least one shard"). In {@code ratio} mode,
     * {@code threshold} is interpreted as a relative deviation (e.g. {@code 0.1} meaning
     * "do not bother unless the imbalance is at least 10%"). Operators enabling ratio mode
     * will typically also lower {@code threshold} from its default of {@code 1.0}.
     *
     * <p><b>Sensitivity to small indices.</b> Ratio mode amplifies the weight of indices
     * with few shards: a single misplaced shard of a 3-shard index is a 33% deviation,
     * while the same misplacement on a 30-shard index is a 3% deviation. This is
     * semantically correct (small indices genuinely are more imbalanced per misplaced
     * shard) but changes prioritization relative to the default mode.
     */
    public static final Setting<BalanceMode> BALANCE_MODE_SETTING = new Setting<>(
        "cluster.routing.allocation.balance.mode",
        BalanceMode.COUNT.toString(),
        BalanceMode::parse,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Move primary shards first from node for shard movement when shards can not stay on node anymore. {@link LocalShardsBalancer#moveShards()}
     */
    public static final Setting<Boolean> SHARD_MOVE_PRIMARY_FIRST_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.move.primary_first",
        false,
        Property.Dynamic,
        Property.NodeScope,
        Property.Deprecated
    );

    /**
     * Decides order in which to move shards from node when shards can not stay on node anymore. {@link LocalShardsBalancer#moveShards()}
     * Encapsulates behavior of above SHARD_MOVE_PRIMARY_FIRST_SETTING.
     */
    public static final Setting<ShardMovementStrategy> SHARD_MOVEMENT_STRATEGY_SETTING = new Setting<ShardMovementStrategy>(
        "cluster.routing.allocation.shard_movement_strategy",
        ShardMovementStrategy.NO_PREFERENCE.toString(),
        ShardMovementStrategy::parse,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Float> THRESHOLD_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.threshold",
        1.0f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Long> PRIMARY_CONSTRAINT_THRESHOLD_SETTING = Setting.longSetting(
        "cluster.routing.allocation.primary_constraint.threshold",
        10,
        0,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * This setting governs whether primary shards balance is desired during allocation. This is used by {@link ConstraintTypes#isPerIndexPrimaryShardsPerNodeBreached()}
     * and {@link ConstraintTypes#isPrimaryShardsPerNodeBreached} which is used during unassigned shard allocation
     * {@link LocalShardsBalancer#allocateUnassigned()} and shard re-balance/relocation to a different node via {@link LocalShardsBalancer#balance()} .
     */

    public static final Setting<Boolean> PREFER_PRIMARY_SHARD_BALANCE = Setting.boolSetting(
        "cluster.routing.allocation.balance.prefer_primary",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> PREFER_PRIMARY_SHARD_REBALANCE = Setting.boolSetting(
        "cluster.routing.allocation.rebalance.primary.enable",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> IGNORE_THROTTLE_FOR_REMOTE_RESTORE = Setting.boolSetting(
        "cluster.routing.allocation.remote_primary.ignore_throttle",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Float> PRIMARY_SHARD_REBALANCE_BUFFER = Setting.floatSetting(
        "cluster.routing.allocation.rebalance.primary.buffer",
        0.10f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<TimeValue> ALLOCATOR_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.balanced_shards_allocator.allocator_timeout",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        timeValue -> {
            if (timeValue.compareTo(MIN_ALLOCATOR_TIMEOUT) < 0 && timeValue.compareTo(TimeValue.MINUS_ONE) != 0) {
                throw new IllegalArgumentException(
                    "Setting ["
                        + "cluster.routing.allocation.balanced_shards_allocator.allocator_timeout"
                        + "] should be more than 20s or -1ms to disable timeout"
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Adjusts the priority of the followup reroute task when current round times out. NORMAL is right for reasonable clusters,
     * but for a cluster in a messed up state which is starving NORMAL priority tasks, it might be necessary to raise this higher
     * to allocate shards.
     */
    public static final Setting<Priority> FOLLOW_UP_REROUTE_PRIORITY_SETTING = new Setting<>(
        "cluster.routing.allocation.balanced_shards_allocator.schedule_reroute.priority",
        Priority.NORMAL.toString(),
        BalancedShardsAllocator::parseReroutePriority,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static Priority parseReroutePriority(String priorityString) {
        final Priority priority = Priority.valueOf(priorityString.toUpperCase(Locale.ROOT));
        switch (priority) {
            case NORMAL:
            case HIGH:
            case URGENT:
                return priority;
        }
        throw new IllegalArgumentException(
            "priority [" + priority + "] not supported for [" + FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey() + "]"
        );
    }

    private volatile boolean movePrimaryFirst;
    private volatile ShardMovementStrategy shardMovementStrategy;

    private volatile boolean preferPrimaryShardBalance;
    private volatile boolean preferPrimaryShardRebalance;
    private volatile float preferPrimaryShardRebalanceBuffer;
    private volatile float indexBalanceFactor;
    private volatile float shardBalanceFactor;
    private volatile float diskUsageBalanceFactor;
    private volatile BalanceMode balanceMode;
    private volatile WeightFunction weightFunction;
    private volatile float threshold;
    private volatile long primaryConstraintThreshold;

    private volatile boolean ignoreThrottleInRestore;
    private volatile TimeValue allocatorTimeout;
    private volatile Priority followUpRerouteTaskPriority;
    private long startTime;
    private RerouteService rerouteService;

    public BalancedShardsAllocator(Settings settings) {
        this(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    @Inject
    public BalancedShardsAllocator(Settings settings, ClusterSettings clusterSettings) {
        setShardBalanceFactor(SHARD_BALANCE_FACTOR_SETTING.get(settings));
        setIndexBalanceFactor(INDEX_BALANCE_FACTOR_SETTING.get(settings));
        setDiskUsageBalanceFactor(DISK_USAGE_BALANCE_FACTOR_SETTING.get(settings));
        setBalanceMode(BALANCE_MODE_SETTING.get(settings));
        setPreferPrimaryShardRebalanceBuffer(PRIMARY_SHARD_REBALANCE_BUFFER.get(settings));
        setIgnoreThrottleInRestore(IGNORE_THROTTLE_FOR_REMOTE_RESTORE.get(settings));
        updateWeightFunction();
        setThreshold(THRESHOLD_SETTING.get(settings));
        setPrimaryConstraintThresholdSetting(PRIMARY_CONSTRAINT_THRESHOLD_SETTING.get(settings));
        setPreferPrimaryShardBalance(PREFER_PRIMARY_SHARD_BALANCE.get(settings));
        setPreferPrimaryShardRebalance(PREFER_PRIMARY_SHARD_REBALANCE.get(settings));
        setShardMovementStrategy(SHARD_MOVEMENT_STRATEGY_SETTING.get(settings));
        setAllocatorTimeout(ALLOCATOR_TIMEOUT_SETTING.get(settings));
        setFollowUpRerouteTaskPriority(FOLLOW_UP_REROUTE_PRIORITY_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(PREFER_PRIMARY_SHARD_BALANCE, this::setPreferPrimaryShardBalance);
        clusterSettings.addSettingsUpdateConsumer(SHARD_MOVE_PRIMARY_FIRST_SETTING, this::setMovePrimaryFirst);
        clusterSettings.addSettingsUpdateConsumer(SHARD_MOVEMENT_STRATEGY_SETTING, this::setShardMovementStrategy);
        clusterSettings.addSettingsUpdateConsumer(INDEX_BALANCE_FACTOR_SETTING, this::updateIndexBalanceFactor);
        clusterSettings.addSettingsUpdateConsumer(SHARD_BALANCE_FACTOR_SETTING, this::updateShardBalanceFactor);
        clusterSettings.addSettingsUpdateConsumer(DISK_USAGE_BALANCE_FACTOR_SETTING, this::updateDiskUsageBalanceFactor);
        clusterSettings.addSettingsUpdateConsumer(BALANCE_MODE_SETTING, this::updateBalanceMode);
        clusterSettings.addSettingsUpdateConsumer(PRIMARY_SHARD_REBALANCE_BUFFER, this::updatePreferPrimaryShardBalanceBuffer);
        clusterSettings.addSettingsUpdateConsumer(PREFER_PRIMARY_SHARD_REBALANCE, this::setPreferPrimaryShardRebalance);
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTING, this::setThreshold);
        clusterSettings.addSettingsUpdateConsumer(PRIMARY_CONSTRAINT_THRESHOLD_SETTING, this::setPrimaryConstraintThresholdSetting);
        clusterSettings.addSettingsUpdateConsumer(IGNORE_THROTTLE_FOR_REMOTE_RESTORE, this::setIgnoreThrottleInRestore);
        clusterSettings.addSettingsUpdateConsumer(ALLOCATOR_TIMEOUT_SETTING, this::setAllocatorTimeout);
        clusterSettings.addSettingsUpdateConsumer(FOLLOW_UP_REROUTE_PRIORITY_SETTING, this::setFollowUpRerouteTaskPriority);
    }

    @Override
    public void setRerouteService(RerouteService rerouteService) {
        assert this.rerouteService == null : "RerouteService is already set";
        this.rerouteService = rerouteService;
    }

    /**
     * Changes in deprecated setting SHARD_MOVE_PRIMARY_FIRST_SETTING affect value of its replacement setting SHARD_MOVEMENT_STRATEGY_SETTING.
     */
    private void setMovePrimaryFirst(boolean movePrimaryFirst) {
        this.movePrimaryFirst = movePrimaryFirst;
        setShardMovementStrategy(this.shardMovementStrategy);
    }

    private void setIgnoreThrottleInRestore(boolean ignoreThrottleInRestore) {
        this.ignoreThrottleInRestore = ignoreThrottleInRestore;
    }

    /**
     * Sets the correct Shard movement strategy to use.
     * If users are still using deprecated setting `move_primary_first`, we want behavior to remain unchanged.
     * In the event of changing ShardMovementStrategy setting from default setting NO_PREFERENCE to either PRIMARY_FIRST or REPLICA_FIRST, we want that
     * to have priority over values set in move_primary_first setting.
     */
    private void setShardMovementStrategy(ShardMovementStrategy shardMovementStrategy) {
        this.shardMovementStrategy = shardMovementStrategy;
        if (shardMovementStrategy == ShardMovementStrategy.NO_PREFERENCE && this.movePrimaryFirst) {
            this.shardMovementStrategy = ShardMovementStrategy.PRIMARY_FIRST;
        }
    }

    private void setIndexBalanceFactor(float indexBalanceFactor) {
        this.indexBalanceFactor = indexBalanceFactor;
    }

    private void setShardBalanceFactor(float shardBalanceFactor) {
        this.shardBalanceFactor = shardBalanceFactor;
    }

    private void setDiskUsageBalanceFactor(float diskUsageBalanceFactor) {
        this.diskUsageBalanceFactor = diskUsageBalanceFactor;
    }

    private void setBalanceMode(BalanceMode balanceMode) {
        this.balanceMode = balanceMode;
    }

    private void setPreferPrimaryShardRebalanceBuffer(float preferPrimaryShardRebalanceBuffer) {
        this.preferPrimaryShardRebalanceBuffer = preferPrimaryShardRebalanceBuffer;
    }

    private void updateIndexBalanceFactor(float indexBalanceFactor) {
        this.indexBalanceFactor = indexBalanceFactor;
        updateWeightFunction();
    }

    private void updateShardBalanceFactor(float shardBalanceFactor) {
        this.shardBalanceFactor = shardBalanceFactor;
        updateWeightFunction();
    }

    private void updateDiskUsageBalanceFactor(float diskUsageBalanceFactor) {
        this.diskUsageBalanceFactor = diskUsageBalanceFactor;
        updateWeightFunction();
    }

    private void updateBalanceMode(BalanceMode balanceMode) {
        this.balanceMode = balanceMode;
        updateWeightFunction();
    }

    private void updatePreferPrimaryShardBalanceBuffer(float preferPrimaryShardBalanceBuffer) {
        this.preferPrimaryShardRebalanceBuffer = preferPrimaryShardBalanceBuffer;
        updateWeightFunction();
    }

    private void updateWeightFunction() {
        weightFunction = new WeightFunction(
            this.indexBalanceFactor,
            this.shardBalanceFactor,
            this.diskUsageBalanceFactor,
            this.preferPrimaryShardRebalanceBuffer,
            this.primaryConstraintThreshold,
            this.preferPrimaryShardBalance,
            this.preferPrimaryShardRebalance,
            this.balanceMode == BalanceMode.RATIO
        );
    }

    /**
     * When primary shards balance is desired, enable primary shard balancing constraints
     * @param preferPrimaryShardBalance boolean to prefer balancing by primary shard
     */
    private void setPreferPrimaryShardBalance(boolean preferPrimaryShardBalance) {
        this.preferPrimaryShardBalance = preferPrimaryShardBalance;
        this.weightFunction.updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, preferPrimaryShardBalance);
        this.weightFunction.updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, preferPrimaryShardBalance);
        this.weightFunction.updateRebalanceConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, preferPrimaryShardBalance);
    }

    private void setPreferPrimaryShardRebalance(boolean preferPrimaryShardRebalance) {
        this.preferPrimaryShardRebalance = preferPrimaryShardRebalance;
        this.weightFunction.updateRebalanceConstraint(CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID, preferPrimaryShardRebalance);
    }

    private void setThreshold(float threshold) {
        this.threshold = threshold;
    }

    private void setPrimaryConstraintThresholdSetting(long threshold) {
        this.primaryConstraintThreshold = threshold;
        this.weightFunction.updatePrimaryConstraintThreshold(threshold);
    }

    private void setAllocatorTimeout(TimeValue allocatorTimeout) {
        this.allocatorTimeout = allocatorTimeout;
    }

    private void setFollowUpRerouteTaskPriority(Priority followUpRerouteTaskPriority) {
        this.followUpRerouteTaskPriority = followUpRerouteTaskPriority;
    }

    protected boolean allocatorTimedOut() {
        if (allocatorTimeout.equals(TimeValue.MINUS_ONE)) {
            if (logger.isTraceEnabled()) {
                logger.trace("Allocator timeout is disabled. Will not short circuit allocator tasks");
            }
            return false;
        }
        return System.nanoTime() - this.startTime > allocatorTimeout.nanos();
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        if (allocation.routingNodes().size() == 0) {
            failAllocationOfNewPrimaries(allocation);
            return;
        }
        final ShardsBalancer localShardsBalancer = new LocalShardsBalancer(
            logger,
            allocation,
            shardMovementStrategy,
            weightFunction,
            threshold,
            preferPrimaryShardBalance,
            preferPrimaryShardRebalance,
            ignoreThrottleInRestore,
            this::allocatorTimedOut
        );
        this.startTime = System.nanoTime();
        localShardsBalancer.allocateUnassigned();
        localShardsBalancer.moveShards();
        localShardsBalancer.balance();
        scheduleRerouteIfAllocatorTimedOut();

        final ShardsBalancer remoteShardsBalancer = new RemoteShardsBalancer(logger, allocation);
        remoteShardsBalancer.allocateUnassigned();
        remoteShardsBalancer.moveShards();
        remoteShardsBalancer.balance();

    }

    @Override
    public ShardAllocationDecision decideShardAllocation(final ShardRouting shard, final RoutingAllocation allocation) {
        ShardsBalancer localShardsBalancer = new LocalShardsBalancer(
            logger,
            allocation,
            shardMovementStrategy,
            weightFunction,
            threshold,
            preferPrimaryShardBalance,
            preferPrimaryShardRebalance,
            ignoreThrottleInRestore,
            () -> false // as we don't need to check if timed out or not while just understanding ShardAllocationDecision
        );
        AllocateUnassignedDecision allocateUnassignedDecision = AllocateUnassignedDecision.NOT_TAKEN;
        MoveDecision moveDecision = MoveDecision.NOT_TAKEN;
        if (shard.unassigned()) {
            allocateUnassignedDecision = localShardsBalancer.decideAllocateUnassigned(shard);
        } else {
            moveDecision = localShardsBalancer.decideMove(shard);
            if (moveDecision.isDecisionTaken() && moveDecision.canRemain()) {
                MoveDecision rebalanceDecision = localShardsBalancer.decideRebalance(shard);
                moveDecision = rebalanceDecision.withRemainDecision(moveDecision.getCanRemainDecision());
            }
        }
        return new ShardAllocationDecision(allocateUnassignedDecision, moveDecision);
    }

    private void failAllocationOfNewPrimaries(RoutingAllocation allocation) {
        RoutingNodes routingNodes = allocation.routingNodes();
        assert routingNodes.size() == 0 : routingNodes;
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            final ShardRouting shardRouting = unassignedIterator.next();
            final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            if (shardRouting.primary() && unassignedInfo.getLastAllocationStatus() == AllocationStatus.NO_ATTEMPT) {
                unassignedIterator.updateUnassigned(
                    new UnassignedInfo(
                        unassignedInfo.getReason(),
                        unassignedInfo.getMessage(),
                        unassignedInfo.getFailure(),
                        unassignedInfo.getNumFailedAllocations(),
                        unassignedInfo.getUnassignedTimeInNanos(),
                        unassignedInfo.getUnassignedTimeInMillis(),
                        unassignedInfo.isDelayed(),
                        AllocationStatus.DECIDERS_NO,
                        unassignedInfo.getFailedNodeIds()
                    ),
                    shardRouting.recoverySource(),
                    allocation.changes()
                );
            }
        }
    }

    private void scheduleRerouteIfAllocatorTimedOut() {
        if (allocatorTimedOut()) {
            if (rerouteService == null) {
                logger.info("RerouteService not set to schedule reroute after allocator time out");
                return;
            }
            rerouteService.reroute(
                "reroute after balanced shards allocator timed out",
                followUpRerouteTaskPriority,
                ActionListener.wrap(
                    r -> logger.trace("reroute after balanced shards allocator timed out completed"),
                    e -> logger.debug("reroute after balanced shards allocator timed out failed", e)
                )
            );
        }
    }

    /**
     * Returns the currently configured delta threshold
     */
    public float getThreshold() {
        return threshold;
    }

    /**
     * Returns the index related weight factor.
     */
    public float getIndexBalance() {
        return weightFunction.indexBalance;
    }

    /**
     * Returns the shard related weight factor.
     */
    public float getShardBalance() {
        return weightFunction.shardBalance;
    }

    /**
     * Returns the disk-usage related weight factor.
     */
    public float getDiskUsageBalance() {
        return weightFunction.diskUsageBalance;
    }

    /**
     * Returns preferPrimaryShardBalance.
     */
    public boolean getPreferPrimaryBalance() {
        return preferPrimaryShardBalance;
    }

    /**
     * This class is the primary weight function used to create balanced over nodes and shards in the cluster.
     * Currently this function has 3 properties:
     * <ul>
     * <li><code>index balance</code> - balance property over shards per index</li>
     * <li><code>shard balance</code> - balance property over shards per cluster</li>
     * <li><code>disk-usage balance</code> - balance property over per-shard byte size per node</li>
     * </ul>
     * <p>
     * Each of these properties are expressed as factor such that the properties factor defines the relative
     * importance of the property for the weight function. For example if the weight function should calculate
     * the weights only based on a global (shard) balance the index balance can be set to {@code 0.0} and will
     * in turn have no effect on the distribution.
     * </p>
     * The weight per index is calculated based on the following formula:
     * <ul>
     * <li>
     * <code>weight<sub>index</sub>(node, index) = indexBalance * (node.numShards(index) - avgShardsPerNode(index))</code>
     * </li>
     * <li>
     * <code>weight<sub>node</sub>(node, index) = shardBalance * (node.numShards() - avgShardsPerNode)</code>
     * </li>
     * <li>
     * <code>weight<sub>disk</sub>(node) = diskUsageBalance * (node.diskUsageInBytes() - avgDiskUsage) / avgDiskUsage</code>
     * (contributes {@code 0} when {@code avgDiskUsage == 0})
     * </li>
     * </ul>
     * <code>weight(node, index) = weight<sub>index</sub>(node, index) + weight<sub>node</sub>(node, index) + weight<sub>disk</sub>(node)</code>
     * <p>
     * Note: the index and shard terms are raw shard-count deviations; the disk term is a dimensionless
     * ratio normalized by the cluster average. See {@link #DISK_USAGE_BALANCE_FACTOR_SETTING} for the
     * implications when tuning {@code diskUsageBalance} relative to the other two factors.
     * <p>
     * package-private for testing
     */
    static class WeightFunction {

        private final float indexBalance;
        private final float shardBalance;
        private final float diskUsageBalance;
        private final float theta0;
        private final float theta1;
        private final float theta2;
        private final boolean ratioMode;
        private long primaryConstraintThreshold;
        private AllocationConstraints constraints;
        private RebalanceConstraints rebalanceConstraints;

        WeightFunction(
            float indexBalance,
            float shardBalance,
            float diskUsageBalance,
            float preferPrimaryBalanceBuffer,
            long primaryConstraintThreshold,
            boolean preferPrimaryShardBalance,
            boolean preferPrimaryShardRebalance,
            boolean ratioMode
        ) {
            float sum = indexBalance + shardBalance + diskUsageBalance;
            if (sum <= 0.0f) {
                throw new IllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
            }
            theta0 = shardBalance / sum;
            theta1 = indexBalance / sum;
            theta2 = diskUsageBalance / sum;
            this.indexBalance = indexBalance;
            this.shardBalance = shardBalance;
            this.diskUsageBalance = diskUsageBalance;
            this.ratioMode = ratioMode;
            this.primaryConstraintThreshold = primaryConstraintThreshold;
            RebalanceParameter rebalanceParameter = new RebalanceParameter(preferPrimaryBalanceBuffer);
            this.constraints = new AllocationConstraints();
            this.rebalanceConstraints = new RebalanceConstraints(rebalanceParameter);
            // Enable index shard per node breach constraint
            updateAllocationConstraint(INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID, true);
            updateAllocationConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, preferPrimaryShardBalance);
            updateAllocationConstraint(CLUSTER_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, preferPrimaryShardBalance);
            updateRebalanceConstraint(INDEX_PRIMARY_SHARD_BALANCE_CONSTRAINT_ID, preferPrimaryShardBalance);
            updateRebalanceConstraint(CLUSTER_PRIMARY_SHARD_REBALANCE_CONSTRAINT_ID, preferPrimaryShardRebalance);
        }

        public float weightWithAllocationConstraints(ShardsBalancer balancer, ModelNode node, String index) {
            float balancerWeight = weight(balancer, node, index);
            return balancerWeight + constraints.weight(balancer, node, index, primaryConstraintThreshold);
        }

        /**
         * Returns the raw disk_usage balance factor configured on this weight function.
         * Callers (e.g. {@link LocalShardsBalancer}) use this to decide whether the
         * per-shard byte bookkeeping in {@link ModelNode} should be performed, so that
         * the default (0.0f) stays zero-overhead.
         */
        float diskUsageBalance() {
            return diskUsageBalance;
        }

        public float weightWithRebalanceConstraints(ShardsBalancer balancer, ModelNode node, String index) {
            float balancerWeight = weight(balancer, node, index);
            return balancerWeight + rebalanceConstraints.weight(balancer, node, index, primaryConstraintThreshold);
        }

        /**
         * Computes the balance weight for a single (node, index) pair.
         *
         * <p>Two modes are supported, selected by the
         * {@link #BALANCE_MODE_SETTING} cluster setting:
         *
         * <ul>
         *   <li><b>Default (count-based) mode:</b> {@code weight_shard} and
         *     {@code weight_index} are raw shard-count deviations while
         *     {@code weight_disk} is a dimensionless ratio. The three terms are not
         *     directly comparable in magnitude; see
         *     {@link #DISK_USAGE_BALANCE_FACTOR_SETTING} javadoc for tuning guidance.</li>
         *   <li><b>Ratio mode:</b> all three terms are relative deviations normalized by
         *     the cluster average for that axis, so the three balance factors operate on
         *     the same dimensionless scale and can be tuned directly relative to each
         *     other. See {@link #BALANCE_MODE_SETTING} javadoc for threshold guidance.</li>
         * </ul>
         *
         * <p>Numerical precision: the disk term subtracts a {@code long} byte count from a
         * {@code double} average and divides in {@code double} before casting the final
         * weighted contribution to {@code float}. This preserves sub-MB resolution on
         * multi-TB clusters where a {@code float} division would round away small deltas.
         */
        float weight(ShardsBalancer balancer, ModelNode node, String index) {
            final double avgShards = balancer.avgShardsPerNode();
            final double avgShardsIdx = balancer.avgShardsPerNode(index);
            final double avgBytes = balancer.avgDiskUsageInBytesPerNode();
            final double nodeBytes = (double) node.diskUsageInBytes();

            final double shardDelta = node.numShards() - avgShards;
            final double indexDelta = node.numShards(index) - avgShardsIdx;
            final double bytesDelta = nodeBytes - avgBytes;

            final double weightShard;
            final double weightIndex;
            final double weightDisk;
            if (ratioMode) {
                // All three terms are dimensionless relative deviations. Guard against a
                // zero denominator (empty index / empty cluster) by contributing zero for
                // that term: a node cannot be "imbalanced" on an axis whose total is zero.
                weightShard = avgShards > 0.0 ? shardDelta / avgShards : 0.0;
                weightIndex = avgShardsIdx > 0.0 ? indexDelta / avgShardsIdx : 0.0;
                weightDisk = avgBytes > 0.0 ? bytesDelta / avgBytes : 0.0;
            } else {
                // Historical behavior: raw count deviations for shard/index, ratio for disk.
                weightShard = shardDelta;
                weightIndex = indexDelta;
                weightDisk = avgBytes > 0.0 ? bytesDelta / avgBytes : 0.0;
            }
            return (float) (theta0 * weightShard + theta1 * weightIndex + theta2 * weightDisk);
        }

        void updateAllocationConstraint(String constraint, boolean enable) {
            this.constraints.updateAllocationConstraint(constraint, enable);
        }

        void updateRebalanceConstraint(String constraint, boolean add) {
            this.rebalanceConstraints.updateRebalanceConstraint(constraint, add);
        }

        void updatePrimaryConstraintThreshold(long primaryConstraintThreshold) {
            this.primaryConstraintThreshold = primaryConstraintThreshold;
        }
    }

    /**
     * A model node.
     *
     * @opensearch.internal
     */
    public static class ModelNode implements Iterable<ModelIndex> {
        private final Map<String, ModelIndex> indices = new HashMap<>();
        private int numShards = 0;
        private int numPrimaryShards = 0;
        private long diskUsageInBytes = 0;
        private final RoutingNode routingNode;
        private final ClusterInfo clusterInfo;
        /**
         * When {@code false} the per-shard byte bookkeeping in {@link #addShard} / {@link #removeShard}
         * is skipped entirely. This keeps the feature zero-overhead on the hot path when
         * {@link BalancedShardsAllocator#DISK_USAGE_BALANCE_FACTOR_SETTING} is at its 0.0f default,
         * avoiding a {@link ClusterInfo#getShardSize} call (and its shard-identifier string
         * allocation) per shard per reroute.
         */
        private final boolean trackDiskUsage;

        ModelNode(RoutingNode routingNode) {
            this(routingNode, ClusterInfo.EMPTY, false);
        }

        ModelNode(RoutingNode routingNode, ClusterInfo clusterInfo) {
            this(routingNode, clusterInfo, true);
        }

        ModelNode(RoutingNode routingNode, ClusterInfo clusterInfo, boolean trackDiskUsage) {
            this.routingNode = routingNode;
            this.clusterInfo = clusterInfo == null ? ClusterInfo.EMPTY : clusterInfo;
            this.trackDiskUsage = trackDiskUsage;
        }

        public ModelIndex getIndex(String indexId) {
            return indices.get(indexId);
        }

        public String getNodeId() {
            return routingNode.nodeId();
        }

        public RoutingNode getRoutingNode() {
            return routingNode;
        }

        public int numShards() {
            return numShards;
        }

        public int numShards(String idx) {
            ModelIndex index = indices.get(idx);
            return index == null ? 0 : index.numShards();
        }

        public int numPrimaryShards(String idx) {
            ModelIndex index = indices.get(idx);
            return index == null ? 0 : index.numPrimaryShards();
        }

        public int numPrimaryShards() {
            return numPrimaryShards;
        }

        public long diskUsageInBytes() {
            return diskUsageInBytes;
        }

        public int highestPrimary(String index) {
            ModelIndex idx = indices.get(index);
            if (idx != null) {
                return idx.highestPrimary();
            }
            return -1;
        }

        public void addShard(ShardRouting shard) {
            ModelIndex index = indices.get(shard.getIndexName());
            if (index == null) {
                index = new ModelIndex(shard.getIndexName());
                indices.put(index.getIndexId(), index);
            }
            index.addShard(shard);
            if (shard.primary()) {
                numPrimaryShards++;
            }

            if (trackDiskUsage) {
                diskUsageInBytes += clusterInfo.getShardSize(shard, 0L);
            }
            numShards++;
        }

        public void removeShard(ShardRouting shard) {
            ModelIndex index = indices.get(shard.getIndexName());
            if (index != null) {
                index.removeShard(shard);
                if (index.numShards() == 0) {
                    indices.remove(shard.getIndexName());
                }
            }

            if (shard.primary()) {
                numPrimaryShards--;
            }

            if (trackDiskUsage) {
                diskUsageInBytes -= clusterInfo.getShardSize(shard, 0L);
            }
            numShards--;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Node(").append(routingNode.nodeId()).append(")");
            return sb.toString();
        }

        @Override
        public Iterator<ModelIndex> iterator() {
            return indices.values().iterator();
        }

        public boolean containsShard(ShardRouting shard) {
            ModelIndex index = getIndex(shard.getIndexName());
            return index == null ? false : index.containsShard(shard);
        }
    }

    /**
     *  A {@link Balancer} used by the {@link BalancedShardsAllocator} to perform allocation operations
     * @deprecated As of 2.4.0, replaced by {@link LocalShardsBalancer}
     *
     * @opensearch.internal
     */
    @Deprecated
    public static class Balancer extends LocalShardsBalancer {
        public Balancer(
            Logger logger,
            RoutingAllocation allocation,
            ShardMovementStrategy shardMovementStrategy,
            BalancedShardsAllocator.WeightFunction weight,
            float threshold,
            boolean preferPrimaryBalance
        ) {
            super(logger, allocation, shardMovementStrategy, weight, threshold, preferPrimaryBalance, false, false, () -> false);
        }
    }

    /**
     * A model index.
     *
     * @opensearch.internal
     */
    static final class ModelIndex implements Iterable<ShardRouting> {
        private final String id;
        private final Set<ShardRouting> shards = new HashSet<>(4); // expect few shards of same index to be allocated on same node
        private final Set<ShardRouting> primaryShards = new HashSet<>();
        private int highestPrimary = -1;

        ModelIndex(String id) {
            this.id = id;
        }

        public int numPrimaryShards() {
            return primaryShards.size();
        }

        public int highestPrimary() {
            if (highestPrimary == -1) {
                int maxId = -1;
                for (ShardRouting shard : shards) {
                    if (shard.primary()) {
                        maxId = Math.max(maxId, shard.id());
                    }
                }
                return highestPrimary = maxId;
            }
            return highestPrimary;
        }

        public String getIndexId() {
            return id;
        }

        public int numShards() {
            return shards.size();
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return shards.iterator();
        }

        public void removeShard(ShardRouting shard) {
            highestPrimary = -1;
            assert shards.contains(shard) : "Shard not allocated on current node: " + shard;
            if (shard.primary()) {
                assert primaryShards.contains(shard) : "Primary shard not allocated on current node: " + shard;
                primaryShards.remove(shard);
            }
            shards.remove(shard);
        }

        public void addShard(ShardRouting shard) {
            highestPrimary = -1;
            assert shards.contains(shard) == false : "Shard already allocated on current node: " + shard;
            if (shard.primary()) {
                assert primaryShards.contains(shard) == false : "Primary shard already allocated on current node: " + shard;
                primaryShards.add(shard);
            }
            shards.add(shard);
        }

        public boolean containsShard(ShardRouting shard) {
            return shards.contains(shard);
        }
    }

    /**
     * A node sorter.
     *
     * @opensearch.internal
     */
    static final class NodeSorter extends IntroSorter {

        final ModelNode[] modelNodes;
        /* the nodes weights with respect to the current weight function / index */
        final float[] weights;
        private final WeightFunction function;
        private String index;
        private final ShardsBalancer balancer;
        private float pivotWeight;

        NodeSorter(ModelNode[] modelNodes, WeightFunction function, ShardsBalancer balancer) {
            this.function = function;
            this.balancer = balancer;
            this.modelNodes = modelNodes;
            weights = new float[modelNodes.length];
        }

        /**
         * Resets the sorter, recalculates the weights per node and sorts the
         * nodes by weight, with minimal weight first.
         */
        public void reset(String index, int from, int to) {
            this.index = index;
            for (int i = from; i < to; i++) {
                weights[i] = weight(modelNodes[i]);
            }
            sort(from, to);
        }

        public void reset(String index) {
            reset(index, 0, modelNodes.length);
        }

        public float weight(ModelNode node) {
            return function.weightWithRebalanceConstraints(balancer, node, index);
        }

        @Override
        protected void swap(int i, int j) {
            final ModelNode tmpNode = modelNodes[i];
            modelNodes[i] = modelNodes[j];
            modelNodes[j] = tmpNode;
            final float tmpWeight = weights[i];
            weights[i] = weights[j];
            weights[j] = tmpWeight;
        }

        @Override
        protected int compare(int i, int j) {
            return Float.compare(weights[i], weights[j]);
        }

        @Override
        protected void setPivot(int i) {
            pivotWeight = weights[i];
        }

        @Override
        protected int comparePivot(int j) {
            return Float.compare(pivotWeight, weights[j]);
        }

        public float delta() {
            return weights[weights.length - 1] - weights[0];
        }
    }
}
