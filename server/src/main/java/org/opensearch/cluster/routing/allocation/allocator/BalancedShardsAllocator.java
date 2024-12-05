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
import java.util.Map;
import java.util.Set;

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

    private volatile boolean movePrimaryFirst;
    private volatile ShardMovementStrategy shardMovementStrategy;

    private volatile boolean preferPrimaryShardBalance;
    private volatile boolean preferPrimaryShardRebalance;
    private volatile float preferPrimaryShardRebalanceBuffer;
    private volatile float indexBalanceFactor;
    private volatile float shardBalanceFactor;
    private volatile WeightFunction weightFunction;
    private volatile float threshold;
    private volatile long primaryConstraintThreshold;

    private volatile boolean ignoreThrottleInRestore;
    private volatile TimeValue allocatorTimeout;
    private long startTime;
    private RerouteService rerouteService;

    public BalancedShardsAllocator(Settings settings) {
        this(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    @Inject
    public BalancedShardsAllocator(Settings settings, ClusterSettings clusterSettings) {
        setShardBalanceFactor(SHARD_BALANCE_FACTOR_SETTING.get(settings));
        setIndexBalanceFactor(INDEX_BALANCE_FACTOR_SETTING.get(settings));
        setPreferPrimaryShardRebalanceBuffer(PRIMARY_SHARD_REBALANCE_BUFFER.get(settings));
        setIgnoreThrottleInRestore(IGNORE_THROTTLE_FOR_REMOTE_RESTORE.get(settings));
        updateWeightFunction();
        setThreshold(THRESHOLD_SETTING.get(settings));
        setPrimaryConstraintThresholdSetting(PRIMARY_CONSTRAINT_THRESHOLD_SETTING.get(settings));
        setPreferPrimaryShardBalance(PREFER_PRIMARY_SHARD_BALANCE.get(settings));
        setPreferPrimaryShardRebalance(PREFER_PRIMARY_SHARD_REBALANCE.get(settings));
        setShardMovementStrategy(SHARD_MOVEMENT_STRATEGY_SETTING.get(settings));
        setAllocatorTimeout(ALLOCATOR_TIMEOUT_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(PREFER_PRIMARY_SHARD_BALANCE, this::setPreferPrimaryShardBalance);
        clusterSettings.addSettingsUpdateConsumer(SHARD_MOVE_PRIMARY_FIRST_SETTING, this::setMovePrimaryFirst);
        clusterSettings.addSettingsUpdateConsumer(SHARD_MOVEMENT_STRATEGY_SETTING, this::setShardMovementStrategy);
        clusterSettings.addSettingsUpdateConsumer(INDEX_BALANCE_FACTOR_SETTING, this::updateIndexBalanceFactor);
        clusterSettings.addSettingsUpdateConsumer(SHARD_BALANCE_FACTOR_SETTING, this::updateShardBalanceFactor);
        clusterSettings.addSettingsUpdateConsumer(PRIMARY_SHARD_REBALANCE_BUFFER, this::updatePreferPrimaryShardBalanceBuffer);
        clusterSettings.addSettingsUpdateConsumer(PREFER_PRIMARY_SHARD_REBALANCE, this::setPreferPrimaryShardRebalance);
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTING, this::setThreshold);
        clusterSettings.addSettingsUpdateConsumer(PRIMARY_CONSTRAINT_THRESHOLD_SETTING, this::setPrimaryConstraintThresholdSetting);
        clusterSettings.addSettingsUpdateConsumer(IGNORE_THROTTLE_FOR_REMOTE_RESTORE, this::setIgnoreThrottleInRestore);
        clusterSettings.addSettingsUpdateConsumer(ALLOCATOR_TIMEOUT_SETTING, this::setAllocatorTimeout);
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

    private void updatePreferPrimaryShardBalanceBuffer(float preferPrimaryShardBalanceBuffer) {
        this.preferPrimaryShardRebalanceBuffer = preferPrimaryShardBalanceBuffer;
        updateWeightFunction();
    }

    private void updateWeightFunction() {
        weightFunction = new WeightFunction(
            this.indexBalanceFactor,
            this.shardBalanceFactor,
            this.preferPrimaryShardRebalanceBuffer,
            this.primaryConstraintThreshold
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
            assert rerouteService != null : "RerouteService not set to schedule reroute after allocator time out";
            rerouteService.reroute(
                "reroute after balanced shards allocator timed out",
                Priority.HIGH,
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
     * </ul>
     * <code>weight(node, index) = weight<sub>index</sub>(node, index) + weight<sub>node</sub>(node, index)</code>
     * <p>
     * package-private for testing
     */
    static class WeightFunction {

        private final float indexBalance;
        private final float shardBalance;
        private final float theta0;
        private final float theta1;
        private long primaryConstraintThreshold;
        private AllocationConstraints constraints;
        private RebalanceConstraints rebalanceConstraints;

        WeightFunction(float indexBalance, float shardBalance, float preferPrimaryBalanceBuffer, long primaryConstraintThreshold) {
            float sum = indexBalance + shardBalance;
            if (sum <= 0.0f) {
                throw new IllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
            }
            theta0 = shardBalance / sum;
            theta1 = indexBalance / sum;
            this.indexBalance = indexBalance;
            this.shardBalance = shardBalance;
            this.primaryConstraintThreshold = primaryConstraintThreshold;
            RebalanceParameter rebalanceParameter = new RebalanceParameter(preferPrimaryBalanceBuffer);
            this.constraints = new AllocationConstraints();
            this.rebalanceConstraints = new RebalanceConstraints(rebalanceParameter);
            // Enable index shard per node breach constraint
            updateAllocationConstraint(INDEX_SHARD_PER_NODE_BREACH_CONSTRAINT_ID, true);
        }

        public float weightWithAllocationConstraints(ShardsBalancer balancer, ModelNode node, String index) {
            float balancerWeight = weight(balancer, node, index);
            return balancerWeight + constraints.weight(balancer, node, index, primaryConstraintThreshold);
        }

        public float weightWithRebalanceConstraints(ShardsBalancer balancer, ModelNode node, String index) {
            float balancerWeight = weight(balancer, node, index);
            return balancerWeight + rebalanceConstraints.weight(balancer, node, index, primaryConstraintThreshold);
        }

        float weight(ShardsBalancer balancer, ModelNode node, String index) {
            final float weightShard = node.numShards() - balancer.avgShardsPerNode();
            final float weightIndex = node.numShards(index) - balancer.avgShardsPerNode(index);
            return theta0 * weightShard + theta1 * weightIndex;
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
        private final RoutingNode routingNode;

        ModelNode(RoutingNode routingNode) {
            this.routingNode = routingNode;
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
