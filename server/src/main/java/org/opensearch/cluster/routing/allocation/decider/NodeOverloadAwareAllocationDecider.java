/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.Setting.Property;

import java.util.function.BiPredicate;

/**
 * This {@link NodeOverloadAwareAllocationDecider} controls shard over-allocation
 * due to node failures or otherwise on the surviving nodes
 * <pre>
 * cluster.routing.allocation.overload_awareness.provisioned_capacity: N
 * </pre>
 * <p>
 * and prevent allocation on the surviving nodes of the under capacity cluster
 * based on oveload factor defined as a percentage by
 * <pre>
 * cluster.routing.allocation.overload_awareness.factor: X
 * </pre>
 */
public class NodeOverloadAwareAllocationDecider extends AllocationDecider {

    public static final String NAME = "overload_awareness";

    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING =
        Setting.intSetting("cluster.routing.allocation.overload_awareness.provisioned_capacity", -1, -1,
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_FACTOR_SETTING =
        Setting.intSetting("cluster.routing.allocation.overload_awareness.factor", 50, -1, Property.Dynamic,
            Property.NodeScope);
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING =
        Setting.boolSetting("cluster.routing.allocation.overload_awareness.allow_unassigned_primaries",
            true, Setting.Property.Dynamic, Property.NodeScope);

    private volatile int provisionedCapacity;

    private volatile int overloadFactor;

    private volatile boolean allowUnassignedPrimaries;

    private static final Logger logger = LogManager.getLogger(NodeOverloadAwareAllocationDecider.class);

    public NodeOverloadAwareAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.overloadFactor = CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_FACTOR_SETTING.get(settings);
        this.provisionedCapacity = CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.get(settings);
        this.allowUnassignedPrimaries = CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_FACTOR_SETTING,
            this::setOverloadFactor);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING,
            this::setProvisionedCapacity);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING,
            this::setAllowUnassignedPrimaries);
    }

    private void setAllowUnassignedPrimaries(boolean allowUnassignedPrimaries) {
        this.allowUnassignedPrimaries = allowUnassignedPrimaries;
    }

    private void setOverloadFactor(int overloadFactor) {
        this.overloadFactor = overloadFactor;
    }

    private void setProvisionedCapacity(int provisionedCapacity) {
        this.provisionedCapacity = provisionedCapacity;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(shardRouting, node, allocation, (count, limit) -> count >= limit);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(shardRouting, node, allocation, (count, limit) -> count > limit);
    }

    private Decision underCapacity(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation,
                                   BiPredicate<Integer, Integer> decider) {
        if (provisionedCapacity <= 0 || overloadFactor < 0 ) {
            return allocation.decision(Decision.YES, NAME,
                "overload awareness allocation is not enabled, set cluster setting [%s] and cluster se=tting [%s] to enable it",
                CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_FACTOR_SETTING.getKey(),
                CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey());
        }
        if (shardRouting.unassigned() && shardRouting.primary() && allowUnassignedPrimaries) {
            return allocation.decision(Decision.YES, NAME,
                "overload allocation awareness is allowed for unassigned primaries, set cluster setting [%s] to disable it",
                CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey());
        }
        Metadata metadata = allocation.metadata();
        float expectedAvgShardsPerNode = (float) metadata.getTotalNumberOfShards() / provisionedCapacity;
        int nodeShardCount = node.numberOfOwningShards();
        logger.debug(() -> new ParameterizedMessage("Expected shards per node {}, current node shard count {}",
            expectedAvgShardsPerNode, nodeShardCount));
        if (decider.test(nodeShardCount, (int) Math.ceil(expectedAvgShardsPerNode * (1 + overloadFactor / 100.0)))) {
            return allocation.decision(Decision.NO, NAME,
                "too many shards [%d] allocated to this node, cluster setting [%s=%d] based on capacity [%s]",
                nodeShardCount, CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARENESS_FACTOR_SETTING.getKey(), overloadFactor, provisionedCapacity);
        }
        return allocation.decision(Decision.YES, NAME, "node meets all skew awareness attribute requirements");
    }
}
