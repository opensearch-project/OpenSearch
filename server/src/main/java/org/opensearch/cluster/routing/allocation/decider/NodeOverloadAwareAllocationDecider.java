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
 * cluster.routing.allocation.overload_aware.capacity: N
 * </pre>
 * <p>
 * and prevent allocation on the surviving nodes of the under capacity cluster
 * based on a skewness limit defined as a percentage by
 * <pre>
 * cluster.routing.allocation.overload_aware.limit: X
 * </pre>
 */
public class NodeOverloadAwareAllocationDecider extends AllocationDecider {

    public static final String NAME = "overload_aware";

    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_TOTAL_CAPACITY_SETTING =
        Setting.intSetting("cluster.routing.allocation.overload_aware.capacity", -1, -1, Property.Dynamic, Property.NodeScope);
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_LIMIT_SETTING =
        Setting.intSetting("cluster.routing.allocation.overload_aware.limit", 50, -1, Property.Dynamic, Property.NodeScope);
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_ALLOW_UNASSIGNED_PRIMARIES_SETTING =
        Setting.boolSetting("cluster.routing.allocation.overload_aware.allow_unassigned_primaries",
            true, Setting.Property.Dynamic, Property.NodeScope);

    private volatile int totalCapacity;

    private volatile int skewnessLimit;

    private volatile boolean allowUnassignedPrimaries;

    private static final Logger logger = LogManager.getLogger(NodeOverloadAwareAllocationDecider.class);

    public NodeOverloadAwareAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.skewnessLimit = CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_LIMIT_SETTING.get(settings);
        this.totalCapacity = CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_TOTAL_CAPACITY_SETTING.get(settings);
        this.allowUnassignedPrimaries = CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_ALLOW_UNASSIGNED_PRIMARIES_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_LIMIT_SETTING,
            this::setSkewnessLimit);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_TOTAL_CAPACITY_SETTING,
            this::setTotalCapacity);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_ALLOW_UNASSIGNED_PRIMARIES_SETTING,
            this::setAllowUnassignedPrimaries);
    }

    private void setAllowUnassignedPrimaries(boolean allowUnassignedPrimaries) {
        this.allowUnassignedPrimaries = allowUnassignedPrimaries;
    }

    private void setSkewnessLimit(int skewnessLimit) {
        this.skewnessLimit = skewnessLimit;
    }

    private void setTotalCapacity(int totalCapacity) {
        this.totalCapacity = totalCapacity;
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
        if (totalCapacity <= 0 || skewnessLimit < 0 ) {
            return allocation.decision(Decision.YES, NAME,
                "overload awareness allocation is not enabled, set cluster setting [%s] and cluster se=tting [%s] to enable it",
                CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_LIMIT_SETTING.getKey(),
                CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_TOTAL_CAPACITY_SETTING.getKey());
        }
        if (shardRouting.unassigned() && shardRouting.primary() && allowUnassignedPrimaries) {
            return allocation.decision(Decision.YES, NAME,
                "overload allocation awareness is allowed for unassigned primaries, set cluster setting [%s] to disable it",
                CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey());
        }
        Metadata metadata = allocation.metadata();
        float expectedAvgShardsPerNode = (float) metadata.getTotalNumberOfShards() / totalCapacity;
        int nodeShardCount = node.numberOfOwningShards();
        logger.debug(() -> new ParameterizedMessage("Expected shards per node {}, current node shard count {}",
            expectedAvgShardsPerNode, nodeShardCount));
        if (decider.test(nodeShardCount, (int) Math.ceil(expectedAvgShardsPerNode * (1 + skewnessLimit / 100.0)))) {
            return allocation.decision(Decision.NO, NAME,
                "too many shards [%d] allocated to this node, cluster setting [%s=%d] based on capacity [%s]",
                nodeShardCount, CLUSTER_ROUTING_ALLOCATION_OVERLOAD_AWARE_LIMIT_SETTING.getKey(), skewnessLimit, totalCapacity);
        }
        return allocation.decision(Decision.YES, NAME, "node meets all skew awareness attribute requirements");
    }
}
