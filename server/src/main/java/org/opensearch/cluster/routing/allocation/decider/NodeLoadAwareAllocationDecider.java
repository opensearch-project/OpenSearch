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
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

import java.util.function.BiPredicate;

/**
 * This {@link NodeLoadAwareAllocationDecider} controls shard over-allocation
 * due to node failures or otherwise on the surviving nodes. The allocation limits
 * are decided by the user provisioned capacity, to determine if there were lost nodes.
 * The provisioned capacity as defined by the below settings needs to be updated on every
 * cluster scale up and scale down operations.
 * <pre>
 * cluster.routing.allocation.overload_awareness.provisioned_capacity: N
 * </pre>
 * <p>
 * and prevent allocation on the surviving nodes of the under capacity cluster
 * based on overload factor defined as a percentage and flat skew as absolute allowed skewness by
 * </p>
 * <pre>
 * cluster.routing.allocation.load_awareness.skew_factor: X
 * cluster.routing.allocation.load_awareness.flat_skew: N
 * </pre>
 * The total limit per node based on skew_factor and flat_skew doesn't limit primaries that previously
 * existed on the disk as those shards are force allocated by
 * {@link AllocationDeciders#canForceAllocatePrimary(ShardRouting, RoutingNode, RoutingAllocation)}
 * however new primaries due to index creation, snapshot restore etc can be controlled via the below settings.
 * Setting the value to true allows newly created primaries to get assigned while preventing the replica allocation
 * breaching the skew factor.
 * Note that setting this to false can result in the primaries not get assigned and the cluster turning RED
 * <pre>
 * cluster.routing.allocation.load_awareness.allow_unassigned_primaries
 * </pre>
 *
 * @opensearch.internal
 */
public class NodeLoadAwareAllocationDecider extends AllocationDecider {

    public static final String NAME = "load_awareness";

    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING = Setting.intSetting(
        "cluster.routing.allocation.load_awareness.provisioned_capacity",
        -1,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Double> CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING = Setting.doubleSetting(
        "cluster.routing.allocation.load_awareness.skew_factor",
        50,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.load_awareness.allow_unassigned_primaries",
        true,
        Setting.Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FLAT_SKEW_SETTING = Setting.intSetting(
        "cluster.routing.allocation.load_awareness.flat_skew",
        2,
        2,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile int provisionedCapacity;

    private volatile double skewFactor;

    private volatile boolean allowUnassignedPrimaries;

    private volatile int flatSkew;

    private static final Logger logger = LogManager.getLogger(NodeLoadAwareAllocationDecider.class);

    public NodeLoadAwareAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.skewFactor = CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.get(settings);
        this.provisionedCapacity = CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.get(settings);
        this.allowUnassignedPrimaries = CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.get(settings);
        this.flatSkew = CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FLAT_SKEW_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING, this::setSkewFactor);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING,
            this::setProvisionedCapacity
        );
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING,
            this::setAllowUnassignedPrimaries
        );
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_FLAT_SKEW_SETTING, this::setFlatSkew);
    }

    private void setAllowUnassignedPrimaries(boolean allowUnassignedPrimaries) {
        this.allowUnassignedPrimaries = allowUnassignedPrimaries;
    }

    private void setSkewFactor(double skewFactor) {
        this.skewFactor = skewFactor;
    }

    private void setProvisionedCapacity(int provisionedCapacity) {
        this.provisionedCapacity = provisionedCapacity;
    }

    private void setFlatSkew(int flatSkew) {
        this.flatSkew = flatSkew;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(shardRouting, node, allocation, (count, limit) -> count >= limit);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(shardRouting, node, allocation, (count, limit) -> count > limit);
    }

    private Decision underCapacity(
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        BiPredicate<Integer, Integer> decider
    ) {
        if (provisionedCapacity <= 0 || skewFactor < 0) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "overload awareness allocation is not enabled, set cluster setting [%s] and cluster setting [%s] to enable it",
                CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_SKEW_FACTOR_SETTING.getKey(),
                CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_PROVISIONED_CAPACITY_SETTING.getKey()
            );
        }
        if (shardRouting.unassigned() && shardRouting.primary() && allowUnassignedPrimaries) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "overload allocation awareness is allowed for unassigned primaries, set cluster setting [%s] to disable it",
                CLUSTER_ROUTING_ALLOCATION_LOAD_AWARENESS_ALLOW_UNASSIGNED_PRIMARIES_SETTING.getKey()
            );
        }
        Metadata metadata = allocation.metadata();
        float expectedAvgShardsPerNode = (float) metadata.getTotalNumberOfShards() / provisionedCapacity;
        int nodeShardCount = node.numberOfOwningShards();
        int limit = flatSkew + (int) Math.ceil(expectedAvgShardsPerNode * (1 + skewFactor / 100.0));
        if (decider.test(nodeShardCount, limit)) {
            logger.debug(
                () -> new ParameterizedMessage(
                    "Too many shards [{}] allocated to this node [{}]. Expected average shards"
                        + " per node [{}], overload factor [{}], node limit [{}]",
                    nodeShardCount,
                    node.nodeId(),
                    expectedAvgShardsPerNode,
                    skewFactor,
                    limit
                )
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "too many shards [%d] allocated to this node, limit per node [%d] considering"
                    + " overload factor [%.2f] and flat skew [%d] based on capacity [%d]",
                nodeShardCount,
                limit,
                skewFactor,
                flatSkew,
                provisionedCapacity
            );
        }
        return allocation.decision(Decision.YES, NAME, "node meets all skew awareness attribute requirements");
    }
}
