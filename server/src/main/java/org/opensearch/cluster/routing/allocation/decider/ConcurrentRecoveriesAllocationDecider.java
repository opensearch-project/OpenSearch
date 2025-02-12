/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

/**
 * This {@link AllocationDecider} controls the number of currently in-progress
 * re-balance (relocation) operations and restricts node allocations if the
 * configured threshold is reached.
 * <p>
 * Re-balance operations can be controlled in real-time via the cluster update API using
 * <code>cluster.routing.allocation.cluster_concurrent_recoveries</code>. Iff this
 * setting is set to <code>-1</code> the number of cluster concurrent recoveries operations
 * are unlimited.
 *
 * @opensearch.internal
 */
public class ConcurrentRecoveriesAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(ConcurrentRecoveriesAllocationDecider.class);

    public static final String NAME = "cluster_concurrent_recoveries";

    public static final Setting<Integer> CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING = Setting.intSetting(
        "cluster.routing.allocation.cluster_concurrent_recoveries",
        -1,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile int clusterConcurrentRecoveries;

    public ConcurrentRecoveriesAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.clusterConcurrentRecoveries = CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING.get(settings);
        logger.debug("using [cluster_concurrent_rebalance] with [{}]", clusterConcurrentRecoveries);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING,
            this::setClusterConcurrentRebalance
        );
    }

    private void setClusterConcurrentRebalance(int clusterConcurrentRecoveries) {
        this.clusterConcurrentRecoveries = clusterConcurrentRecoveries;
    }

    @Override
    public Decision canMoveAnyShard(RoutingAllocation allocation) {
        if (clusterConcurrentRecoveries == -1) {
            return allocation.decision(Decision.YES, NAME, "undefined cluster concurrent recoveries");
        }
        int relocatingShards = allocation.routingNodes().getRelocatingShardCount();
        if (relocatingShards >= clusterConcurrentRecoveries) {
            return allocation.decision(
                Decision.THROTTLE,
                NAME,
                "too many shards are concurrently relocating [%d], limit: [%d] cluster setting [%s=%d]",
                relocatingShards,
                clusterConcurrentRecoveries,
                CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_RECOVERIES_SETTING.getKey(),
                clusterConcurrentRecoveries
            );
        }
        return allocation.decision(
            Decision.YES,
            NAME,
            "below threshold [%d] for concurrent recoveries, current relocating shard count [%d]",
            clusterConcurrentRecoveries,
            relocatingShards
        );
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canMoveAnyShard(allocation);
    }

}
