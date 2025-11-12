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

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

import java.util.function.BiPredicate;

/**
 * This {@link AllocationDecider} limits the number of shards per node on a per
 * index or node-wide basis. The allocator prevents a single node to hold more
 * than {@code index.routing.allocation.total_shards_per_node} per index, {@code index.routing.allocation.total_primary_shards_per_node} per index,
 * {@code cluster.routing.allocation.total_shards_per_node} globally and
 * {@code cluster.routing.allocation.total_primary_shards_per_node} globally during the allocation
 * process. The limits of this decider can be changed in real-time via a the
 * index settings API.
 * <p>
 * If {@code index.routing.allocation.total_shards_per_node} or {@code index.routing.allocation.total_primary_shards_per_node}is reset to a negative value shards
 * per index are unlimited per node or primary shards per index are unlimited per node respectively. Shards currently in the
 * {@link ShardRoutingState#RELOCATING relocating} state are ignored by this
 * {@link AllocationDecider} until the shard changed its state to either
 * {@link ShardRoutingState#STARTED started},
 * {@link ShardRoutingState#INITIALIZING inializing} or
 * {@link ShardRoutingState#UNASSIGNED unassigned}
 * <p>
 * Note: Reducing the number of shards per node via the index update API can
 * trigger relocation and significant additional load on the clusters nodes.
 * </p>
 *
 * @opensearch.internal
 */
public class ShardsLimitAllocationDecider extends AllocationDecider {

    public static final String NAME = "shards_limit";

    private volatile int clusterShardLimit;
    private volatile int clusterPrimaryShardLimit;
    private volatile int clusterRemoteCapableShardLimit;

    /**
     * Controls the maximum number of shards per index on a single OpenSearch
     * node. Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> INDEX_TOTAL_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "index.routing.allocation.total_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the maximum number of primary shards per index on a single OpenSearch
     * node for segment replication enabled indices. Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "index.routing.allocation.total_primary_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the maximum number of remote capable shards per index on a single OpenSearch
     * node. Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> INDEX_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "index.routing.allocation.total_remote_capable_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the maximum number of remote capable primary shards per index on a single OpenSearch
     * node. Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> INDEX_TOTAL_REMOTE_CAPABLE_PRIMARY_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "index.routing.allocation.total_remote_capable_primary_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the maximum number of shards per node on a cluster level.
     * Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "cluster.routing.allocation.total_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls the maximum number of primary shards per node on a cluster level.
     * Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "cluster.routing.allocation.total_primary_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls the maximum number of remote capable shards per node on a cluster level.
     * Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> CLUSTER_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "cluster.routing.allocation.total_remote_capable_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );

    private final Settings settings;

    public ShardsLimitAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.settings = settings;
        this.clusterShardLimit = CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.get(settings);
        this.clusterPrimaryShardLimit = CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.get(settings);
        this.clusterRemoteCapableShardLimit = CLUSTER_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING, this::setClusterShardLimit);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING, this::setClusterPrimaryShardLimit);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING,
            this::setClusterRemoteCapableShardLimit
        );
    }

    private void setClusterShardLimit(int clusterShardLimit) {
        this.clusterShardLimit = clusterShardLimit;
    }

    private void setClusterPrimaryShardLimit(int clusterPrimaryShardLimit) {
        this.clusterPrimaryShardLimit = clusterPrimaryShardLimit;
    }

    private void setClusterRemoteCapableShardLimit(int clusterRemoteCapableShardLimit) {
        this.clusterRemoteCapableShardLimit = clusterRemoteCapableShardLimit;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(shardRouting, node, allocation, (count, limit) -> count >= limit);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(shardRouting, node, allocation, (count, limit) -> count > limit);
    }

    private Decision doDecide(
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        BiPredicate<Integer, Integer> decider
    ) {
        RoutingPool shardRoutingPool = RoutingPool.getShardPool(shardRouting, allocation);
        RoutingPool nodeRoutingPool = RoutingPool.getNodePool(node);
        // TargetPoolAllocationDecider will handle for this case, hence short-circuiting from here
        if (shardRoutingPool != nodeRoutingPool) {
            return Decision.ALWAYS;
        }

        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        final int indexShardLimit;
        final int indexPrimaryShardLimit;
        final int clusterShardLimit;
        final int clusterPrimaryShardLimit;
        // Capture the limit here in case it changes during this method's execution
        if (nodeRoutingPool == RoutingPool.REMOTE_CAPABLE) {
            indexShardLimit = indexMetadata.getIndexTotalRemoteCapableShardsPerNodeLimit();
            indexPrimaryShardLimit = indexMetadata.getIndexTotalRemoteCapablePrimaryShardsPerNodeLimit();
            clusterShardLimit = this.clusterRemoteCapableShardLimit;
            clusterPrimaryShardLimit = -1; // No primary shard limit for remote capable nodes
        } else {
            indexShardLimit = indexMetadata.getIndexTotalShardsPerNodeLimit();
            indexPrimaryShardLimit = indexMetadata.getIndexTotalPrimaryShardsPerNodeLimit();
            clusterShardLimit = this.clusterShardLimit;
            clusterPrimaryShardLimit = this.clusterPrimaryShardLimit;
        }

        if (indexShardLimit <= 0 && indexPrimaryShardLimit <= 0 && clusterShardLimit <= 0 && clusterPrimaryShardLimit <= 0) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "total shard limits are disabled: [index: %d, index primary: %d, cluster: %d, cluster primary: %d] <= 0",
                indexShardLimit,
                indexPrimaryShardLimit,
                clusterShardLimit,
                clusterPrimaryShardLimit
            );
        }

        final int nodeShardCount = node.numberOfOwningShards();

        if (clusterShardLimit > 0 && decider.test(nodeShardCount, clusterShardLimit)) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "too many shards [%d] allocated to this node, cluster setting [%s=%d]",
                nodeShardCount,
                nodeRoutingPool == RoutingPool.REMOTE_CAPABLE
                    ? CLUSTER_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING.getKey()
                    : CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
                clusterShardLimit
            );
        }
        if (shardRouting.primary() && clusterPrimaryShardLimit > 0) {
            final int nodePrimaryShardCount = node.numberOfOwningPrimaryShards();
            if (decider.test(nodePrimaryShardCount, clusterPrimaryShardLimit)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "too many primary shards [%d] allocated to this node, cluster setting [%s=%d]",
                    nodePrimaryShardCount,
                    CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(),
                    clusterPrimaryShardLimit
                );
            }
        }
        if (indexShardLimit > 0) {
            final int indexShardCount = node.numberOfOwningShardsForIndex(shardRouting.index());
            if (decider.test(indexShardCount, indexShardLimit)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "too many shards [%d] allocated to this node for index [%s], index setting [%s=%d]",
                    indexShardCount,
                    shardRouting.getIndexName(),
                    shardRoutingPool == RoutingPool.REMOTE_CAPABLE
                        ? INDEX_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING.getKey()
                        : INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
                    indexShardLimit
                );
            }
        }
        if (indexPrimaryShardLimit > 0 && shardRouting.primary()) {
            final int indexPrimaryShardCount = node.numberOfOwningPrimaryShardsForIndex(shardRouting.index());
            if (decider.test(indexPrimaryShardCount, indexPrimaryShardLimit)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "too many primary shards [%d] allocated to this node for index [%s], index setting [%s=%d]",
                    indexPrimaryShardCount,
                    shardRouting.getIndexName(),
                    shardRoutingPool == RoutingPool.REMOTE_CAPABLE
                        ? INDEX_TOTAL_REMOTE_CAPABLE_PRIMARY_SHARDS_PER_NODE_SETTING.getKey()
                        : INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(),
                    indexPrimaryShardLimit
                );
            }
        }
        return allocation.decision(
            Decision.YES,
            NAME,
            "the shard count [%d] for this node is under the index limit [%d], index primary limit [%d], cluster level node limit [%d] and cluster level primary node limit [%d]",
            nodeShardCount,
            indexShardLimit,
            indexPrimaryShardLimit,
            clusterShardLimit,
            clusterPrimaryShardLimit
        );
    }

}
