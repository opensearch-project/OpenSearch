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
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.replication.common.ReplicationType;

import java.util.function.BiPredicate;

/**
 * This {@link AllocationDecider} limits the number of shards per node on a per
 * index or node-wide basis. The allocator prevents a single node to hold more
 * than {@code index.routing.allocation.total_shards_per_node} per index, {@code index.routing.allocation.total_primary_shards_per_node} per index and
 * {@code cluster.routing.allocation.total_shards_per_node} globally during the allocation
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
     * Controls the maximum number of shards per node on a global level.
     * Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "cluster.routing.allocation.total_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );

    private final Settings settings;

    public ShardsLimitAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.settings = settings;
        this.clusterShardLimit = CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING, this::setClusterShardLimit);
    }

    private void setClusterShardLimit(int clusterShardLimit) {
        this.clusterShardLimit = clusterShardLimit;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(shardRouting, node, allocation, (count, limit) -> count >= limit);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(shardRouting, node, allocation, (count, limit) -> count > limit);
    }

    /**
     * Counts the number of primary shards on a given node for a given index
     */
    private int countIndexPrimaryShards(RoutingNode node, ShardRouting shardRouting) {
        Index index = shardRouting.index();
        int count = 0;
        for (ShardRouting shard : node) {
            if (shard.index().equals(index) && shard.primary() && !shard.relocating()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Checks whether the given shardRouting's index uses segment replication
     */
    private boolean isIndexSegmentReplicationUsed(RoutingAllocation allocation, ShardRouting shardRouting) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        Settings indexSettings = indexMetadata.getSettings();
        return IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexSettings) == ReplicationType.SEGMENT;
    }

    private Decision doDecide(
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        BiPredicate<Integer, Integer> decider
    ) {
        final int indexShardLimit = allocation.metadata().getIndexSafe(shardRouting.index()).getIndexTotalShardsPerNodeLimit();
        final int indexPrimaryShardLimit = allocation.metadata()
            .getIndexSafe(shardRouting.index())
            .getIndexTotalPrimaryShardsPerNodeLimit();
        // Capture the limit here in case it changes during this method's
        // execution
        final int clusterShardLimit = this.clusterShardLimit;
        if (indexShardLimit <= 0 && indexPrimaryShardLimit <= 0 && clusterShardLimit <= 0) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "total shard limits are disabled: [index: %d, cluster: %d] <= 0",
                indexShardLimit,
                clusterShardLimit
            );
        }

        final int nodeShardCount = node.numberOfOwningShards();

        if (clusterShardLimit > 0 && decider.test(nodeShardCount, clusterShardLimit)) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "too many shards [%d] allocated to this node, cluster setting [%s=%d]",
                nodeShardCount,
                CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
                clusterShardLimit
            );
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
                    INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
                    indexShardLimit
                );
            }
        }

        final boolean isIndexSegmentReplicationUsed = isIndexSegmentReplicationUsed(allocation, shardRouting);
        if (indexPrimaryShardLimit > 0 && isIndexSegmentReplicationUsed && shardRouting.primary()) {
            final int indexPrimaryShardCount = countIndexPrimaryShards(node, shardRouting);
            if (decider.test(indexPrimaryShardCount, indexPrimaryShardLimit)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "too many primary shards [%d] allocated to this node for index [%s], index setting [%s=%d]",
                    indexPrimaryShardCount,
                    shardRouting.getIndexName(),
                    INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(),
                    indexPrimaryShardLimit
                );
            }
        }
        return allocation.decision(
            Decision.YES,
            NAME,
            "the shard count [%d] for this node is under the index limit [%d] and cluster level node limit [%d]",
            nodeShardCount,
            indexShardLimit,
            clusterShardLimit
        );
    }

}
