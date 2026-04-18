/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;

/**
 * Stateless utility for resolving target shards/nodes for a stage.
 * With shuffle and broadcast removed, this only handles index-shard resolution
 * for DATA_NODE scan stages.
 *
 * @opensearch.internal
 */
final class TargetResolver {

    private TargetResolver() {}

    /**
     * Resolve target shards/nodes based on the stage's properties.
     * Uses {@code stage.getTableName()} — no RelNode tree walking needed.
     */
    public static List<ShardTarget> resolveTargets(
        Stage stage,
        ClusterService clusterService,
        QueryContext config
    ) {
        if (stage.getTableName() != null) {
            return resolveIndexShards(stage.getTableName(), clusterService);
        }
        return List.of();
    }

    static List<ShardTarget> resolveIndexShards(String tableName, ClusterService clusterService) {
        ClusterState state = clusterService.state();
        // TODO: support routing/preference params?
        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(state, new String[] { tableName }, null, null);

        List<ShardTarget> targets = new ArrayList<>();
        for (ShardIterator shardIt : shardIterators) {
            ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                DiscoveryNode node = state.nodes().get(shard.currentNodeId());
                targets.add(new ShardTarget(shard.shardId(), node));
            }
        }
        return targets;
    }
}
