/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Resolves {@link ShardExecutionTarget}s for a DATA_NODE scan stage.
 * Extracts the index name from the fragment at construction time, then
 * resolves shard targets lazily when the Scheduler calls {@link #resolve}.
 *
 * <p>Shard routing gives both the node and the shardId in one pass —
 * these are coupled and cannot be separated, hence a dedicated resolver
 * rather than {@link ComposableTargetResolver}.
 *
 * @opensearch.internal
 */
public class ShardTargetResolver extends TargetResolver {

    private final String indexName;
    private final ClusterService clusterService;

    public ShardTargetResolver(RelNode fragment, ClusterService clusterService) {
        this.indexName = findTableName(fragment);
        this.clusterService = clusterService;
        if (this.indexName == null) {
            throw new IllegalArgumentException("ShardTargetResolver: no OpenSearchTableScan found in fragment");
        }
    }

    @Override
    public List<ExecutionTarget> resolve(ClusterState clusterState, @Nullable Object childManifest) {
        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(clusterState, new String[] { indexName }, null, null);
        List<ExecutionTarget> targets = new ArrayList<>();
        for (ShardIterator shardIt : shardIterators) {
            ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                DiscoveryNode node = clusterState.nodes().get(shard.currentNodeId());
                if (node != null) {
                    targets.add(new ShardExecutionTarget(node, shard.shardId()));
                }
            }
        }
        return targets;
    }

    private static String findTableName(RelNode node) {
        if (node instanceof OpenSearchTableScan scan) {
            return scan.getTable().getQualifiedName().getLast();
        }
        for (RelNode input : node.getInputs()) {
            String name = findTableName(input);
            if (name != null) return name;
        }
        return null;
    }
}
