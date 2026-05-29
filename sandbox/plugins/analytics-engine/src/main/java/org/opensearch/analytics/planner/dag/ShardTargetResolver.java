/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.IndexResolution;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
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
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public ShardTargetResolver(RelNode fragment, ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexName = RelNodeUtils.findTableName(fragment);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        if (this.indexName == null) {
            throw new IllegalArgumentException("ShardTargetResolver: no OpenSearchTableScan found in fragment");
        }
    }

    @Override
    public List<ExecutionTarget> resolve(ClusterState clusterState, @Nullable Object childManifest) {
        // Expand the table name (alias or concrete) to its concrete indices against the freshest
        // cluster state. operationRouting().searchShards requires concrete names — aliases are
        // not accepted there — so the expansion has to happen here, not at construction time.
        IndexResolution resolution = IndexResolution.resolve(indexName, clusterState, indexNameExpressionResolver);
        String[] concreteNames = resolution.concreteIndexNames().toArray(new String[0]);
        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(clusterState, concreteNames, null, null);
        List<ExecutionTarget> targets = new ArrayList<>();
        int ordinal = 0;
        for (ShardIterator shardIt : shardIterators) {
            ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                DiscoveryNode node = clusterState.nodes().get(shard.currentNodeId());
                if (node != null) {
                    // Pass the remaining iterator + cluster state to the target so dispatch
                    // failure can fall over to a replica copy via ShardExecutionTarget.nextCopy.
                    targets.add(new ShardExecutionTarget(node, shard.shardId(), ordinal++, shardIt, clusterState));
                }
            }
        }
        return targets;
    }

}
