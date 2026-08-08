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
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

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
    // Defaults to the setting's declared default; the actual per-query value (snapshotted from
    // the dynamic cluster setting) is injected via setMaxShardsPerQuery before resolve() runs —
    // see ShardFragmentStageExecutionFactory.
    private volatile int maxShardsPerQuery = AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.get(Settings.EMPTY);

    public ShardTargetResolver(RelNode fragment, ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexName = RelNodeUtils.findTableName(fragment);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        if (this.indexName == null) {
            throw new IllegalArgumentException("ShardTargetResolver: no OpenSearchTableScan found in fragment");
        }
    }

    /**
     * Sets the max-shards-per-query limit enforced in {@link #resolve}. Called per query from
     * {@code ShardFragmentStageExecutionFactory} with the value snapshotted from the dynamic
     * {@code analytics.query.max_shards_per_query} cluster setting.
     */
    public void setMaxShardsPerQuery(int maxShardsPerQuery) {
        this.maxShardsPerQuery = maxShardsPerQuery;
    }

    @Override
    public List<ExecutionTarget> resolve(ClusterState clusterState, @Nullable Object childManifest) {
        // Expand the table name (alias or concrete) to its concrete indices against the freshest
        // cluster state. operationRouting().searchShards requires concrete names — aliases are
        // not accepted there — so the expansion has to happen here, not at construction time.
        //
        // Pass an empty referenced-field set so this expansion does NOT re-run cross-index type
        // validation: OpenSearchTableScanRule already validated the query's referenced fields at
        // plan time (scoped to what the query reads). Re-validating here with the default (all
        // fields) would reject a query over a conflicted alias even when it never reads the
        // conflicting field, defeating the plan-time scoping.
        IndexResolution resolution = IndexResolution.resolve(
            indexName,
            clusterState,
            indexNameExpressionResolver,
            java.util.Set.of()
        );
        String[] concreteNames = resolution.concreteIndexNames().toArray(new String[0]);
        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(clusterState, concreteNames, null, null);
        // TODO: Hard rejection in absence of a can-match phase. Without can-match to prune
        // non-matching shards upfront, an unbounded fan-out can overload the coordinator.
        // Once can-match is implemented, this limit can be relaxed or applied post-pruning.
        int shardCount = shardIterators.size();
        if (shardCount > maxShardsPerQuery && resolution.concreteIndices().size() > 1) {
            String sourceType = describeIndexSource(indexName, clusterState);
            throw new IllegalArgumentException(
                "Query via "
                    + sourceType
                    + " targets ["
                    + shardCount
                    + "] shards which exceeds the limit of ["
                    + maxShardsPerQuery
                    + "] set by [analytics.query.max_shards_per_query]. "
                    + "Query an individual backing index directly."
            );
        }
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

    private static String describeIndexSource(String name, ClusterState clusterState) {
        SortedMap<String, IndexAbstraction> lookup = clusterState.metadata().getIndicesLookup();
        IndexAbstraction abstraction = lookup != null ? lookup.get(name) : null;
        if (abstraction != null) {
            return switch (abstraction.getType()) {
                case ALIAS -> "alias [" + name + "]";
                case DATA_STREAM -> "data stream [" + name + "]";
                case CONCRETE_INDEX -> "index [" + name + "]";
            };
        }
        return "index pattern [" + name + "]";
    }
}
