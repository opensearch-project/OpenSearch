/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link QueryPlanExecutor} default implementation.
 * <p>
 * Acquires a composite reader, creates a per-query {@link org.opensearch.analytics.backend.SearchExecEngine}
 * bound to the reader, and delegates convert + execute to it.
 * No backend-specific context is exposed to this class.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    // TODO: - move out as data node side service
    private final AnalyticsQueryService queryService;

    public DefaultPlanExecutor(List<AnalyticsSearchBackendPlugin> plugins, IndicesService indicesService, ClusterService clusterService) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin plugin : plugins) {
            this.backEnds.put(plugin.name(), plugin);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.queryService = new AnalyticsQueryService(backEnds);
        // TODO : init planning components
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        String tableName = extractTableName(logicalFragment);
        IndexMetadata indexMetadata = clusterService.state().metadata().index(tableName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("Index [" + tableName + "] not found in cluster state");
        }
        int shardCount = indexMetadata.getNumberOfShards();

        ResolvedPlan plan = null; // TODO : queryPlanner.plan(logicalFragment, shardCount);

        if ("unresolved".equals(plan.getPrimaryBackend())) {
            throw new IllegalStateException("Planning did not resolve backend assignment for plan root");
        }

        logger.info("[DefaultPlanExecutor] Plan resolved to backend [{}]", plan.getPrimaryBackend());

        IndexShard shard = resolveShard(tableName);
        SearchShardTask task = null; // TODO : init task
        return queryService.execute(plan, shard, task);
    }

    static String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan fragment");
    }

    private IndexShard resolveShard(String indexName) {
        IndexMetadata meta = clusterService.state().metadata().index(indexName);
        if (meta == null) throw new IllegalArgumentException("Index [" + indexName + "] not found");
        IndexService indexService = indicesService.indexService(meta.getIndex());
        if (indexService == null) throw new IllegalStateException("Index [" + indexName + "] not on this node");
        Set<Integer> shardIds = indexService.shardIds();
        if (shardIds.isEmpty()) throw new IllegalStateException("No shards for [" + indexName + "]");
        IndexShard shard = indexService.getShardOrNull(shardIds.iterator().next());
        return shard;
    }

    private AnalyticsSearchBackendPlugin selectBackEnd() {
        if (backEnds.isEmpty()) throw new IllegalStateException("No back-end plugins registered");
        return backEnds.values().iterator().next();
    }
}
