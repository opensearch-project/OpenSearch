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
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link QueryPlanExecutor} default implementation.
 * <p>
 * Acquires a composite reader, creates a per-query {@link SearchExecEngine}
 * bound to the reader, and delegates convert + execute to it.
 * No backend-specific context is exposed to this class.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;

    /**
     * Constructs a DefaultPlanExecutor with the given plugins and services.
     *
     * @param plugins list of analytics search backend plugins
     * @param indicesService service for accessing index shards
     * @param clusterService service for accessing cluster state
     */
    public DefaultPlanExecutor(List<AnalyticsSearchBackendPlugin> plugins, IndicesService indicesService, ClusterService clusterService) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin plugin : plugins) {
            this.backEnds.put(plugin.name(), plugin);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        String tableName = extractTableName(logicalFragment);
        String backendName = selectBackEnd().name();

        IndexShard shard = resolveShard(tableName);
        DataFormatAwareEngine dataFormatAwareEngine = shard.getCompositeEngine();
        if (dataFormatAwareEngine == null) {
            throw new IllegalStateException("No CompositeEngine on shard [" + shard.shardId() + "]");
        }

        AnalyticsSearchBackendPlugin plugin = backEnds.get(backendName);
        SearchShardTask task = null; // TODO : init task
        List<Object[]> rows = new ArrayList<>();
        try (DataFormatAwareEngine.DataFormatAwareReader dataFormatAwareReader = dataFormatAwareEngine.acquireReader()) {
            ExecutionContext ctx = new ExecutionContext(tableName, task, dataFormatAwareReader);
            try (SearchExecEngine engine = plugin.searcher(ctx)) {
                logger.info("[DefaultPlanExecutor] Executing via [{}]", plugin.name());
                try (EngineResultStream resultStream = engine.execute(ctx)) {
                    EngineResultBatchIterator batchIterator = resultStream.iterator();
                    while (batchIterator.hasNext()) {
                        EngineResultBatch batch = batchIterator.next();
                        List<String> fieldNames = batch.getFieldNames();
                        for (int row = 0; row < batch.getRowCount(); row++) {
                            Object[] rowValues = new Object[fieldNames.size()];
                            for (int col = 0; col < fieldNames.size(); col++) {
                                rowValues[col] = batch.getFieldValue(fieldNames.get(col), row);
                            }
                            rows.add(rowValues);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Execution failed for [" + plugin.name() + "]", e);
        }
        return rows;
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
        IndexService indexService = indicesService.indexService(clusterService.state().metadata().index(indexName).getIndex());
        if (indexService == null) throw new IllegalStateException("Index [" + indexName + "] not on this node");
        Set<Integer> shardIds = indexService.shardIds();
        if (shardIds.isEmpty()) throw new IllegalStateException("No shards for [" + indexName + "]");
        return indexService.getShardOrNull(shardIds.iterator().next());
    }

    private AnalyticsSearchBackendPlugin selectBackEnd() {
        if (backEnds.isEmpty()) throw new IllegalStateException("No back-end plugins registered");
        return backEnds.values().iterator().next();
    }
}
