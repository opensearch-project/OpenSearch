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
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link QueryPlanExecutor} default implementation.
 * <p>
 * Acquires a composite reader, selects a {@link AnalyticsSearchBackendPlugin}, and
 * delegates query execution to it.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;

    /**
     * Constructs a DefaultPlanExecutor.
     *
     * @param providers list of search execution engine providers
     * @param indicesService service for accessing index shards
     * @param clusterService service for accessing cluster state
     */
    public DefaultPlanExecutor(List<AnalyticsSearchBackendPlugin> providers, IndicesService indicesService, ClusterService clusterService) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin provider : providers) {
            this.backEnds.put(provider.name(), provider);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        String tableName = extractTableName(logicalFragment);
        AnalyticsSearchBackendPlugin provider = selectBackEnd();
        if (provider == null) {
            return new ArrayList<>();
        }

        IndexShard shard = resolveShard(tableName);
        IndexReaderProvider indexReaderProvider = shard.getReaderProvider();
        if (indexReaderProvider == null) {
            throw new IllegalStateException("No CompositeEngine on shard [" + shard.shardId() + "]");
        }

        SearchShardTask task = null; // TODO: init task
        List<Object[]> rows = new ArrayList<>();
        try (var dataFormatAwareReader = indexReaderProvider.acquireReader()) {
            ExecutionContext ctx = new ExecutionContext(tableName, task, dataFormatAwareReader.get());
            try (SearchExecEngine<ExecutionContext, EngineResultStream> engine = provider.getSearchExecEngineProvider().createSearchExecEngine(ctx)) {
                logger.info("[DefaultPlanExecutor] Executing via [{}]", provider.name());
                try (EngineResultStream resultStream = engine.execute(ctx)) {
                    Iterator<EngineResultBatch> batchIterator = resultStream.iterator();
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
            throw new RuntimeException("Execution failed for [" + provider.name() + "]", e);
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
        if (backEnds.isEmpty()) {
            logger.warn("No back-end plugins registered — queries will return empty results");
            return null;
        }
        // TODO: select based on data format available in the catalog snapshot
        return backEnds.values().iterator().next();
    }
}
