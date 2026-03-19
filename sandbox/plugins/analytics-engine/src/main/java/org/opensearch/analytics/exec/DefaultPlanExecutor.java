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
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.plugins.spi.vectorized.DataFormat;
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
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsBackEndPlugin> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;

    /**
     * Creates a plan executor with the given back-end plugins and services.
     * Plugins are stored in a {@link LinkedHashMap} keyed by {@link AnalyticsBackEndPlugin#name()},
     * preserving insertion order. If duplicate names exist, the last plugin in the list wins.
     *
     * @param plugins        registered back-end engine plugins
     * @param indicesService the indices service used to look up shards
     * @param clusterService the cluster service used to resolve index metadata
     */
    @Inject
    public DefaultPlanExecutor(
        List<AnalyticsBackEndPlugin> plugins,
        IndicesService indicesService,
        ClusterService clusterService
    ) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsBackEndPlugin plugin : plugins) {
            this.backEnds.put(plugin.name(), plugin);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        AnalyticsBackEndPlugin plugin = selectBackEnd();
        int fieldCount = logicalFragment.getRowType().getFieldCount();
        logger.info("[DefaultPlanExecutor] Executing fragment with {} fields via back-end [{}]",
            fieldCount, plugin.name());

        String tableName = extractTableName(logicalFragment);
        List<ShardId> shardIds = resolveShardIds(tableName);
        if (shardIds.isEmpty()) {
            throw new IllegalStateException("No shards found for index [" + tableName + "]");
        }
        IndexShard indexShard = getIndexShard(shardIds.get(0));
        if (indexShard == null) {
            throw new IllegalStateException("Shard [" + shardIds.get(0) + "] not found on this node");
        }

        CompositeEngine engine = (CompositeEngine) indexShard.getIndexer();

        // Prefer SearchExecEngine path if supported
        if (plugin.supportsSearchExecEngine() && engine.getSearchBackendFactory() != null) {
            logger.info("[DefaultPlanExecutor] Using SearchExecEngine path for back-end [{}]", plugin.name());
            try {
                return executeViaSearchExecEngine(engine, logicalFragment);
            } catch (Exception e) {
                throw new RuntimeException("SearchExecEngine execution failed for [" + plugin.name() + "]", e);
            }
        }

        // Bridge path
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> snapshot = engine.acquireSnapshot()) {
            EngineBridge<byte[], ? extends EngineResultStream, RelNode> bridge =
                (EngineBridge<byte[], ? extends EngineResultStream, RelNode>) plugin.bridge(snapshot.getRef());
            byte[] converted = bridge.convertFragment(logicalFragment);

            List<Object[]> rows = new ArrayList<>();
            try (EngineResultStream resultStream = bridge.execute(converted)) {
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

            logger.info("[DefaultPlanExecutor] Execution completed via back-end [{}], {} rows",
                plugin.name(), rows.size());
            return rows;
        } catch (Exception e) {
            logger.error("[DefaultPlanExecutor] Execution failed via back-end [{}]: {}",
                plugin.name(), e.getMessage(), e);
            throw new RuntimeException("Execution failed for back-end [" + plugin.name() + "]: " + e.getMessage(), e);
        }
    }

    /**
     * Walks the {@link RelNode} tree to find the first {@link TableScan} and
     * returns the table name (last element of the qualified name).
     *
     * @param node the root of the plan fragment
     * @return the table name
     * @throws IllegalArgumentException if no TableScan is found
     */
    static String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qualifiedName = node.getTable().getQualifiedName();
            return qualifiedName.get(qualifiedName.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) {
                return name;
            }
        }
        throw new IllegalArgumentException("No TableScan found in plan fragment: " + node.explain());
    }

    /**
     * Resolves an index name to the set of {@link ShardId}s that are allocated
     * on this node.
     *
     * @param indexName the index name
     * @return list of shard ids for the index on this node
     * @throws IllegalArgumentException if the index does not exist in cluster state
     * @throws IllegalStateException    if the index is not allocated on this node
     */
    List<ShardId> resolveShardIds(String indexName) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("Index [" + indexName + "] not found in cluster state");
        }

        IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
        if (indexService == null) {
            throw new IllegalStateException("Index [" + indexName + "] is not allocated on this node");
        }

        Set<Integer> localShardIds = indexService.shardIds();
        List<ShardId> shardIds = new ArrayList<>(localShardIds.size());
        for (int id : localShardIds) {
            shardIds.add(new ShardId(indexMetadata.getIndex(), id));
        }
        return shardIds;
    }

    /**
     * Fetches an {@link IndexShard} by its {@link ShardId}.
     *
     * @param shardId the shard identifier
     * @return the index shard, or {@code null} if the index or shard is not found on this node
     */
    IndexShard getIndexShard(ShardId shardId) {
        IndexService indexService = indicesService.indexService(shardId.getIndex());
        if (indexService == null) {
            return null;
        }
        return indexService.getShardOrNull(shardId.id());
    }

    /**
     * Selects the back-end plugin to use for execution.
     * <p>
     * If a single plugin is registered, it is returned. If multiple plugins are registered,
     * the first in iteration order (insertion order of the {@link LinkedHashMap}) is returned.
     *
     * @return the selected back-end plugin
     * @throws IllegalStateException if no back-end plugins are registered
     */
    @SuppressWarnings("unchecked")
    private List<Object[]> executeViaSearchExecEngine(
        CompositeEngine compositeEngine,
        RelNode logicalFragment
    ) throws Exception {
        DataFormat format = DataFormat.PARQUET;

        var searchEngine = (org.opensearch.index.engine.exec.SearchExecEngine) compositeEngine.getSearchExecBackendEngine(format);
        // TODO: get composite reader from compositeEngine directly (compositeEngine owns the snapshot)
        Object reader = compositeEngine.getReader(format);
        Object plan = searchEngine.convertFragment(logicalFragment);
        var context = searchEngine.createContext(reader, null, null, null);
        Iterator<?> result = searchEngine.executePlan(plan, context);

        // Read results — executePlan returns Iterator with a single EngineResultStream
        List<Object[]> rows = new ArrayList<>();
        while (result.hasNext()) {
            Object item = result.next();
            if (item instanceof EngineResultStream) {
                try (EngineResultStream resultStream = (EngineResultStream) item) {
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
        }

        logger.info("[DefaultPlanExecutor] SearchExecEngine completed, {} rows", rows.size());
        return rows;
    }

    private AnalyticsBackEndPlugin selectBackEnd() {
        if (backEnds.isEmpty()) {
            throw new IllegalStateException("No analytics back-end plugins registered");
        }
        return backEnds.values().iterator().next();
    }
}
