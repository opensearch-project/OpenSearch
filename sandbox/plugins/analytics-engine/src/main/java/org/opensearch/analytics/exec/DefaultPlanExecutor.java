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
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.index.engine.exec.FilterTreeCallbackBridge;
import org.opensearch.index.engine.exec.IndexFilterTreeContext;
import org.opensearch.index.engine.exec.IndexFilterTreeProvider;
import org.opensearch.index.engine.exec.SearchBackendFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
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
    private final SearchBackendFactory searchBackendFactory;

    /**
     * Constructs a DefaultPlanExecutor.
     *
     * @param providers list of search execution engine providers
     * @param indicesService service for accessing index shards
     * @param clusterService service for accessing cluster state
     */
    public DefaultPlanExecutor(List<AnalyticsSearchBackendPlugin> providers, IndicesService indicesService, ClusterService clusterService) {
        this(providers, indicesService, clusterService, new SearchBackendFactory());
    }

    /**
     * Constructs a DefaultPlanExecutor with an explicit SearchBackendFactory.
     *
     * @param providers list of search execution engine providers
     * @param indicesService service for accessing index shards
     * @param clusterService service for accessing cluster state
     * @param searchBackendFactory registry for tree query providers
     */
    public DefaultPlanExecutor(
        List<AnalyticsSearchBackendPlugin> providers,
        IndicesService indicesService,
        ClusterService clusterService,
        SearchBackendFactory searchBackendFactory
    ) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin provider : providers) {
            this.backEnds.put(provider.name(), provider);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.searchBackendFactory = searchBackendFactory;
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        // TODO: replace this direct execution path with PlannerImpl → QueryDAG → Scheduler.
        // PlannerImpl.createPlan() returns a QueryDAG; the Scheduler traverses it bottom-up,
        // dispatches FragmentExecutionRequests to data nodes, and streams results back.
        String tableName = extractTableName(logicalFragment);
        AnalyticsSearchBackendPlugin backendPlugin = selectBackEnd();
        if (backendPlugin == null) {
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
            try (
                SearchExecEngine<ExecutionContext, EngineResultStream> engine = backendPlugin.getSearchExecEngineProvider()
                    .createSearchExecEngine(ctx)
            ) {
                logger.info("[DefaultPlanExecutor] Executing via [{}]", backendPlugin.name());
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
            throw new RuntimeException("Execution failed for [" + backendPlugin.name() + "]", e);
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

    /**
     * Returns the SearchBackendFactory for registering tree query providers.
     *
     * @return the search backend factory
     */
    public SearchBackendFactory getSearchBackendFactory() {
        return searchBackendFactory;
    }

    // ── Tree Query Execution ────────────────────────────────────────────

    /**
     * Executes a query using the boolean filter tree path.
     * <p>
     * Orchestrates the full lifecycle: normalize tree → group CollectorLeaf by providerId →
     * look up providers from SearchBackendFactory → create tree contexts → register with
     * FilterTreeCallbackBridge → serialize tree → delegate to primary engine → cleanup.
     *
     * @param tableName      the target table name
     * @param substraitBytes serialized substrait plan bytes
     * @param tree           the boolean filter tree
     * @param collectorQueries per-provider queries indexed by providerId → query array
     * @param engine         the primary search execution engine to delegate to
     * @param ctx            the execution context
     * @return list of result rows
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    List<Object[]> executeViaTreeQuery(
        String tableName,
        byte[] substraitBytes,
        IndexFilterTree tree,
        Map<Integer, Object[]> collectorQueries,
        SearchExecEngine<ExecutionContext, EngineResultStream> engine,
        ExecutionContext ctx
    ) {
        long contextId = 0;
        List<IndexFilterTreeContext<?>> createdContexts = new ArrayList<>();

        try {
            // Step 1: Normalize the tree (De Morgan's NOT push-down)
            IndexFilterTree normalizedTree = tree.normalize();

            // Step 2: Group CollectorLeaf by providerId and create tree contexts
            // The collectorQueries map is keyed by providerId, each value is the query array for that provider
            contextId = FilterTreeCallbackBridge.createContext();

            for (Map.Entry<Integer, Object[]> entry : collectorQueries.entrySet()) {
                int providerId = entry.getKey();
                Object[] queries = entry.getValue();

                IndexFilterTreeProvider provider = searchBackendFactory.getProvider(providerId);

                // Create tree context — the provider knows its own query/reader types
                IndexFilterTreeContext<?> treeContext = provider.createTreeContext(
                    queries,
                    ctx.getReader(),
                    normalizedTree
                );
                createdContexts.add(treeContext);

                FilterTreeCallbackBridge.registerProvider(contextId, providerId, provider, treeContext);
            }

            // Step 3: Serialize tree and delegate to primary engine
            byte[] treeBytes = normalizedTree.serialize();

            logger.info(
                "[DefaultPlanExecutor] Tree query: contextId={}, providers={}, collectorLeaves={}, treeBytes={}",
                contextId, collectorQueries.size(), normalizedTree.collectorLeafCount(), treeBytes.length
            );

            // Step 4: Consume the result stream
            List<Object[]> rows = new ArrayList<>();
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

            logger.info("[DefaultPlanExecutor] Tree query completed, {} rows", rows.size());
            return rows;

        } catch (Exception e) {
            throw new RuntimeException("Tree query execution failed: " + e.getMessage(), e);
        } finally {
            // Cleanup: unregister + close all contexts (always runs)
            try {
                if (contextId > 0) {
                    FilterTreeCallbackBridge.unregister(contextId);
                }
            } finally {
                for (IndexFilterTreeContext<?> treeCtx : createdContexts) {
                    try {
                        treeCtx.close();
                    } catch (IOException e) {
                        logger.warn("Failed to close tree context", e);
                    }
                }
            }
        }
    }
}
