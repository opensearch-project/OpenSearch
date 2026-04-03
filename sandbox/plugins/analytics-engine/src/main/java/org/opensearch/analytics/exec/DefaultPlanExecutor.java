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
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
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
        AnalyticsSearchBackendPlugin plugin = selectBackEnd();
        if (plugin == null) {
            return new ArrayList<>();
        }

        IndexShard shard = resolveShard(tableName);
        DataFormatAwareEngine dataFormatAwareEngine = shard.getCompositeEngine();
        if (dataFormatAwareEngine == null) {
            throw new IllegalStateException("No CompositeEngine on shard [" + shard.shardId() + "]");
        }

        SearchShardTask task = null; // TODO : init task
        List<Object[]> rows = new ArrayList<>();
        try (var dataFormatAwareReader = dataFormatAwareEngine.acquireReader()) {
            ExecutionContext ctx = new ExecutionContext(tableName, task, dataFormatAwareReader.get());
            try (SearchExecEngine<ExecutionContext, EngineResultStream> engine = plugin.searcher(ctx)) {
                logger.info("[DefaultPlanExecutor] Executing via [{}]", plugin.name());
                try (EngineResultStream resultStream = engine.execute(ctx)) {
                    Iterator<EngineResultBatch> batchIterator = resultStream.iterator();
        CompositeEngine engine = (CompositeEngine) indexShard.getIndexer();

        // Bridge path
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> snapshot = engine.acquireSnapshot()) {
            EngineBridge<byte[], ? extends EngineResultStream, RelNode> bridge =
                (EngineBridge<byte[], ? extends EngineResultStream, RelNode>) plugin.bridge(engine, snapshot.getRef());

            byte[] converted = bridge.convertFragment(resolved.getRoot());

            // Check if the query needs tree-based execution (FTS + columnar predicates).
            // The planner determines this based on whether the query has filter predicates
            // that require collector-based evaluation (e.g. Lucene FTS).
            // TODO: Replace setting-based check with planner-driven IndexFilterTree construction.
            //       The planner should produce an IndexFilterTree when the query has FTS predicates,
            //       and the tree query path should be used automatically.
            if (clusterService.getClusterSettings().get(SearchService.INDEXED_QUERY_ENABLED_SETTING)) {
                logger.info("[DefaultPlanExecutor] Indexed query enabled, routing through tree query path");

                // Build IndexFilterTree from the logical plan's filter predicates
                org.opensearch.index.engine.IndexFilterTree filterTree = buildFilterTree(logicalFragment);

                if (filterTree != null) {
                    return executeWithFilterTree(engine, filterTree, tableName, converted, logicalFragment, bridge);
                }
                // Fall through to single-query indexed path if no tree could be built
                return executeViaIndexedQuery(engine, tableName, converted, logicalFragment);
            }

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
        } catch (QueryPlanningException e) {
            throw e; // propagate unwrapped per Req 8.4
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
        if (indexService == null) return null;
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
    private List<Object[]> executeViaIndexedQuery(
        CompositeEngine compositeEngine,
        String tableName,
        byte[] substraitBytes,
        RelNode logicalFragment
    ) {
        try {
            org.apache.lucene.index.IndexWriter luceneWriter = compositeEngine.getLuceneIndexWriter();
            if (luceneWriter == null) {
                throw new IllegalStateException("Indexed query requires a Lucene IndexWriter");
            }
            org.apache.lucene.index.DirectoryReader luceneReader =
                org.apache.lucene.index.DirectoryReader.open(luceneWriter);

            int partitions = clusterService.getClusterSettings().get(SearchService.INDEXED_QUERY_PARTITIONS_SETTING);
            int bitsetMode = clusterService.getClusterSettings().get(SearchService.INDEXED_QUERY_BITSET_MODE_SETTING);

            // Extract Lucene query from the logical plan's filter predicates.
            // e.g. `where URL = 'google'` -> TermQuery(URL, google)
            //       `where URL like '%google%'` -> WildcardQuery(URL, *google*)
            // Falls back to MatchAllDocsQuery if no filter is present.
            org.apache.lucene.search.Query luceneQuery = buildLuceneQueryFromPlan(logicalFragment);

            logger.info("[DefaultPlanExecutor] Indexed query: luceneQuery={}, segments={}",
                luceneQuery, luceneReader.leaves().size());

            @SuppressWarnings("rawtypes")
            SearchExecEngine searchExecEngine = compositeEngine.getPrimaryReadEngine();

            CompletableFuture<Long> streamFuture = new CompletableFuture<>();
            searchExecEngine.executeIndexedQuery(
                luceneReader, luceneQuery, tableName, substraitBytes,
                partitions, bitsetMode, true,
                new ActionListener<Long>() {
                    @Override
                    public void onResponse(Long streamPtr) {
                        streamFuture.complete(streamPtr);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        streamFuture.completeExceptionally(e);
                    }
                }
            );
            long streamPtr = streamFuture.join();

            // Consume the stream using the bridge's consumeStream (wraps the native pointer)
            AnalyticsBackEndPlugin bePlugin = selectBackEnd();
            try (CompositeEngine.ReleasableRef<CatalogSnapshot> snapshot = compositeEngine.acquireSnapshot()) {
                EngineBridge<byte[], ? extends EngineResultStream, RelNode> bridge =
                    (EngineBridge<byte[], ? extends EngineResultStream, RelNode>) bePlugin.bridge(compositeEngine, snapshot.getRef());
                List<Object[]> rows = new ArrayList<>();
                try (EngineResultStream resultStream = bridge.consumeStream(streamPtr)) {
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
    private boolean isStringField(List<RexNode> operands, org.apache.calcite.rel.type.RelDataType rowType) {
        for (RexNode op : operands) {
            if (op instanceof RexInputRef) {
                var sqlType = rowType.getFieldList().get(((RexInputRef) op).getIndex()).getType().getSqlTypeName();
                return sqlType == org.apache.calcite.sql.type.SqlTypeName.VARCHAR
                    || sqlType == org.apache.calcite.sql.type.SqlTypeName.CHAR;
            }
        }
        return false;
    }

    private String extractFieldName(RexNode node, org.apache.calcite.rel.type.RelDataType rowType) {
        if (node instanceof RexInputRef) {
            int idx = ((RexInputRef) node).getIndex();
            return rowType.getFieldList().get(idx).getName();
        }
        return null;
    }

    private String extractLiteral(RexNode node) {
        if (node instanceof RexLiteral) {
            RexLiteral lit = (RexLiteral) node;
            Comparable<?> val = lit.getValue();
            if (val == null) return null;
            // NlsString (charset-prefixed string) — extract the raw value
            String className = val.getClass().getSimpleName();
            if ("NlsString".equals(className)) {
                try {
                    return (String) val.getClass().getMethod("getValue").invoke(val);
                } catch (Exception e) {
                    return val.toString();
                }
            }
            return val.toString();
        }
        return null;
    }

    // ── Tree Query Execution ────────────────────────────────────────────

    /**
     * Builds an {@link org.opensearch.index.engine.IndexFilterTree} from the logical plan's
     * filter predicates. Returns null if no tree can be built (e.g. no FTS predicates).
     * <p>
     * TODO: This is a placeholder. The real implementation should be driven by the query planner
     * which decomposes the RelNode filter into CollectorLeaf (FTS) and PredicateLeaf (columnar)
     * nodes based on which predicates each backend can handle.
     */
    private org.opensearch.index.engine.IndexFilterTree buildFilterTree(RelNode logicalFragment) {
        // Walk the plan to find Filter nodes with FTS-eligible predicates
        Filter filter = findFilter(logicalFragment);
        if (filter == null) {
            return null;
        }

        RexNode condition = filter.getCondition();
        if (condition instanceof RexCall == false) {
            return null;
        }

        // For now, build a simple tree: one CollectorLeaf per FTS predicate,
        // one PredicateLeaf per numeric/columnar predicate.
        // TODO: The planner should do this decomposition properly.
        return buildFilterTreeFromRex((RexCall) condition, filter.getInput().getRowType());
    }

    /**
     * Recursively converts a RexCall into an IndexFilterTree.
     * Returns null if the expression cannot be decomposed into a tree.
     */
    private org.opensearch.index.engine.IndexFilterTree buildFilterTreeFromRex(
        RexCall call,
        org.apache.calcite.rel.type.RelDataType rowType
    ) {
        // TODO: Implement proper decomposition. For now, delegate to the existing
        // single-query path by returning null (falls through to executeViaIndexedQuery).
        return null;
    }

    /**
     * Executes a query using the boolean filter tree path.
     * <p>
     * Orchestrates: normalize tree → create provider contexts → register with bridge →
     * serialize → delegate to primary engine → consume stream → cleanup.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private List<Object[]> executeWithFilterTree(
        CompositeEngine compositeEngine,
        org.opensearch.index.engine.IndexFilterTree tree,
        String tableName,
        byte[] substraitBytes,
        RelNode logicalFragment,
        EngineBridge<byte[], ? extends EngineResultStream, RelNode> bridge
    ) {
        long contextId = 0;
        List<org.opensearch.index.engine.exec.IndexFilterTreeContext<?>> createdContexts = new ArrayList<>();

        try {
            // Step 1: Normalize the tree (De Morgan's NOT push-down)
            org.opensearch.index.engine.IndexFilterTree normalizedTree = tree.normalize();

            // Step 2: Get collector tree providers from CompositeEngine (no plugin imports)
            Map<Integer, org.opensearch.index.engine.exec.IndexFilterTreeProvider<?, ?, ?>> providers =
                compositeEngine.getCollectorTreeProviders();
            if (providers.isEmpty()) {
                throw new IllegalStateException("No collector tree providers registered");
            }

            // Step 3: Open Lucene reader for collector leaves
            org.apache.lucene.index.IndexWriter luceneWriter = compositeEngine.getLuceneIndexWriter();
            if (luceneWriter == null) {
                throw new IllegalStateException("Tree query requires a Lucene IndexWriter");
            }
            org.apache.lucene.index.DirectoryReader luceneReader =
                org.apache.lucene.index.DirectoryReader.open(luceneWriter);

            // Step 4: Build queries for each CollectorLeaf
            // TODO: The planner should provide these queries alongside the tree.
            org.apache.lucene.search.Query[] luceneQueries = buildLuceneQueriesForTree(
                normalizedTree, logicalFragment
            );

            // Step 5: Create tree contexts and register with FilterTreeCallbackBridge
            contextId = org.opensearch.index.engine.exec.FilterTreeCallbackBridge.createContext();

            for (Map.Entry<Integer, org.opensearch.index.engine.exec.IndexFilterTreeProvider<?, ?, ?>> entry : providers.entrySet()) {
                int providerId = entry.getKey();
                org.opensearch.index.engine.exec.IndexFilterTreeProvider provider = entry.getValue();

                // Create tree context — the provider knows its own query/reader types
                org.opensearch.index.engine.exec.IndexFilterTreeContext<?> treeContext =
                    provider.createTreeContext(luceneQueries, luceneReader, normalizedTree);
                createdContexts.add(treeContext);

                org.opensearch.index.engine.exec.FilterTreeCallbackBridge.registerProvider(
                    contextId, providerId, provider, treeContext
                );
            }

            // Step 6: Serialize tree and delegate to primary engine
            byte[] treeBytes = normalizedTree.serialize();

            logger.info("[DefaultPlanExecutor] Tree query: contextId={}, providers={}, collectorLeaves={}, treeBytes={}",
                contextId, providers.size(), normalizedTree.collectorLeafCount(), treeBytes.length);

            // Get the exec.SearchExecEngine (e.g. DatafusionSearchExecEngine) for tree query delegation.
            // This is the interface in org.opensearch.index.engine.exec, NOT the abstract class.
            org.opensearch.index.engine.exec.SearchExecEngine<?, ?> treeEngine =
                compositeEngine.getSearchExecBackendEngine(
                    org.opensearch.plugins.spi.vectorized.DataFormat.PARQUET);
            if (treeEngine == null) {
                throw new IllegalStateException("No search exec backend engine for PARQUET format");
            }

            CompletableFuture<Long> streamFuture = new CompletableFuture<>();
            final long finalContextId = contextId;

            treeEngine.executeTreeQuery(treeBytes, finalContextId, null,
                new ActionListener<Long>() {
                    @Override
                    public void onResponse(Long streamPtr) {
                        streamFuture.complete(streamPtr);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        streamFuture.completeExceptionally(e);
                    }
                }
            );

            long streamPtr = streamFuture.join();

            // Step 7: Consume the stream
            List<Object[]> rows = new ArrayList<>();
            try (EngineResultStream resultStream = bridge.consumeStream(streamPtr)) {
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

            luceneReader.close();
            logger.info("[DefaultPlanExecutor] Tree query completed, {} rows", rows.size());
            return rows;

        } catch (Exception e) {
            throw new RuntimeException("Tree query execution failed: " + e.getMessage(), e);
        } finally {
            // Cleanup: unregister + close all contexts
            try {
                if (contextId > 0) {
                    org.opensearch.index.engine.exec.FilterTreeCallbackBridge.unregister(contextId);
                }
            } finally {
                for (org.opensearch.index.engine.exec.IndexFilterTreeContext<?> ctx : createdContexts) {
                    try {
                        ctx.close();
                    } catch (java.io.IOException e) {
                        logger.warn("Failed to close tree context", e);
                    }
                }
            }
        }
    }

    /**
     * Builds Lucene queries for each CollectorLeaf in the tree.
     * TODO: The planner should provide these alongside the tree.
     */
    private org.apache.lucene.search.Query[] buildLuceneQueriesForTree(
        org.opensearch.index.engine.IndexFilterTree tree,
        RelNode logicalFragment
    ) {
        // Placeholder: create one MatchAllDocsQuery per collector leaf
        // The real implementation should extract per-leaf queries from the planner output
        org.apache.lucene.search.Query[] queries = new org.apache.lucene.search.Query[tree.collectorLeafCount()];
        for (int i = 0; i < queries.length; i++) {
            queries[i] = new org.apache.lucene.search.MatchAllDocsQuery();
        }
        return queries;
    }

    private AnalyticsBackEndPlugin selectBackEnd() {
        if (backEnds.isEmpty()) {
            logger.warn("No back-end plugins registered — queries will return empty results");
            return null;
        }
        // TODO : This is placeholder - select based on data format
        return backEnds.values().iterator().next();
    }
}
