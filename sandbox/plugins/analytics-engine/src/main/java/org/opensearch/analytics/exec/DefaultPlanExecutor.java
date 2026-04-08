/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.plan.DefaultQueryPlanner;
import org.opensearch.analytics.plan.FieldCapabilityResolver;
import org.opensearch.analytics.plan.QueryPlanningException;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.analytics.plan.registry.BackendCapabilityRegistry;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * {@link QueryPlanExecutor} default implementation.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsBackEndPlugin> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final DefaultQueryPlanner queryPlanner;

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
        this.indicesService = indicesService;
        this.clusterService = clusterService;

        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsBackEndPlugin plugin : plugins) {
            this.backEnds.put(plugin.name(), plugin);
        }

        // Build BackendCapabilityRegistry from plugins
        BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
        for (AnalyticsBackEndPlugin plugin : plugins) {
            Set<Class<? extends RelNode>> ops = plugin.supportedOperators();
            Set<String> fns = extractFunctionNames(plugin);
            registry.register(plugin.name(), ops, fns, plugin);
        }

        // Build cluster for HepPlanner (used by DefaultQueryPlanner internally)
        RexBuilder rexBuilder = new RexBuilder(
            new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        FieldCapabilityResolver fieldCapabilityResolver =
            new FieldCapabilityResolver(indicesService, clusterService);

        this.queryPlanner = new DefaultQueryPlanner(registry, cluster, fieldCapabilityResolver);
    }

    private static Set<String> extractFunctionNames(AnalyticsBackEndPlugin plugin) {
        if (plugin.operatorTable() == null) return Set.of();
        return plugin.operatorTable().getOperatorList().stream()
            .map(op -> op.getName().toUpperCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableSet());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        // Resolve shard count from index metadata
        String tableName = extractTableName(logicalFragment);
        IndexMetadata indexMetadata = clusterService.state().metadata().index(tableName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("Index [" + tableName + "] not found in cluster state");
        }
        int shardCount = indexMetadata.getNumberOfShards();

        // Planning — throws QueryPlanningException (propagates unwrapped per Req 6.2, 8.4)
        ResolvedPlan resolved = queryPlanner.plan(logicalFragment, shardCount);

        // Safety net (Req 6.5)
        if ("unresolved".equals(resolved.getBackendName())) {
            throw new IllegalStateException(
                "Planning did not resolve backend assignment for plan root");
        }

        AnalyticsBackEndPlugin plugin = backEnds.get(resolved.getBackendName());
        if (plugin == null) {
            throw new IllegalStateException(
                "No plugin registered for backend [" + resolved.getBackendName() + "]");
        }

        int fieldCount = resolved.getRoot().getRowType().getFieldCount();
        logger.info("[DefaultPlanExecutor] Final plan before execution (backend={}, fields={}):\n{}",
            plugin.name(), fieldCount, resolved.getRoot().explain());

        List<ShardId> shardIds = resolveShardIds(tableName);
        if (shardIds.isEmpty()) {
            throw new IllegalStateException("No shards found for index [" + tableName + "]");
        }
        IndexShard indexShard = getIndexShard(shardIds.get(0));
        if (indexShard == null) {
            throw new IllegalStateException("Shard [" + shardIds.get(0) + "] not found on this node");
        }

        CompositeEngine engine = (CompositeEngine) indexShard.getIndexer();

        // Bridge path
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> snapshot = engine.acquireSnapshot()) {
            EngineBridge<byte[], ? extends EngineResultStream, RelNode> bridge =
                (EngineBridge<byte[], ? extends EngineResultStream, RelNode>) plugin.bridge(engine, snapshot.getRef());

            byte[] converted = bridge.convertFragment(resolved.getRoot());

            // Check if indexed query path is enabled — route through Lucene+Parquet indexed table
            // TODO : wire DF + Lucene - this is just to validate that once wired , query in backend will work
            if (clusterService.getClusterSettings().get(SearchService.INDEXED_QUERY_ENABLED_SETTING)) {
                logger.info("[DefaultPlanExecutor] Indexed query enabled, routing through Lucene+Parquet indexed table");
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
                luceneReader.close();
                logger.info("[DefaultPlanExecutor] Indexed query completed, {} rows", rows.size());
                return rows;
            }
        } catch (Exception e) {
            throw new RuntimeException("Indexed query execution failed: " + e.getMessage(), e);
        }
    }

    /**
     * Extracts a Lucene query from the RelNode plan tree by finding Filter nodes
     * and converting simple equality/like predicates to TermQuery/WildcardQuery.
     * Falls back to MatchAllDocsQuery when no extractable filter is found.
     */
    private org.apache.lucene.search.Query buildLuceneQueryFromPlan(RelNode node) {
        // Walk the tree to find a Filter
        Filter filter = findFilter(node);
        if (filter != null) {
            RexNode condition = filter.getCondition();
            if (condition instanceof RexCall) {
                RexCall call = (RexCall) condition;
                org.apache.lucene.search.Query q = rexCallToLuceneQuery(call, filter.getInput().getRowType());
                if (q != null) {
                    logger.info("[DefaultPlanExecutor] Extracted Lucene query from plan filter: {}", q);
                    return q;
                }
            }
        }
        logger.info("[DefaultPlanExecutor] No filter in plan, using MatchAllDocsQuery");
        return new org.apache.lucene.search.MatchAllDocsQuery();
    }

    private Filter findFilter(RelNode node) {
        if (node instanceof Filter) {
            return (Filter) node;
        }
        for (RelNode input : node.getInputs()) {
            Filter f = findFilter(input);
            if (f != null) return f;
        }
        return null;
    }

    /**
     * Converts a RexCall to a Lucene query.
     * Supports: field = 'value' -> TermQuery
     * field != 'value' -> BooleanQuery(MatchAll, NOT TermQuery)
     * field like '%value%' -> WildcardQuery
     */
    private org.apache.lucene.search.Query rexCallToLuceneQuery(
        RexCall call,
        org.apache.calcite.rel.type.RelDataType rowType
    ) {
        SqlKind kind = call.getKind();
        List<RexNode> operands = call.getOperands();

        if ((kind == SqlKind.EQUALS || kind == SqlKind.NOT_EQUALS) && operands.size() == 2) {
            String fieldName = extractFieldName(operands.get(0), rowType);
            String value = extractLiteral(operands.get(1));
            if (fieldName == null) {
                fieldName = extractFieldName(operands.get(1), rowType);
                value = extractLiteral(operands.get(0));
            }
            // Only build Lucene term queries for string-typed fields (keyword/text).
            // Numeric fields aren't indexed as keyword terms in Lucene.
            if (fieldName != null && value != null && isStringField(operands, rowType)) {
                var termQuery = new org.apache.lucene.search.TermQuery(
                    new org.apache.lucene.index.Term(fieldName, value));
                if (kind == SqlKind.EQUALS) {
                    return termQuery;
                }
                var bq = new org.apache.lucene.search.BooleanQuery.Builder();
                bq.add(new org.apache.lucene.search.MatchAllDocsQuery(), org.apache.lucene.search.BooleanClause.Occur.MUST);
                bq.add(termQuery, org.apache.lucene.search.BooleanClause.Occur.MUST_NOT);
                return bq.build();
            }
        }

        if (kind == SqlKind.LIKE && operands.size() == 2) {
            String fieldName = extractFieldName(operands.get(0), rowType);
            String pattern = extractLiteral(operands.get(1));
            if (fieldName != null && pattern != null) {
                String lucenePattern = pattern.replace('%', '*').replace('_', '?');
                return new org.apache.lucene.search.WildcardQuery(
                    new org.apache.lucene.index.Term(fieldName, lucenePattern));
            }
        }

        if (kind == SqlKind.AND) {
            var bq = new org.apache.lucene.search.BooleanQuery.Builder();
            boolean hasClause = false;
            for (RexNode operand : operands) {
                if (operand instanceof RexCall) {
                    var sub = rexCallToLuceneQuery((RexCall) operand, rowType);
                    if (sub != null) {
                        bq.add(sub, org.apache.lucene.search.BooleanClause.Occur.MUST);
                        hasClause = true;
                    }
                }
            }
            return hasClause ? bq.build() : null;
        }

        return null;
    }

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

    private AnalyticsBackEndPlugin selectBackEnd() {
        if (backEnds.isEmpty()) {
            throw new IllegalStateException("No analytics back-end plugins registered");
        }
        return backEnds.values().iterator().next();
    }
}
