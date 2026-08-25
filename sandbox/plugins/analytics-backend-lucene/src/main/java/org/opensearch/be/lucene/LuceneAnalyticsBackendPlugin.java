/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.BackendShardPreference;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegatedSubtreeConvertor;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.DataFormatAwareEngine.DataFormatAwareReader;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineBackedIndexer;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Analytics SPI extension for the Lucene backend. Declares filter capabilities
 * for full-text and standard predicates, and provides {@link DelegatedPredicateSerializer}
 * implementations for serializing delegated queries into {@link QueryBuilder} bytes.
 *
 * <p>At the data node, the serialized bytes are deserialized back into a {@link QueryBuilder},
 * which uses the field name encoded within it to look up the appropriate
 * {@link org.opensearch.index.mapper.MappedFieldType} and create the Lucene query.
 *
 * @opensearch.internal
 */
public class LuceneAnalyticsBackendPlugin implements AnalyticsSearchBackendPlugin {

    private static final String LUCENE_FORMAT = LuceneDataFormat.LUCENE_FORMAT_NAME;
    private static final Set<String> LUCENE_FORMATS = Set.of(LUCENE_FORMAT);

    // Lucene's STANDARD filter capabilities must stay in lockstep with the serializers
    // registered in QuerySerializerRegistry — declaring a capability without a matching
    // DelegatedPredicateSerializer makes the marking layer pick Lucene as viable for
    // operators it can't actually translate, and the failure surfaces at convert time as
    // an IllegalStateException ("No Lucene serializer for [..]"). Today only EQUALS has
    // a serializer; range ops, NOT_EQUALS, IS_NULL, IS_NOT_NULL, IN, LIKE are deferred
    // until their serializers land.
    // TODO: have CapabilityRegistry intersect declared FilterCapability against the
    // backend's serializer keyset at startup so this list can't drift again. The TODO in
    // OpenSearchFilterRule.resolveViableBackends references the same constraint.
    private static final Set<ScalarFunction> STANDARD_OPS = Set.of(
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.IS_NULL,
        ScalarFunction.IS_NOT_NULL,
        ScalarFunction.LIKE,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        ScalarFunction.SARG_PREDICATE
    );

    private static final Set<ScalarFunction> FULL_TEXT_OPS = Set.of(
        ScalarFunction.MATCH,
        ScalarFunction.MATCH_PHRASE,
        ScalarFunction.MATCH_BOOL_PREFIX,
        ScalarFunction.MATCH_PHRASE_PREFIX,
        ScalarFunction.MULTI_MATCH,
        ScalarFunction.QUERY_STRING,
        ScalarFunction.SIMPLE_QUERY_STRING,
        ScalarFunction.FUZZY,
        ScalarFunction.WILDCARD,
        ScalarFunction.REGEXP,
        ScalarFunction.WILDCARD_QUERY,
        ScalarFunction.QUERY,
        ScalarFunction.MATCHALL
    );

    // Field types Lucene's secondary data format actually indexes (see LuceneFieldFactoryRegistry).
    // Numeric/date/boolean fields are not indexed under composite-parquet primary, so listing them
    // would cause peer consultation to return null scorers and zero-out candidate sets.
    // TODO: derive this list from LuceneFieldFactoryRegistry instead of hardcoding.
    private static final Set<FieldType> STANDARD_TYPES = new HashSet<>();
    static {
        STANDARD_TYPES.add(FieldType.KEYWORD);
        STANDARD_TYPES.add(FieldType.TEXT);
        STANDARD_TYPES.add(FieldType.MATCH_ONLY_TEXT);
    }

    private static final Set<FieldType> FULL_TEXT_TYPES = new HashSet<>();
    static {
        FULL_TEXT_TYPES.addAll(FieldType.keyword());
        FULL_TEXT_TYPES.addAll(FieldType.text());
    }

    private static final Set<FieldType> KEYWORD_ONLY = Set.of(FieldType.KEYWORD);

    /** Field types whose values the Arrow source reader can expose without type coercion. */
    private static final Set<FieldType> DOC_VALUES_TYPES = Set.of(FieldType.LONG, FieldType.DATE, FieldType.KEYWORD);

    private static final Set<FilterCapability> FILTER_CAPS;
    static {
        Set<FilterCapability> caps = new HashSet<>();
        for (ScalarFunction op : STANDARD_OPS) {
            if (op == ScalarFunction.LIKE) {
                caps.add(new FilterCapability.Standard(op, KEYWORD_ONLY, LUCENE_FORMATS));
            } else {
                caps.add(new FilterCapability.Standard(op, STANDARD_TYPES, LUCENE_FORMATS));
                caps.add(new FilterCapability.Standard(op, DOC_VALUES_TYPES, LUCENE_FORMATS));
            }
        }
        for (ScalarFunction op : FULL_TEXT_OPS) {
            for (FieldType type : FULL_TEXT_TYPES) {
                caps.add(new FilterCapability.FullText(op, type, LUCENE_FORMATS, Set.of()));
            }
        }
        FILTER_CAPS = caps;
    }

    private static final Set<FieldType> NUMERIC_DOC_VALUES_TYPES = Set.of(FieldType.LONG);

    /** Scalar expressions evaluated by DataFusion after Lucene supplies doc-values batches. */
    private static final Set<ProjectCapability> PROJECT_CAPS;
    static {
        Set<FieldType> returnTypes = new HashSet<>(DOC_VALUES_TYPES);
        returnTypes.add(FieldType.DOUBLE);
        returnTypes.add(FieldType.FLOAT);
        returnTypes.add(FieldType.BOOLEAN);
        Set<ProjectCapability> capabilities = new HashSet<>();
        for (ScalarFunction function : List.of(
            ScalarFunction.PLUS,
            ScalarFunction.MINUS,
            ScalarFunction.TIMES,
            ScalarFunction.DIVIDE,
            ScalarFunction.MOD,
            ScalarFunction.CAST,
            ScalarFunction.EXTRACT,
            ScalarFunction.DATE_FORMAT,
            ScalarFunction.REGEXP_REPLACE,
            ScalarFunction.CASE,
            ScalarFunction.AND,
            ScalarFunction.OR,
            ScalarFunction.NOT,
            ScalarFunction.EQUALS,
            ScalarFunction.NOT_EQUALS,
            ScalarFunction.GREATER_THAN,
            ScalarFunction.GREATER_THAN_OR_EQUAL,
            ScalarFunction.LESS_THAN,
            ScalarFunction.LESS_THAN_OR_EQUAL
        )) {
            capabilities.add(new ProjectCapability.Scalar(function, returnTypes, LUCENE_FORMATS, true));
        }
        capabilities.add(new ProjectCapability.Scalar(ScalarFunction.CHAR_LENGTH, Set.of(FieldType.LONG), LUCENE_FORMATS, true));
        PROJECT_CAPS = Set.copyOf(capabilities);
    }

    /**
     * Lucene-secondary indexes the term dictionary (inverted index) for the same field
     * types it accepts filters on — keyword / text / match_only_text. The Index
     * scan capability lets the planner mark Lucene viable as a driver for metadata-only
     * operations over scans whose fields are listed here. The separate DocValues capability
     * covers supported value-producing plans; shape validation rejects unsupported referenced
     * columns before selection.
     */
    private static final Set<ScanCapability> SCAN_CAPS = Set.of(
        new ScanCapability.Index(LUCENE_FORMATS, STANDARD_TYPES),
        new ScanCapability.DocValues(LUCENE_FORMATS, DOC_VALUES_TYPES)
    );

    /** Aggregate shapes supported by either the count fast path or the Arrow source plan. */
    private static final Set<AggregateCapability> AGGREGATE_CAPS;
    static {
        Set<AggregateCapability> capabilities = new HashSet<>();
        capabilities.add(AggregateCapability.simple(AggregateFunction.COUNT, STANDARD_TYPES, LUCENE_FORMATS));
        for (AggregateFunction function : List.of(AggregateFunction.SUM, AggregateFunction.SUM0, AggregateFunction.AVG)) {
            capabilities.add(AggregateCapability.simple(function, NUMERIC_DOC_VALUES_TYPES, LUCENE_FORMATS));
        }
        for (AggregateFunction function : List.of(AggregateFunction.COUNT, AggregateFunction.MIN, AggregateFunction.MAX)) {
            capabilities.add(AggregateCapability.simple(function, DOC_VALUES_TYPES, LUCENE_FORMATS));
        }
        AGGREGATE_CAPS = Set.copyOf(capabilities);
    }

    private final LucenePlugin plugin;
    private volatile AnalyticsSearchBackendPlugin arrowBatchSourceBackend;

    public LuceneAnalyticsBackendPlugin(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return LuceneDataFormat.LUCENE_FORMAT_NAME;
    }

    @Override
    public void bindBackends(Map<String, AnalyticsSearchBackendPlugin> backends) {
        arrowBatchSourceBackend = backends.values()
            .stream()
            .filter(AnalyticsSearchBackendPlugin::supportsArrowBatchSourceExecution)
            .findFirst()
            .orElse(null);
    }

    @Override
    public BackendCapabilityProvider getCapabilityProvider() {
        return new BackendCapabilityProvider() {
            @Override
            public Set<EngineCapability> supportedEngineCapabilities() {
                return Set.of();
            }

            @Override
            public Set<FilterCapability> filterCapabilities() {
                return FILTER_CAPS;
            }

            @Override
            public Set<ScanCapability> scanCapabilities() {
                return SCAN_CAPS;
            }

            @Override
            public Set<AggregateCapability> aggregateCapabilities() {
                return AGGREGATE_CAPS;
            }

            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return PROJECT_CAPS;
            }

            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return QuerySerializerRegistry.getSerializers();
            }

            @Override
            public BackendShardPreference shardPreference() {
                return new LuceneShardPreference(() -> arrowBatchSourceBackend != null);
            }
        };
    }

    private static final Logger LOGGER = LogManager.getLogger(LuceneAnalyticsBackendPlugin.class);

    /**
     * Standard Lucene shards expose an {@link Engine.Searcher}, not the pluggable-format reader
     * implemented by composite engines. Adapt that searcher to the shared reader contract so the
     * same Lucene execution code can consume both index types.
     */
    @Override
    public GatedCloseable<IndexReaderProvider.Reader> acquireReader(IndexShard shard) throws IOException {
        IndexReaderProvider readerProvider = shard.getReaderProvider();
        if (!(readerProvider instanceof EngineBackedIndexer indexer)) {
            return readerProvider.acquireReader();
        }

        Engine.Searcher searcher = shard.acquireSearcher("analytics-lucene");
        GatedCloseable<CatalogSnapshot> snapshotRef;
        try {
            snapshotRef = indexer.acquireSnapshot();
        } catch (RuntimeException | Error e) {
            searcher.close();
            throw e;
        }
        try {
            DataFormatAwareReader reader = new DataFormatAwareReader(
                snapshotRef,
                Map.of(plugin.getDataFormat(), new LuceneReader(searcher.getDirectoryReader(), Map.of()))
            );
            return new GatedCloseable<>(reader, () -> IOUtils.close(searcher, reader));
        } catch (RuntimeException | Error e) {
            IOUtils.closeWhileHandlingException(searcher, snapshotRef);
            throw e;
        }
    }

    @Override
    public FilterDelegationHandle getFilterDelegationHandle(List<DelegatedExpression> expressions, CommonExecutionContext ctx) {
        ShardScanExecutionContext shardCtx = (ShardScanExecutionContext) ctx;
        IndexReaderProvider.Reader reader = shardCtx.getReader();
        LuceneReader luceneReader = reader.getReader(plugin.getDataFormat(), LuceneReader.class);
        // Shared per-reader searcher (see LuceneReader#searcher) — a fresh one here crashes the node
        // on self-union delegated scans.
        IndexSearcher searcher = luceneReader.searcher(shardCtx.getQueryCache(), shardCtx.getQueryCachingPolicy());
        QueryShardContext queryShardContext = buildMinimalQueryShardContext(shardCtx, searcher);
        BooleanSupplier isCancelled = () -> {
            Task task = shardCtx.getTask();
            return task instanceof CancellableTask ct && ct.isCancelled();
        };
        return new LuceneFilterDelegationHandle(
            expressions,
            queryShardContext,
            luceneReader,
            reader.catalogSnapshot(),
            shardCtx.getNamedWriteableRegistry(),
            isCancelled
        );
    }

    // ── Lucene-as-driver execution path (count fast path) ──

    @Override
    public FragmentConvertor getFragmentConvertor() {
        return new LuceneFragmentConvertor(QuerySerializerRegistry.getSerializers(), arrowBatchSourceBackend);
    }

    @Override
    public FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
        return new LuceneInstructionHandlerFactory(plugin);
    }

    @Override
    public SearchExecEngineProvider getSearchExecEngineProvider() {
        return (ctx, backendContext) -> {
            if (!(backendContext instanceof LuceneSearcherState state)) {
                throw new IllegalStateException(
                    "Lucene SearchExecEngineProvider expected LuceneSearcherState but got "
                        + (backendContext == null ? "null" : backendContext.getClass().getName())
                );
            }
            LuceneSearchExecEngine engine = new LuceneSearchExecEngine(state, arrowBatchSourceBackend);
            engine.prepare(ctx);
            return engine;
        };
    }

    /** Package-private — also reused by {@link LuceneScanInstructionHandler} in driver mode. */
    static QueryShardContext buildMinimalQueryShardContext(ShardScanExecutionContext ctx, IndexSearcher searcher) {
        return new QueryShardContext(
            0,
            ctx.getIndexSettings(),
            null,  // bigArrays
            null,  // bitsetFilterCache
            null,  // indexFieldDataLookup
            ctx.getMapperService(),
            null,  // similarityService
            null,  // scriptService
            null,  // xContentRegistry
            null,  // namedWriteableRegistry
            null,  // client
            searcher,
            System::currentTimeMillis,
            null,  // clusterAlias
            s -> true,  // indexNameMatcher
            () -> true,  // allowExpensiveQueries
            null   // valuesSourceRegistry
        );
    }

    // ---- Serializers ----

    @Override
    public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
        return QuerySerializerRegistry.getSerializers();
    }

    @Override
    public DelegatedSubtreeConvertor getDelegatedSubtreeConvertor() {
        return new LuceneSubtreeConvertor(QuerySerializerRegistry.getSerializers());
    }
}
