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
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

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
    // an IllegalStateException ("No Lucene serializer for [..]"). EQUALS and LIKE have
    // serializers today; range ops, NOT_EQUALS, IS_NULL, IS_NOT_NULL, IN are deferred until
    // their serializers land.
    //
    // LIKE is a performance pre-filter (DataFusion re-verifies), so the Lucene query only needs to be a
    // superset. On KEYWORD it runs against the single raw term (correct). On analyzed TEXT a wildcard
    // matches per-token and can fail in the scan path, so text LIKE is delegated only when the field has
    // a keyword exact-match subfield to route to (LikeSerializer targets field.<keyword>); the marking
    // layer (OpenSearchFilterRule) drops delegation for text LIKE without one.
    // TODO: have CapabilityRegistry intersect declared FilterCapability against the
    // backend's serializer keyset at startup so this list can't drift again. The TODO in
    // OpenSearchFilterRule.resolveViableBackends references the same constraint.
    private static final Set<ScalarFunction> STANDARD_OPS = Set.of(ScalarFunction.EQUALS, ScalarFunction.LIKE);

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

    private static final Set<FilterCapability> FILTER_CAPS;
    static {
        Set<FilterCapability> caps = new HashSet<>();
        for (ScalarFunction op : STANDARD_OPS) {
            caps.add(new FilterCapability.Standard(op, STANDARD_TYPES, LUCENE_FORMATS));
        }
        for (ScalarFunction op : FULL_TEXT_OPS) {
            for (FieldType type : FULL_TEXT_TYPES) {
                caps.add(new FilterCapability.FullText(op, type, LUCENE_FORMATS, Set.of()));
            }
        }
        FILTER_CAPS = caps;
    }

    /**
     * Lucene-secondary indexes the term dictionary (inverted index) for the same field
     * types it accepts filters on — keyword / text / match_only_text. The Index
     * scan capability lets the planner mark Lucene viable as a driver for metadata-only
     * operations (count today, group-by-count and top-K terms in future) over scans whose
     * fields are listed here. It does NOT imply Lucene can deliver row values; consumers
     * needing values (Project, Sort) consult value-producing scan capabilities separately
     * and self-restrict, which the chain-agreement filter at PlanForker enforces.
     */
    private static final Set<ScanCapability> SCAN_CAPS = Set.of(new ScanCapability.Index(LUCENE_FORMATS, STANDARD_TYPES));

    /**
     * Lucene drives count(*) and (in a follow-up) count(col) over fields it indexes.
     * Coupled with the Index scan capability above, this lets PlanForker emit a
     * Lucene-driver StagePlan alternative for count-shaped fragments without bypassing
     * the existing engine path.
     */
    private static final Set<AggregateCapability> AGGREGATE_CAPS = Set.of(
        AggregateCapability.simple(AggregateFunction.COUNT, STANDARD_TYPES, LUCENE_FORMATS)
    );

    private final LucenePlugin plugin;

    public LuceneAnalyticsBackendPlugin(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return LuceneDataFormat.LUCENE_FORMAT_NAME;
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
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return QuerySerializerRegistry.getSerializers();
            }

            @Override
            public BackendShardPreference shardPreference() {
                return SHARD_PREFERENCE;
            }
        };
    }

    private static final BackendShardPreference SHARD_PREFERENCE = new LuceneShardPreference();

    private static final Logger LOGGER = LogManager.getLogger(LuceneAnalyticsBackendPlugin.class);

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
        return new LuceneFragmentConvertor(QuerySerializerRegistry.getSerializers());
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
            LuceneSearchExecEngine engine = new LuceneSearchExecEngine(state);
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
