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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.ScalarFunction;
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

    private static final Set<ScalarFunction> STANDARD_OPS = Set.of(
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        ScalarFunction.IS_NULL,
        ScalarFunction.IS_NOT_NULL,
        ScalarFunction.IN,
        ScalarFunction.LIKE
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

    private static final Set<FieldType> STANDARD_TYPES = new HashSet<>();
    static {
        STANDARD_TYPES.addAll(FieldType.numeric());
        STANDARD_TYPES.addAll(FieldType.keyword());
        STANDARD_TYPES.addAll(FieldType.text());
        STANDARD_TYPES.addAll(FieldType.date());
        STANDARD_TYPES.add(FieldType.BOOLEAN);
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
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return QuerySerializerRegistry.getSerializers();
            }
        };
    }

    private static final Logger LOGGER = LogManager.getLogger(LuceneAnalyticsBackendPlugin.class);

    @Override
    public FilterDelegationHandle getFilterDelegationHandle(List<DelegatedExpression> expressions, CommonExecutionContext ctx) {
        ShardScanExecutionContext shardCtx = (ShardScanExecutionContext) ctx;
        DirectoryReader directoryReader = shardCtx.getReader().getReader(plugin.getDataFormat(), DirectoryReader.class);
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        QueryShardContext queryShardContext = buildMinimalQueryShardContext(shardCtx, searcher);
        BooleanSupplier isCancelled = () -> {
            Task task = shardCtx.getTask();
            return task instanceof CancellableTask ct && ct.isCancelled();
        };
        return new LuceneFilterDelegationHandle(
            expressions,
            queryShardContext,
            directoryReader,
            shardCtx.getNamedWriteableRegistry(),
            isCancelled
        );
    }

    private QueryShardContext buildMinimalQueryShardContext(ShardScanExecutionContext ctx, IndexSearcher searcher) {
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

}
