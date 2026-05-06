/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
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
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        ScalarFunction.FUZZY,
        ScalarFunction.WILDCARD,
        ScalarFunction.REGEXP
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

    // TODO: add serializers for MATCH_PHRASE, FUZZY, WILDCARD, REGEXP, and standard ops (for performance delegation)
    private static final Map<ScalarFunction, DelegatedPredicateSerializer> SERIALIZERS = Map.of(
        ScalarFunction.MATCH,
        LuceneAnalyticsBackendPlugin::serializeMatch
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
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return SERIALIZERS;
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
        return new LuceneFilterDelegationHandle(expressions, queryShardContext, directoryReader);
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

    private static byte[] serializeMatch(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = extractFieldName(call, 0, fieldStorage);
        String queryText = extractStringLiteral(call, 1);
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(fieldName, queryText);
        byte[] bytes = serializeQueryBuilder(queryBuilder);
        LOGGER.debug(
            "Serialized MATCH delegation: field=[{}], query=[{}], QueryBuilder=[{}], bytes={}",
            fieldName,
            queryText,
            queryBuilder,
            bytes.length
        );
        return bytes;
    }

    // ---- Helpers ----

    private static String extractFieldName(RexCall call, int operandIndex, List<FieldStorageInfo> fieldStorage) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (!(operand instanceof RexInputRef inputRef)) {
            throw new IllegalArgumentException(
                "Expected RexInputRef at operand " + operandIndex + ", got: " + operand.getClass().getSimpleName()
            );
        }
        return FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
    }

    private static String extractStringLiteral(RexCall call, int operandIndex) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (!(operand instanceof RexLiteral literal)) {
            throw new IllegalArgumentException(
                "Expected RexLiteral at operand " + operandIndex + ", got: " + operand.getClass().getSimpleName()
            );
        }
        return literal.getValueAs(String.class);
    }

    private static byte[] serializeQueryBuilder(QueryBuilder queryBuilder) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(queryBuilder);
            return BytesReference.toBytes(output.bytes());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to serialize delegated query: " + queryBuilder, exception);
        }
    }
}
