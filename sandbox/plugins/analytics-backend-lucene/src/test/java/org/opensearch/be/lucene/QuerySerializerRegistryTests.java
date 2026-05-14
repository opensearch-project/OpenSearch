/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link QuerySerializerRegistry} serializers — optional parameter round-trips
 * and SQL text relevance serializer behavior (WILDCARD_QUERY, QUERY, MATCHALL).
 */
public class QuerySerializerRegistryTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchQueryBuilder.NAME, MatchQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchPhraseQueryBuilder.NAME, MatchPhraseQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchBoolPrefixQueryBuilder.NAME, MatchBoolPrefixQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchPhrasePrefixQueryBuilder.NAME, MatchPhrasePrefixQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MultiMatchQueryBuilder.NAME, MultiMatchQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, QueryStringQueryBuilder.NAME, QueryStringQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, SimpleQueryStringBuilder.NAME, SimpleQueryStringBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, WildcardQueryBuilder.NAME, WildcardQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new)
        )
    );

    private static final SqlFunction MATCHALL_FUNCTION = new SqlFunction(
        "MATCHALL",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction WILDCARD_QUERY_FUNCTION = new SqlFunction(
        "WILDCARD_QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction QUERY_FUNCTION = new SqlFunction(
        "QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private Map<ScalarFunction, DelegatedPredicateSerializer> serializers;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        serializers = QuerySerializerRegistry.getSerializers();
    }

    /**
     * Tests that serializeMatch with operator=AND produces a MatchQueryBuilder with AND operator.
     * Validates Requirement 11.2.
     */
    public void testSerializeMatchWithOperatorAnd() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH);
        assertNotNull("MATCH serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("title", "hello world", "MATCH", Map.of("operator", "AND"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchQueryBuilder);
            MatchQueryBuilder matchQuery = (MatchQueryBuilder) deserialized;
            assertEquals("title", matchQuery.fieldName());
            assertEquals("hello world", matchQuery.value());
            assertEquals(Operator.AND, matchQuery.operator());
        }
    }

    /**
     * Tests that serializeMatchPhrase with slop=2 produces a MatchPhraseQueryBuilder with slop=2.
     * Validates Requirement 11.3.
     */
    public void testSerializeMatchPhraseWithSlop() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH_PHRASE);
        assertNotNull("MATCH_PHRASE serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("body", "quick fox", "MATCH_PHRASE", Map.of("slop", "2"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("body", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchPhraseQueryBuilder);
            MatchPhraseQueryBuilder matchPhrase = (MatchPhraseQueryBuilder) deserialized;
            assertEquals("body", matchPhrase.fieldName());
            assertEquals("quick fox", matchPhrase.value());
            assertEquals(2, matchPhrase.slop());
        }
    }

    /**
     * Tests that serializeMultiMatch with type=phrase produces a MultiMatchQueryBuilder with PHRASE type.
     * Validates Requirement 11.6.
     */
    public void testSerializeMultiMatchWithTypePhrase() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MULTI_MATCH);
        assertNotNull("MULTI_MATCH serializer must be registered", serializer);

        RexCall call = buildMultiFieldRexCallWithParams(List.of("title", "body"), "search text", "MULTI_MATCH", Map.of("type", "phrase"));
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MultiMatchQueryBuilder);
            MultiMatchQueryBuilder multiMatch = (MultiMatchQueryBuilder) deserialized;
            assertEquals("search text", multiMatch.value());
            assertEquals(MultiMatchQueryBuilder.Type.PHRASE, multiMatch.getType());
        }
    }

    /**
     * Tests that unrecognized parameter keys are silently ignored (no exception thrown).
     * Validates Requirement 11.10.
     */
    public void testUnrecognizedParameterKeysAreIgnored() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH);
        assertNotNull("MATCH serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams(
            "title",
            "hello",
            "MATCH",
            Map.of("unknown_param", "some_value", "another_unknown", "42")
        );
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        // Should not throw any exception
        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchQueryBuilder);
            MatchQueryBuilder matchQuery = (MatchQueryBuilder) deserialized;
            assertEquals("title", matchQuery.fieldName());
            assertEquals("hello", matchQuery.value());
        }
    }

    /**
     * Tests that serializers with no optional params produce default QueryBuilders.
     * Validates Requirement 11.9.
     */
    public void testSerializerWithNoOptionalParamsProducesDefaults() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH);
        assertNotNull("MATCH serializer must be registered", serializer);

        // Build a call with only field and query (no optional params)
        RexCall call = buildSingleFieldRexCallWithParams("title", "hello", "MATCH", Map.of());
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchQueryBuilder);
            MatchQueryBuilder matchQuery = (MatchQueryBuilder) deserialized;
            assertEquals("title", matchQuery.fieldName());
            assertEquals("hello", matchQuery.value());
            // Default operator is OR
            assertEquals(Operator.OR, matchQuery.operator());
        }
    }

    // --- Serializer refactoring verification tests ---

    /**
     * Tests that each serializer in the registry is an instance of DelegatedPredicateSerializer
     * (class instances, not method references).
     */
    public void testSerializersAreDelegatedPredicateSerializerInstances() {
        for (Map.Entry<ScalarFunction, DelegatedPredicateSerializer> entry : serializers.entrySet()) {
            assertNotNull("Serializer for " + entry.getKey() + " must not be null", entry.getValue());
            assertTrue(
                "Serializer for " + entry.getKey() + " must be an instance of DelegatedPredicateSerializer",
                entry.getValue() instanceof DelegatedPredicateSerializer
            );
        }
    }

    // --- MatchAllSerializer tests ---

    /**
     * Tests MatchAllSerializer serialize → deserialize produces MatchAllQueryBuilder.
     */
    public void testMatchAllSerializerRoundTrip() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCHALL);
        assertNotNull("MATCHALL serializer must be registered", serializer);

        RexCall call = (RexCall) rexBuilder.makeCall(MATCHALL_FUNCTION);
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);
        assertNotNull("Serialized bytes must not be null", serialized);
        assertTrue("Serialized bytes must not be empty", serialized.length > 0);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Deserialized must be MatchAllQueryBuilder", deserialized instanceof MatchAllQueryBuilder);
        }
    }

    /**
     * Tests MatchAllSerializer ignores unexpected operands without throwing.
     */
    public void testMatchAllSerializerIgnoresUnrecognizedOperands() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCHALL);
        assertNotNull("MATCHALL serializer must be registered", serializer);

        RexNode extraMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("unexpected"),
            rexBuilder.makeLiteral("value")
        );
        RexCall call = (RexCall) rexBuilder.makeCall(MATCHALL_FUNCTION, extraMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Deserialized must be MatchAllQueryBuilder", deserialized instanceof MatchAllQueryBuilder);
        }
    }

    // --- WildcardQuerySerializer tests ---

    /**
     * Tests WildcardQuerySerializer throws IllegalArgumentException with "wildcard_query" when field is missing.
     */
    public void testWildcardQuerySerializerThrowsWhenFieldMissing() {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.WILDCARD_QUERY);
        assertNotNull("WILDCARD_QUERY serializer must be registered", serializer);

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("test*")
        );
        RexCall call = (RexCall) rexBuilder.makeCall(WILDCARD_QUERY_FUNCTION, queryMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> serializer.serialize(call, fieldStorage));
        assertTrue(
            "Exception message must contain 'wildcard_query', got: " + exception.getMessage(),
            exception.getMessage().contains("wildcard_query")
        );
    }

    /**
     * Tests WildcardQuerySerializer round-trip with field and pattern.
     */
    public void testWildcardQuerySerializerRoundTrip() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.WILDCARD_QUERY);
        assertNotNull("WILDCARD_QUERY serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("title", "test*", "WILDCARD_QUERY", Map.of());
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Deserialized must be WildcardQueryBuilder", deserialized instanceof WildcardQueryBuilder);
            WildcardQueryBuilder wildcardQb = (WildcardQueryBuilder) deserialized;
            assertEquals("title", wildcardQb.fieldName());
            assertEquals("test*", wildcardQb.value());
        }
    }

    /**
     * Tests WildcardQuerySerializer silently ignores unrecognized params.
     */
    public void testWildcardQuerySerializerIgnoresUnrecognizedParams() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.WILDCARD_QUERY);
        assertNotNull("WILDCARD_QUERY serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("title", "test*", "WILDCARD_QUERY", Map.of("unknown_param", "value"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Deserialized must be WildcardQueryBuilder", deserialized instanceof WildcardQueryBuilder);
            WildcardQueryBuilder wildcardQb = (WildcardQueryBuilder) deserialized;
            assertEquals("title", wildcardQb.fieldName());
            assertEquals("test*", wildcardQb.value());
        }
    }

    /**
     * Tests WildcardQuerySerializer with boost optional param round-trip.
     */
    public void testWildcardQuerySerializerWithBoost() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.WILDCARD_QUERY);
        assertNotNull("WILDCARD_QUERY serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("title", "test*", "WILDCARD_QUERY", Map.of("boost", "2.5"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Deserialized must be WildcardQueryBuilder", deserialized instanceof WildcardQueryBuilder);
            WildcardQueryBuilder wildcardQb = (WildcardQueryBuilder) deserialized;
            assertEquals("title", wildcardQb.fieldName());
            assertEquals("test*", wildcardQb.value());
            assertEquals(2.5f, wildcardQb.boost(), 0.001f);
        }
    }

    // --- QuerySerializer (no-field) tests ---

    /**
     * Tests QuerySerializer throws IllegalArgumentException with "query" when query is missing.
     */
    public void testQuerySerializerThrowsWhenQueryMissing() {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.QUERY);
        assertNotNull("QUERY serializer must be registered", serializer);

        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );
        RexCall call = (RexCall) rexBuilder.makeCall(QUERY_FUNCTION, fieldMap);
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> serializer.serialize(call, fieldStorage));
        assertTrue(
            "Exception message must contain 'query', got: " + exception.getMessage(),
            exception.getMessage().contains("query")
        );
    }

    /**
     * Tests QuerySerializer accepts missing field without exception (no-field variant).
     */
    public void testQuerySerializerAcceptsMissingField() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.QUERY);
        assertNotNull("QUERY serializer must be registered", serializer);

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("status:active AND type:premium")
        );
        RexCall call = (RexCall) rexBuilder.makeCall(QUERY_FUNCTION, queryMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);
        assertNotNull("Serialized bytes must not be null", serialized);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Deserialized must be QueryStringQueryBuilder", deserialized instanceof QueryStringQueryBuilder);
            QueryStringQueryBuilder queryStringQb = (QueryStringQueryBuilder) deserialized;
            assertEquals("status:active AND type:premium", queryStringQb.queryString());
        }
    }

    /**
     * Tests QuerySerializer silently ignores unrecognized params.
     */
    public void testQuerySerializerIgnoresUnrecognizedParams() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.QUERY);
        assertNotNull("QUERY serializer must be registered", serializer);

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello world")
        );
        RexNode unknownParamMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("unknown_param"),
            rexBuilder.makeLiteral("some_value")
        );
        RexCall call = (RexCall) rexBuilder.makeCall(QUERY_FUNCTION, queryMap, unknownParamMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Deserialized must be QueryStringQueryBuilder", deserialized instanceof QueryStringQueryBuilder);
            QueryStringQueryBuilder queryStringQb = (QueryStringQueryBuilder) deserialized;
            assertEquals("hello world", queryStringQb.queryString());
        }
    }

    /**
     * Tests QuerySerializer with default_operator=AND optional param round-trip.
     */
    public void testQuerySerializerWithDefaultOperator() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.QUERY);
        assertNotNull("QUERY serializer must be registered", serializer);

        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral("hello world")
        );
        RexNode operatorMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("default_operator"),
            rexBuilder.makeLiteral("AND")
        );
        RexCall call = (RexCall) rexBuilder.makeCall(QUERY_FUNCTION, queryMap, operatorMap);
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Deserialized must be QueryStringQueryBuilder", deserialized instanceof QueryStringQueryBuilder);
            QueryStringQueryBuilder queryStringQb = (QueryStringQueryBuilder) deserialized;
            assertEquals("hello world", queryStringQb.queryString());
            assertEquals(Operator.AND, queryStringQb.defaultOperator());
        }
    }

    // --- New optional parameter round-trip tests ---

    /**
     * Tests that max_expansions=50 round-trips correctly on MatchQueryBuilder.
     */
    public void testSerializeMatchWithMaxExpansions() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH);
        assertNotNull("MATCH serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("title", "hello", "MATCH", Map.of("max_expansions", "50"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchQueryBuilder);
            MatchQueryBuilder matchQuery = (MatchQueryBuilder) deserialized;
            assertEquals("title", matchQuery.fieldName());
            assertEquals("hello", matchQuery.value());
            assertEquals(50, matchQuery.maxExpansions());
        }
    }

    /**
     * Tests that lenient=true round-trips correctly on MatchQueryBuilder.
     */
    public void testSerializeMatchWithLenient() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH);
        assertNotNull("MATCH serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("title", "hello", "MATCH", Map.of("lenient", "true"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchQueryBuilder);
            MatchQueryBuilder matchQuery = (MatchQueryBuilder) deserialized;
            assertEquals("title", matchQuery.fieldName());
            assertEquals("hello", matchQuery.value());
            assertTrue("lenient should be true", matchQuery.lenient());
        }
    }

    /**
     * Tests that prefix_length=3 round-trips correctly on MatchBoolPrefixQueryBuilder.
     */
    public void testSerializeMatchBoolPrefixWithPrefixLength() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH_BOOL_PREFIX);
        assertNotNull("MATCH_BOOL_PREFIX serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("title", "quick br", "MATCH_BOOL_PREFIX", Map.of("prefix_length", "3"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("title", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchBoolPrefixQueryBuilder);
            MatchBoolPrefixQueryBuilder matchBoolPrefix = (MatchBoolPrefixQueryBuilder) deserialized;
            assertEquals("title", matchBoolPrefix.fieldName());
            assertEquals("quick br", matchBoolPrefix.value());
            assertEquals(3, matchBoolPrefix.prefixLength());
        }
    }

    /**
     * Tests that boost=2.5 round-trips correctly on MatchPhraseQueryBuilder.
     */
    public void testSerializeMatchPhraseWithBoost() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH_PHRASE);
        assertNotNull("MATCH_PHRASE serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("body", "quick fox", "MATCH_PHRASE", Map.of("boost", "2.5"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("body", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchPhraseQueryBuilder);
            MatchPhraseQueryBuilder matchPhrase = (MatchPhraseQueryBuilder) deserialized;
            assertEquals("body", matchPhrase.fieldName());
            assertEquals("quick fox", matchPhrase.value());
            assertEquals(2.5f, matchPhrase.boost(), 0.001f);
        }
    }

    /**
     * Tests that boost=1.5 round-trips correctly on MatchPhrasePrefixQueryBuilder.
     */
    public void testSerializeMatchPhrasePrefixWithBoost() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH_PHRASE_PREFIX);
        assertNotNull("MATCH_PHRASE_PREFIX serializer must be registered", serializer);

        RexCall call = buildSingleFieldRexCallWithParams("body", "quick br", "MATCH_PHRASE_PREFIX", Map.of("boost", "1.5"));
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo("body", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MatchPhrasePrefixQueryBuilder);
            MatchPhrasePrefixQueryBuilder matchPhrasePrefix = (MatchPhrasePrefixQueryBuilder) deserialized;
            assertEquals("body", matchPhrasePrefix.fieldName());
            assertEquals("quick br", matchPhrasePrefix.value());
            assertEquals(1.5f, matchPhrasePrefix.boost(), 0.001f);
        }
    }

    /**
     * Tests that slop=3 round-trips correctly on MultiMatchQueryBuilder.
     */
    public void testSerializeMultiMatchWithSlop() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MULTI_MATCH);
        assertNotNull("MULTI_MATCH serializer must be registered", serializer);

        RexCall call = buildMultiFieldRexCallWithParams(List.of("title", "body"), "search text", "MULTI_MATCH", Map.of("slop", "3"));
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MultiMatchQueryBuilder);
            MultiMatchQueryBuilder multiMatch = (MultiMatchQueryBuilder) deserialized;
            assertEquals("search text", multiMatch.value());
            assertEquals(3, multiMatch.slop());
        }
    }

    /**
     * Tests that fuzzy_rewrite="constant_score" round-trips correctly on MultiMatchQueryBuilder.
     */
    public void testSerializeMultiMatchWithFuzzyRewrite() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MULTI_MATCH);
        assertNotNull("MULTI_MATCH serializer must be registered", serializer);

        RexCall call = buildMultiFieldRexCallWithParams(
            List.of("title", "body"),
            "search text",
            "MULTI_MATCH",
            Map.of("fuzzy_rewrite", "constant_score")
        );
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof MultiMatchQueryBuilder);
            MultiMatchQueryBuilder multiMatch = (MultiMatchQueryBuilder) deserialized;
            assertEquals("search text", multiMatch.value());
            assertEquals("constant_score", multiMatch.fuzzyRewrite());
        }
    }

    /**
     * Tests that fuzziness=AUTO round-trips correctly on QueryStringQueryBuilder.
     */
    public void testSerializeQueryStringWithFuzziness() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.QUERY_STRING);
        assertNotNull("QUERY_STRING serializer must be registered", serializer);

        RexCall call = buildMultiFieldRexCallWithParams(
            List.of("title", "body"),
            "search text",
            "QUERY_STRING",
            Map.of("fuzziness", "AUTO")
        );
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof QueryStringQueryBuilder);
            QueryStringQueryBuilder queryString = (QueryStringQueryBuilder) deserialized;
            assertEquals("search text", queryString.queryString());
            assertEquals(Fuzziness.AUTO, queryString.fuzziness());
        }
    }

    /**
     * Tests that phrase_slop=2 round-trips correctly on QueryStringQueryBuilder.
     */
    public void testSerializeQueryStringWithPhraseSlop() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.QUERY_STRING);
        assertNotNull("QUERY_STRING serializer must be registered", serializer);

        RexCall call = buildMultiFieldRexCallWithParams(
            List.of("title", "body"),
            "search text",
            "QUERY_STRING",
            Map.of("phrase_slop", "2")
        );
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof QueryStringQueryBuilder);
            QueryStringQueryBuilder queryString = (QueryStringQueryBuilder) deserialized;
            assertEquals("search text", queryString.queryString());
            assertEquals(2, queryString.phraseSlop());
        }
    }

    /**
     * Tests that lenient=true round-trips correctly on SimpleQueryStringBuilder.
     */
    public void testSerializeSimpleQueryStringWithLenient() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.SIMPLE_QUERY_STRING);
        assertNotNull("SIMPLE_QUERY_STRING serializer must be registered", serializer);

        RexCall call = buildMultiFieldRexCallWithParams(
            List.of("title", "body"),
            "search text",
            "SIMPLE_QUERY_STRING",
            Map.of("lenient", "true")
        );
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof SimpleQueryStringBuilder);
            SimpleQueryStringBuilder simpleQsQb = (SimpleQueryStringBuilder) deserialized;
            assertEquals("search text", simpleQsQb.value());
            assertTrue("lenient should be true", simpleQsQb.lenient());
        }
    }

    // --- SimpleQueryString flags tests ---

    /**
     * Tests that pipe-separated flags "NONE|PREFIX|ESCAPE" round-trip correctly on SimpleQueryStringBuilder.
     * The flags should be applied without throwing, and the deserialized builder should match
     * an expected builder with the same flags set.
     */
    public void testSerializeSimpleQueryStringWithPipeSeparatedFlags() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.SIMPLE_QUERY_STRING);
        assertNotNull("SIMPLE_QUERY_STRING serializer must be registered", serializer);

        RexCall call = buildMultiFieldRexCallWithParams(
            List.of("title", "body"),
            "search text",
            "SIMPLE_QUERY_STRING",
            Map.of("flags", "NONE|PREFIX|ESCAPE")
        );
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof SimpleQueryStringBuilder);
            SimpleQueryStringBuilder simpleQsQb = (SimpleQueryStringBuilder) deserialized;
            assertEquals("search text", simpleQsQb.value());
            // Build an expected builder with the same flags to verify equality
            SimpleQueryStringBuilder expected = new SimpleQueryStringBuilder("search text");
            expected.field("title", 1.0f);
            expected.field("body", 1.0f);
            expected.flags(SimpleQueryStringFlag.NONE, SimpleQueryStringFlag.PREFIX, SimpleQueryStringFlag.ESCAPE);
            assertEquals(expected, simpleQsQb);
        }
    }

    /**
     * Tests that flags="ALL" works correctly on SimpleQueryStringBuilder.
     */
    public void testSerializeSimpleQueryStringWithSingleFlag() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.SIMPLE_QUERY_STRING);
        assertNotNull("SIMPLE_QUERY_STRING serializer must be registered", serializer);

        RexCall call = buildMultiFieldRexCallWithParams(
            List.of("title", "body"),
            "search text",
            "SIMPLE_QUERY_STRING",
            Map.of("flags", "ALL")
        );
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(deserialized instanceof SimpleQueryStringBuilder);
            SimpleQueryStringBuilder simpleQsQb = (SimpleQueryStringBuilder) deserialized;
            assertEquals("search text", simpleQsQb.value());
            // Build an expected builder with ALL flag to verify equality
            SimpleQueryStringBuilder expected = new SimpleQueryStringBuilder("search text");
            expected.field("title", 1.0f);
            expected.field("body", 1.0f);
            expected.flags(SimpleQueryStringFlag.ALL);
            assertEquals(expected, simpleQsQb);
        }
    }

    // --- Helper methods ---

    /**
     * Builds a single-field RexCall with optional parameters:
     * FUNC(MAP('field', $0), MAP('query', 'text'), MAP('key1', 'val1'), MAP('key2', 'val2'), ...)
     */
    private RexCall buildSingleFieldRexCallWithParams(
        String fieldName,
        String queryText,
        String functionName,
        Map<String, String> optionalParams
    ) {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        // Operand 0: MAP('field', $0)
        RexNode fieldMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("field"),
            rexBuilder.makeInputRef(varcharType, 0)
        );

        // Operand 1: MAP('query', 'queryText')
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral(queryText)
        );

        // Build operand list: field, query, then optional params
        List<RexNode> operands = new ArrayList<>();
        operands.add(fieldMap);
        operands.add(queryMap);

        for (Map.Entry<String, String> entry : optionalParams.entrySet()) {
            RexNode paramMap = rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral(entry.getKey()),
                rexBuilder.makeLiteral(entry.getValue())
            );
            operands.add(paramMap);
        }

        SqlFunction sqlFunction = new SqlFunction(
            functionName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );

        return (RexCall) rexBuilder.makeCall(sqlFunction, operands);
    }

    /**
     * Builds a multi-field RexCall with optional parameters:
     * FUNC(MAP('fields', MAP('field1', 1.0, 'field2', 1.0, ...)), MAP('query', 'text'), MAP('key1', 'val1'), ...)
     */
    private RexCall buildMultiFieldRexCallWithParams(
        List<String> fields,
        String queryText,
        String functionName,
        Map<String, String> optionalParams
    ) {
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        // Build nested MAP with field/boost pairs
        List<RexNode> nestedMapOperands = new ArrayList<>();
        for (String field : fields) {
            nestedMapOperands.add(rexBuilder.makeLiteral(field));
            nestedMapOperands.add(rexBuilder.makeExactLiteral(new java.math.BigDecimal("1.0"), doubleType));
        }
        RexNode nestedMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, nestedMapOperands);

        // Outer MAP: MAP('fields', nestedMap)
        RexNode outerMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral("fields"), nestedMap);

        // Query MAP: MAP('query', 'text')
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral(queryText)
        );

        // Build operand list
        List<RexNode> operands = new ArrayList<>();
        operands.add(outerMap);
        operands.add(queryMap);

        for (Map.Entry<String, String> entry : optionalParams.entrySet()) {
            RexNode paramMap = rexBuilder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                rexBuilder.makeLiteral(entry.getKey()),
                rexBuilder.makeLiteral(entry.getValue())
            );
            operands.add(paramMap);
        }

        SqlFunction sqlFunction = new SqlFunction(
            functionName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );

        return (RexCall) rexBuilder.makeCall(sqlFunction, operands);
    }
}
