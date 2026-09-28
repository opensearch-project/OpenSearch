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
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Property-based test for multi-field serialization round-trip.
 *
 *
 * <p>For any non-empty list of valid field names and non-empty query string, serializing a
 * multi-field full-text predicate (multi_match, query_string, or simple_query_string) via its
 * registered serializer and then deserializing the resulting bytes with
 * {@code readNamedWriteable(QueryBuilder.class)} shall produce a QueryBuilder of the correct
 * subclass containing all the specified fields and the same query text.
 *
 */
public class MultiFieldSerializationPropertyTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MultiMatchQueryBuilder.NAME, MultiMatchQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, QueryStringQueryBuilder.NAME, QueryStringQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, SimpleQueryStringBuilder.NAME, SimpleQueryStringBuilder::new)
        )
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
     * Multi-field serialization round-trip for MULTI_MATCH.
     *
     * For a random list of 1-5 field names and a random query string,
     * serializing via the MULTI_MATCH serializer and deserializing produces a
     * MultiMatchQueryBuilder with all fields present and the correct query text.
     */
    public void testMultiMatchRoundTrip() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MULTI_MATCH);
        assertNotNull("MULTI_MATCH serializer must be registered", serializer);

        int fieldCount = randomIntBetween(1, 5);
        List<String> fieldNames = generateUniqueFieldNames(fieldCount);
        String queryText = randomAlphaOfLengthBetween(1, 200);

        RexCall call = buildMultiFieldRexCall(fieldNames, queryText, "MULTI_MATCH");
        List<FieldStorageInfo> fieldStorage = List.of(); // not used in nested MAP literal path

        byte[] serialized = serializer.serialize(call, fieldStorage);
        assertNotNull("Serialized bytes should not be null", serialized);
        assertTrue("Serialized bytes should not be empty", serialized.length > 0);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(
                "Deserialized should be MultiMatchQueryBuilder but was " + deserialized.getClass().getSimpleName(),
                deserialized instanceof MultiMatchQueryBuilder
            );
            MultiMatchQueryBuilder multiMatch = (MultiMatchQueryBuilder) deserialized;
            assertEquals("Query text mismatch", queryText, multiMatch.value());
            assertEquals("Field count mismatch", fieldCount, multiMatch.fields().size());
            for (String expectedField : fieldNames) {
                assertTrue("Missing field: " + expectedField, multiMatch.fields().containsKey(expectedField));
            }
        }
    }

    /**
     * Multi-field serialization round-trip for QUERY_STRING.
     *
     * For a random list of 1-5 field names and a random query string,
     * serializing via the QUERY_STRING serializer and deserializing produces a
     * QueryStringQueryBuilder with all fields present and the correct query text.
     */
    public void testQueryStringRoundTrip() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.QUERY_STRING);
        assertNotNull("QUERY_STRING serializer must be registered", serializer);

        int fieldCount = randomIntBetween(1, 5);
        List<String> fieldNames = generateUniqueFieldNames(fieldCount);
        String queryText = randomAlphaOfLengthBetween(1, 200);

        RexCall call = buildMultiFieldRexCall(fieldNames, queryText, "QUERY_STRING");
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);
        assertNotNull("Serialized bytes should not be null", serialized);
        assertTrue("Serialized bytes should not be empty", serialized.length > 0);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(
                "Deserialized should be QueryStringQueryBuilder but was " + deserialized.getClass().getSimpleName(),
                deserialized instanceof QueryStringQueryBuilder
            );
            QueryStringQueryBuilder queryString = (QueryStringQueryBuilder) deserialized;
            assertEquals("Query text mismatch", queryText, queryString.queryString());
            assertEquals("Field count mismatch", fieldCount, queryString.fields().size());
            for (String expectedField : fieldNames) {
                assertTrue("Missing field: " + expectedField, queryString.fields().containsKey(expectedField));
            }
        }
    }

    /**
     * Multi-field serialization round-trip for SIMPLE_QUERY_STRING.
     *
     * For a random list of 1-5 field names and a random query string,
     * serializing via the SIMPLE_QUERY_STRING serializer and deserializing produces a
     * SimpleQueryStringBuilder with all fields present and the correct query text.
     */
    public void testSimpleQueryStringRoundTrip() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.SIMPLE_QUERY_STRING);
        assertNotNull("SIMPLE_QUERY_STRING serializer must be registered", serializer);

        int fieldCount = randomIntBetween(1, 5);
        List<String> fieldNames = generateUniqueFieldNames(fieldCount);
        String queryText = randomAlphaOfLengthBetween(1, 200);

        RexCall call = buildMultiFieldRexCall(fieldNames, queryText, "SIMPLE_QUERY_STRING");
        List<FieldStorageInfo> fieldStorage = List.of();

        byte[] serialized = serializer.serialize(call, fieldStorage);
        assertNotNull("Serialized bytes should not be null", serialized);
        assertTrue("Serialized bytes should not be empty", serialized.length > 0);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(
                "Deserialized should be SimpleQueryStringBuilder but was " + deserialized.getClass().getSimpleName(),
                deserialized instanceof SimpleQueryStringBuilder
            );
            SimpleQueryStringBuilder simpleQueryString = (SimpleQueryStringBuilder) deserialized;
            assertEquals("Query text mismatch", queryText, simpleQueryString.value());
            assertEquals("Field count mismatch", fieldCount, simpleQueryString.fields().size());
            for (String expectedField : fieldNames) {
                assertTrue("Missing field: " + expectedField, simpleQueryString.fields().containsKey(expectedField));
            }
        }
    }

    /**
     * Generates a list of unique random field names.
     */
    private List<String> generateUniqueFieldNames(int count) {
        List<String> names = new ArrayList<>();
        while (names.size() < count) {
            String candidate = randomAlphaOfLengthBetween(1, 50);
            if (names.contains(candidate) == false) {
                names.add(candidate);
            }
        }
        return names;
    }

    /**
     * Builds a mock RexCall simulating the multi-field relevance function structure:
     * FUNC(MAP('fields', MAP('field1', 1.0, 'field2', 1.0, ...)), MAP('query', 'queryText'))
     *
     * The serializer extracts fields via extractFieldsFromRelevanceMap(call, 0, fieldStorage)
     * which expects operand 0 to be an outer MAP with a nested MAP at child index 1.
     * The nested MAP has alternating field name literals (even indices) and boost literals (odd indices).
     */
    private RexCall buildMultiFieldRexCall(List<String> fieldNames, String queryText, String functionName) {
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        // Build nested MAP operands: field1, boost1, field2, boost2, ...
        List<RexNode> nestedMapOperands = new ArrayList<>();
        for (String field : fieldNames) {
            nestedMapOperands.add(rexBuilder.makeLiteral(field));
            nestedMapOperands.add(rexBuilder.makeExactLiteral(new BigDecimal("1.0"), doubleType));
        }

        // Nested MAP: MAP_VALUE_CONSTRUCTOR(field1, 1.0, field2, 1.0, ...)
        RexNode nestedMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, nestedMapOperands.toArray(new RexNode[0]));

        // Outer MAP: MAP('fields', nestedMap)
        RexNode outerMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral("fields"), nestedMap);

        // Query MAP: MAP('query', 'queryText')
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral(queryText)
        );

        // Top-level function call
        SqlFunction sqlFunction = new SqlFunction(
            functionName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );

        return (RexCall) rexBuilder.makeCall(sqlFunction, outerMap, queryMap);
    }
}
