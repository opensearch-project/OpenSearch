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
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Property-based test for single-field serialization round-trip.
 *
 * <p>Feature: lucene-fulltext-serializers, Property: Single-field serialization round-trip
 *
 * <p>For any valid field name and non-empty query string, serializing a single-field full-text
 * predicate (match_phrase, match_bool_prefix, or match_phrase_prefix) via its registered serializer
 * and then deserializing the resulting bytes with {@code readNamedWriteable(QueryBuilder.class)}
 * shall produce a QueryBuilder of the correct subclass containing the same field name and query text.
 *
 * <p>Validates: Requirements 3.5, 4.5, 5.5
 */
public class SingleFieldSerializationPropertyTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchPhraseQueryBuilder.NAME, MatchPhraseQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchBoolPrefixQueryBuilder.NAME, MatchBoolPrefixQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchPhrasePrefixQueryBuilder.NAME, MatchPhrasePrefixQueryBuilder::new)
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
     * Single-field serialization round-trip for MATCH_PHRASE.
     *
     * For any random field name (1-50 alphanumeric chars) and random query string (1-200 chars),
     * serializing via the MATCH_PHRASE serializer and deserializing produces a MatchPhraseQueryBuilder
     * with the same field and query.
     */
    public void testMatchPhraseRoundTrip() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH_PHRASE);
        assertNotNull("MATCH_PHRASE serializer must be registered", serializer);

        String fieldName = randomAlphaOfLengthBetween(1, 50);
        String queryText = randomAlphaOfLengthBetween(1, 200);

        RexCall call = buildSingleFieldRexCall(fieldName, queryText, "MATCH_PHRASE");
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo(fieldName, "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);
        assertNotNull("Serialized bytes should not be null", serialized);
        assertTrue("Serialized bytes should not be empty", serialized.length > 0);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(
                "Deserialized should be MatchPhraseQueryBuilder but was " + deserialized.getClass().getSimpleName(),
                deserialized instanceof MatchPhraseQueryBuilder
            );
            MatchPhraseQueryBuilder matchPhrase = (MatchPhraseQueryBuilder) deserialized;
            assertEquals("Field name mismatch", fieldName, matchPhrase.fieldName());
            assertEquals("Query text mismatch", queryText, matchPhrase.value());
        }
    }

    /**
     * Single-field serialization round-trip for MATCH_BOOL_PREFIX.
     *
     * For any random field name (1-50 alphanumeric chars) and random query string (1-200 chars),
     * serializing via the MATCH_BOOL_PREFIX serializer and deserializing produces a
     * MatchBoolPrefixQueryBuilder with the same field and query.
     */
    public void testMatchBoolPrefixRoundTrip() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH_BOOL_PREFIX);
        assertNotNull("MATCH_BOOL_PREFIX serializer must be registered", serializer);

        String fieldName = randomAlphaOfLengthBetween(1, 50);
        String queryText = randomAlphaOfLengthBetween(1, 200);

        RexCall call = buildSingleFieldRexCall(fieldName, queryText, "MATCH_BOOL_PREFIX");
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo(fieldName, "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);
        assertNotNull("Serialized bytes should not be null", serialized);
        assertTrue("Serialized bytes should not be empty", serialized.length > 0);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(
                "Deserialized should be MatchBoolPrefixQueryBuilder but was " + deserialized.getClass().getSimpleName(),
                deserialized instanceof MatchBoolPrefixQueryBuilder
            );
            MatchBoolPrefixQueryBuilder matchBoolPrefix = (MatchBoolPrefixQueryBuilder) deserialized;
            assertEquals("Field name mismatch", fieldName, matchBoolPrefix.fieldName());
            assertEquals("Query text mismatch", queryText, matchBoolPrefix.value());
        }
    }

    /**
     * Single-field serialization round-trip for MATCH_PHRASE_PREFIX.
     *
     * For any random field name (1-50 alphanumeric chars) and random query string (1-200 chars),
     * serializing via the MATCH_PHRASE_PREFIX serializer and deserializing produces a
     * MatchPhrasePrefixQueryBuilder with the same field and query.
     */
    public void testMatchPhrasePrefixRoundTrip() throws IOException {
        DelegatedPredicateSerializer serializer = serializers.get(ScalarFunction.MATCH_PHRASE_PREFIX);
        assertNotNull("MATCH_PHRASE_PREFIX serializer must be registered", serializer);

        String fieldName = randomAlphaOfLengthBetween(1, 50);
        String queryText = randomAlphaOfLengthBetween(1, 200);

        RexCall call = buildSingleFieldRexCall(fieldName, queryText, "MATCH_PHRASE_PREFIX");
        List<FieldStorageInfo> fieldStorage = List.of(
            new FieldStorageInfo(fieldName, "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false)
        );

        byte[] serialized = serializer.serialize(call, fieldStorage);
        assertNotNull("Serialized bytes should not be null", serialized);
        assertTrue("Serialized bytes should not be empty", serialized.length > 0);

        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(serialized), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue(
                "Deserialized should be MatchPhrasePrefixQueryBuilder but was " + deserialized.getClass().getSimpleName(),
                deserialized instanceof MatchPhrasePrefixQueryBuilder
            );
            MatchPhrasePrefixQueryBuilder matchPhrasePrefix = (MatchPhrasePrefixQueryBuilder) deserialized;
            assertEquals("Field name mismatch", fieldName, matchPhrasePrefix.fieldName());
            assertEquals("Query text mismatch", queryText, matchPhrasePrefix.value());
        }
    }

    /**
     * Builds a mock RexCall simulating the single-field relevance function structure:
     * FUNC(MAP('field', $0), MAP('query', 'queryText'))
     *
     * The serializer extracts the field via extractFieldFromRelevanceMap(call, 0, fieldStorage)
     * which expects operand 0 to be a RexCall (MAP) with a RexInputRef at child index 1.
     * It extracts the query via extractStringFromRelevanceMap(call, 1) which expects operand 1
     * to be a RexCall (MAP) with a RexLiteral at child index 1.
     */
    private RexCall buildSingleFieldRexCall(String fieldName, String queryText, String functionName) {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        // Operand 0: MAP('field', $0) — field reference
        RexNode fieldKey = rexBuilder.makeLiteral("field");
        RexNode fieldRef = rexBuilder.makeInputRef(varcharType, 0);
        RexNode fieldMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, fieldKey, fieldRef);

        // Operand 1: MAP('query', 'queryText') — query text literal
        RexNode queryKey = rexBuilder.makeLiteral("query");
        RexNode queryLiteral = rexBuilder.makeLiteral(queryText);
        RexNode queryMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, queryKey, queryLiteral);

        // Top-level function call
        SqlFunction sqlFunction = new SqlFunction(
            functionName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );

        return (RexCall) rexBuilder.makeCall(sqlFunction, fieldMap, queryMap);
    }
}
