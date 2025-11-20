/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.protobufs.MultiMatchQuery;
import org.opensearch.protobufs.TextQueryType;
import org.opensearch.protobufs.ZeroTermsQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.transport.grpc.proto.request.search.query.MultiMatchQueryBuilderProtoUtils.fromProto;

public class MultiMatchQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        QueryBuilderProtoTestUtils.setupRegistry();
    }

    public void testFromProtoWithRequiredFieldsOnly() {
        // Create a minimal MultiMatchQuery proto with only required fields
        MultiMatchQuery proto = MultiMatchQuery.newBuilder().setQuery("test query").addFields("field1").build();

        // Convert to MultiMatchQueryBuilder
        MultiMatchQueryBuilder builder = fromProto(proto);

        // Verify basic properties
        assertEquals("test query", builder.value());
        assertTrue(builder.fields().containsKey("field1"));
        assertEquals(1.0f, builder.fields().get("field1"), 0.001f);
        assertEquals(MultiMatchQueryBuilder.DEFAULT_TYPE, builder.type());
        assertNull(builder.analyzer());
        assertEquals(MultiMatchQueryBuilder.DEFAULT_PHRASE_SLOP, builder.slop());
        assertEquals(MultiMatchQueryBuilder.DEFAULT_PREFIX_LENGTH, builder.prefixLength());
        assertEquals(MultiMatchQueryBuilder.DEFAULT_MAX_EXPANSIONS, builder.maxExpansions());
        assertEquals(MultiMatchQueryBuilder.DEFAULT_OPERATOR, builder.operator());
        assertNull(builder.minimumShouldMatch());
        assertNull(builder.fuzzyRewrite());
        assertNull(builder.tieBreaker());
        assertEquals(1.0f, builder.boost(), 0.001f);
        assertNull(builder.queryName());
    }

    public void testFromProtoWithAllFields() {
        // Create a complete MultiMatchQuery proto with all fields set
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .addFields("field2")
            .setType(TextQueryType.TEXT_QUERY_TYPE_PHRASE)
            .setAnalyzer("standard")
            .setSlop(2)
            .setPrefixLength(3)
            .setMaxExpansions(10)
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_AND)
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setString("2").build())
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .setTieBreaker(0.5f)
            .setLenient(true)
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_ALL)
            .setAutoGenerateSynonymsPhraseQuery(false)
            .setFuzzyTranspositions(false)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        // Convert to MultiMatchQueryBuilder
        MultiMatchQueryBuilder builder = fromProto(proto);

        // Verify all properties
        assertEquals("test query", builder.value());
        assertEquals(2, builder.fields().size());
        assertTrue(builder.fields().containsKey("field1"));
        assertTrue(builder.fields().containsKey("field2"));
        assertEquals(1.0f, builder.fields().get("field1"), 0.001f);
        assertEquals(1.0f, builder.fields().get("field2"), 0.001f);
        assertEquals(MultiMatchQueryBuilder.Type.PHRASE, builder.type());
        assertEquals("standard", builder.analyzer());
        assertEquals(2, builder.slop());
        assertEquals(3, builder.prefixLength());
        assertEquals(10, builder.maxExpansions());
        assertEquals(Operator.AND, builder.operator());
        assertEquals("2", builder.minimumShouldMatch());
        assertEquals("constant_score", builder.fuzzyRewrite());
        assertEquals(0.5f, builder.tieBreaker(), 0.001f);
        assertTrue(builder.lenient());
        assertEquals(MatchQuery.ZeroTermsQuery.ALL, builder.zeroTermsQuery());
        assertFalse(builder.autoGenerateSynonymsPhraseQuery());
        assertFalse(builder.fuzzyTranspositions());
        assertEquals(2.0f, builder.boost(), 0.001f);
        assertEquals("test_query", builder.queryName());
    }

    public void testFromProtoWithIntMinimumShouldMatch() {
        // Create a proto with int32 minimum_should_match
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setInt32(2).build())
            .build();

        // Convert to MultiMatchQueryBuilder
        MultiMatchQueryBuilder builder = fromProto(proto);

        // Verify minimum_should_match
        assertEquals("2", builder.minimumShouldMatch());
    }

    public void testFromProtoWithStringMinimumShouldMatch() {
        // Create a proto with string minimum_should_match
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setString("75%").build())
            .build();

        // Convert to MultiMatchQueryBuilder
        MultiMatchQueryBuilder builder = fromProto(proto);

        // Verify minimum_should_match
        assertEquals("75%", builder.minimumShouldMatch());
    }

    public void testFromProtoWithDifferentTypes() {
        // Test all possible types
        TextQueryType[] types = {
            TextQueryType.TEXT_QUERY_TYPE_BEST_FIELDS,
            TextQueryType.TEXT_QUERY_TYPE_MOST_FIELDS,
            TextQueryType.TEXT_QUERY_TYPE_CROSS_FIELDS,
            TextQueryType.TEXT_QUERY_TYPE_PHRASE,
            TextQueryType.TEXT_QUERY_TYPE_PHRASE_PREFIX,
            TextQueryType.TEXT_QUERY_TYPE_BOOL_PREFIX };

        MultiMatchQueryBuilder.Type[] expectedTypes = {
            MultiMatchQueryBuilder.Type.BEST_FIELDS,
            MultiMatchQueryBuilder.Type.MOST_FIELDS,
            MultiMatchQueryBuilder.Type.CROSS_FIELDS,
            MultiMatchQueryBuilder.Type.PHRASE,
            MultiMatchQueryBuilder.Type.PHRASE_PREFIX,
            MultiMatchQueryBuilder.Type.BOOL_PREFIX };

        for (int i = 0; i < types.length; i++) {
            MultiMatchQuery proto = MultiMatchQuery.newBuilder().setQuery("test query").addFields("field1").setType(types[i]).build();

            MultiMatchQueryBuilder builder = fromProto(proto);
            assertEquals(expectedTypes[i], builder.type());
        }
    }

    public void testFromProtoWithDifferentOperators() {
        // Test all possible operators
        org.opensearch.protobufs.Operator[] operators = {
            org.opensearch.protobufs.Operator.OPERATOR_AND,
            org.opensearch.protobufs.Operator.OPERATOR_OR };

        Operator[] expectedOperators = { Operator.AND, Operator.OR };

        for (int i = 0; i < operators.length; i++) {
            MultiMatchQuery proto = MultiMatchQuery.newBuilder()
                .setQuery("test query")
                .addFields("field1")
                .setOperator(operators[i])
                .build();

            MultiMatchQueryBuilder builder = fromProto(proto);
            assertEquals(expectedOperators[i], builder.operator());
        }
    }

    public void testFromProtoWithDifferentZeroTermsQuery() {
        // Test all possible zero_terms_query values
        ZeroTermsQuery[] zeroTermsQueries = { ZeroTermsQuery.ZERO_TERMS_QUERY_NONE, ZeroTermsQuery.ZERO_TERMS_QUERY_ALL };

        MatchQuery.ZeroTermsQuery[] expectedZeroTermsQueries = { MatchQuery.ZeroTermsQuery.NONE, MatchQuery.ZeroTermsQuery.ALL };

        for (int i = 0; i < zeroTermsQueries.length; i++) {
            MultiMatchQuery proto = MultiMatchQuery.newBuilder()
                .setQuery("test query")
                .addFields("field1")
                .setZeroTermsQuery(zeroTermsQueries[i])
                .build();

            MultiMatchQueryBuilder builder = fromProto(proto);
            assertEquals(expectedZeroTermsQueries[i], builder.zeroTermsQuery());
        }
    }

    public void testFromProtoWithMultipleFields() {
        // Create a proto with multiple fields
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .addFields("field2")
            .addFields("field3")
            .build();

        // Convert to MultiMatchQueryBuilder
        MultiMatchQueryBuilder builder = fromProto(proto);

        // Verify fields
        assertEquals(3, builder.fields().size());
        Set<String> expectedFields = new HashSet<>(Arrays.asList("field1", "field2", "field3"));
        assertEquals(expectedFields, builder.fields().keySet());
    }

    /**
     * Test that compares the results of fromXContent and fromProto to ensure they produce equivalent results.
     */
    public void testFromProtoMatchesFromXContent() throws IOException {
        // 1. Create a JSON string for XContent parsing
        String json = "{\n"
            + "  \"query\": \"test query\",\n"
            + "  \"fields\": [\"field1\", \"field2\"],\n"
            + "  \"type\": \"phrase\",\n"
            + "  \"analyzer\": \"standard\",\n"
            + "  \"slop\": 2,\n"
            + "  \"prefix_length\": 3,\n"
            + "  \"max_expansions\": 10,\n"
            + "  \"operator\": \"AND\",\n"
            + "  \"minimum_should_match\": \"2\",\n"
            + "  \"fuzzy_rewrite\": \"constant_score\",\n"
            + "  \"tie_breaker\": 0.5,\n"
            + "  \"lenient\": true,\n"
            + "  \"zero_terms_query\": \"ALL\",\n"
            + "  \"auto_generate_synonyms_phrase_query\": false,\n"
            + "  \"fuzzy_transpositions\": false,\n"
            + "  \"boost\": 2.0,\n"
            + "  \"_name\": \"test_query\"\n"
            + "}";

        // 2. Parse the JSON to create a MultiMatchQueryBuilder via fromXContent
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // Move to the first token
        MultiMatchQueryBuilder fromXContent = MultiMatchQueryBuilder.fromXContent(parser);

        // 3. Create an equivalent MultiMatchQuery proto
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .addFields("field2")
            .setType(TextQueryType.TEXT_QUERY_TYPE_PHRASE)
            .setAnalyzer("standard")
            .setSlop(2)
            .setPrefixLength(3)
            .setMaxExpansions(10)
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_AND)
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setString("2").build())
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .setTieBreaker(0.5f)
            .setLenient(true)
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_ALL)
            .setAutoGenerateSynonymsPhraseQuery(false)
            .setFuzzyTranspositions(false)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        // 4. Convert the proto to a MultiMatchQueryBuilder
        MultiMatchQueryBuilder fromProto = MultiMatchQueryBuilderProtoUtils.fromProto(proto);

        // 5. Compare the two builders
        assertEquals(fromXContent.value(), fromProto.value());
        assertEquals(fromXContent.fields(), fromProto.fields());
        assertEquals(fromXContent.type(), fromProto.type());
        assertEquals(fromXContent.analyzer(), fromProto.analyzer());
        assertEquals(fromXContent.slop(), fromProto.slop());
        assertEquals(fromXContent.prefixLength(), fromProto.prefixLength());
        assertEquals(fromXContent.maxExpansions(), fromProto.maxExpansions());
        assertEquals(fromXContent.operator(), fromProto.operator());
        assertEquals(fromXContent.minimumShouldMatch(), fromProto.minimumShouldMatch());
        assertEquals(fromXContent.fuzzyRewrite(), fromProto.fuzzyRewrite());
        assertEquals(fromXContent.tieBreaker(), fromProto.tieBreaker(), 0.001f);
        assertEquals(fromXContent.lenient(), fromProto.lenient());
        assertEquals(fromXContent.zeroTermsQuery(), fromProto.zeroTermsQuery());
        assertEquals(fromXContent.autoGenerateSynonymsPhraseQuery(), fromProto.autoGenerateSynonymsPhraseQuery());
        assertEquals(fromXContent.fuzzyTranspositions(), fromProto.fuzzyTranspositions());
        assertEquals(fromXContent.boost(), fromProto.boost(), 0.001f);
        assertEquals(fromXContent.queryName(), fromProto.queryName());
    }

    // ========== Missing Coverage Tests ==========

    public void testFromProtoWithNoFields() {
        // Test with fieldsCount = 0 to cover line 60 branch
        MultiMatchQuery proto = MultiMatchQuery.newBuilder().setQuery("test query").build();

        MultiMatchQueryBuilder builder = fromProto(proto);

        assertEquals("test query", builder.value());
        assertTrue("Fields should be empty", builder.fields().isEmpty());
    }

    public void testFromProtoWithUnspecifiedType() {
        // Test with TEXT_QUERY_TYPE_UNSPECIFIED to cover default case in switch
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setType(TextQueryType.TEXT_QUERY_TYPE_UNSPECIFIED)
            .build();

        MultiMatchQueryBuilder builder = fromProto(proto);

        assertEquals("test query", builder.value());
        assertEquals(MultiMatchQueryBuilder.DEFAULT_TYPE, builder.type()); // Should keep default
    }

    public void testFromProtoWithUnspecifiedOperator() {
        // Test with OPERATOR_UNSPECIFIED to cover default case in operator switch
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_UNSPECIFIED)
            .build();

        MultiMatchQueryBuilder builder = fromProto(proto);

        assertEquals("test query", builder.value());
        assertEquals(MultiMatchQueryBuilder.DEFAULT_OPERATOR, builder.operator()); // Should keep default
    }

    public void testFromProtoWithMinimumShouldMatchNeitherStringNorInt() {
        // Test with MinimumShouldMatch that has neither string nor int32 to cover the else if branch
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().build()) // Empty - no string or int32
            .build();

        MultiMatchQueryBuilder builder = fromProto(proto);

        assertEquals("test query", builder.value());
        assertNull("MinimumShouldMatch should be null when neither string nor int32 is set", builder.minimumShouldMatch());
    }

    public void testFromProtoWithZeroTermsQueryUnspecified() {
        // Test with ZERO_TERMS_QUERY_UNSPECIFIED to cover lines 152-154
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_UNSPECIFIED)
            .build();

        MultiMatchQueryBuilder builder = fromProto(proto);

        assertEquals("test query", builder.value());
        assertEquals(MultiMatchQueryBuilder.DEFAULT_ZERO_TERMS_QUERY, builder.zeroTermsQuery()); // Should keep default
    }

    public void testFromProtoWithSlopValidationForBoolPrefix() {
        // Test slop validation for BOOL_PREFIX type to cover lines 172-173
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setType(TextQueryType.TEXT_QUERY_TYPE_BOOL_PREFIX)
            .setSlop(2) // Non-default slop with BOOL_PREFIX should throw exception
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> fromProto(proto));
        assertTrue("Exception message should mention slop not allowed", exception.getMessage().contains("slop not allowed for type"));
        assertTrue("Exception message should mention BOOL_PREFIX", exception.getMessage().contains("BOOL_PREFIX"));
    }

    public void testFromProtoWithBoolPrefixAndDefaultSlop() {
        // Test BOOL_PREFIX with default slop (should work fine)
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setType(TextQueryType.TEXT_QUERY_TYPE_BOOL_PREFIX)
            // No slop set - should use default
            .build();

        MultiMatchQueryBuilder builder = fromProto(proto);

        assertEquals("test query", builder.value());
        assertEquals(MultiMatchQueryBuilder.Type.BOOL_PREFIX, builder.type());
        assertEquals(MultiMatchQueryBuilder.DEFAULT_PHRASE_SLOP, builder.slop());
    }

    public void testFromProtoWithFuzziness() {
        // Test 1: Fuzziness with string value
        MultiMatchQuery protoString = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setFuzziness(org.opensearch.protobufs.Fuzziness.newBuilder().setString("AUTO").build())
            .build();
        MultiMatchQueryBuilder builderString = fromProto(protoString);
        assertEquals("test query", builderString.value());
        assertNotNull("Fuzziness should not be null (STRING)", builderString.fuzziness());
        assertEquals(Fuzziness.build("AUTO"), builderString.fuzziness());

        MultiMatchQuery protoInt = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setFuzziness(org.opensearch.protobufs.Fuzziness.newBuilder().setInt32(2).build())
            .build();
        MultiMatchQueryBuilder builderInt = fromProto(protoInt);
        assertEquals("test query", builderInt.value());
        assertNotNull("Fuzziness should not be null (INT32)", builderInt.fuzziness());
        assertEquals(Fuzziness.fromEdits(2), builderInt.fuzziness());

        // Test 3: Fuzziness with empty/neither string nor int32
        MultiMatchQuery protoEmpty = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setFuzziness(org.opensearch.protobufs.Fuzziness.newBuilder().build())
            .build();
        MultiMatchQueryBuilder builderEmpty = fromProto(protoEmpty);
        assertEquals("test query", builderEmpty.value());
        assertNull("Fuzziness should be null (EMPTY)", builderEmpty.fuzziness());
    }

    public void testFromProtoWithFuzzyRewriteUnspecified() {
        // Test that UNSPECIFIED fuzzyRewrite is treated as null
        MultiMatchQuery proto = MultiMatchQuery.newBuilder()
            .setQuery("test query")
            .addFields("field1")
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_UNSPECIFIED)
            .build();

        MultiMatchQueryBuilder builder = fromProto(proto);

        // Verify fuzzyRewrite is null when UNSPECIFIED
        assertNull("FuzzyRewrite should be null for UNSPECIFIED", builder.fuzzyRewrite());
    }

    public void testFromProtoWithAllFuzzyRewriteValues() {
        // Test all MultiTermQueryRewrite enum values
        MultiMatchQuery protoConstantScore = MultiMatchQuery.newBuilder()
            .setQuery("test")
            .addFields("field1")
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .build();
        assertEquals("constant_score", fromProto(protoConstantScore).fuzzyRewrite());

        MultiMatchQuery protoConstantScoreBoolean = MultiMatchQuery.newBuilder()
            .setQuery("test")
            .addFields("field1")
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE_BOOLEAN)
            .build();
        assertEquals("constant_score_boolean", fromProto(protoConstantScoreBoolean).fuzzyRewrite());

        MultiMatchQuery protoScoringBoolean = MultiMatchQuery.newBuilder()
            .setQuery("test")
            .addFields("field1")
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_SCORING_BOOLEAN)
            .build();
        assertEquals("scoring_boolean", fromProto(protoScoringBoolean).fuzzyRewrite());

        MultiMatchQuery protoTopTermsN = MultiMatchQuery.newBuilder()
            .setQuery("test")
            .addFields("field1")
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_TOP_TERMS_N)
            .build();
        assertEquals("top_terms_n", fromProto(protoTopTermsN).fuzzyRewrite());

        MultiMatchQuery protoTopTermsBlended = MultiMatchQuery.newBuilder()
            .setQuery("test")
            .addFields("field1")
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_TOP_TERMS_BLENDED_FREQS_N)
            .build();
        assertEquals("top_terms_blended_freqs_n", fromProto(protoTopTermsBlended).fuzzyRewrite());

        MultiMatchQuery protoTopTermsBoost = MultiMatchQuery.newBuilder()
            .setQuery("test")
            .addFields("field1")
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_TOP_TERMS_BOOST_N)
            .build();
        assertEquals("top_terms_boost_n", fromProto(protoTopTermsBoost).fuzzyRewrite());
    }

    public void testFromProtoWithoutFuzzyRewrite() {
        // Test that missing fuzzyRewrite field results in null
        MultiMatchQuery proto = MultiMatchQuery.newBuilder().setQuery("test query").addFields("field1").build();

        MultiMatchQueryBuilder builder = fromProto(proto);

        // Verify fuzzyRewrite is null when not set
        assertNull("FuzzyRewrite should be null when not set", builder.fuzzyRewrite());
    }
}
