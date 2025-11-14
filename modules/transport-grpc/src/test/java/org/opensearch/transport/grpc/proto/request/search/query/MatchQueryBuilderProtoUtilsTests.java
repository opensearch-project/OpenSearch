/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.ZeroTermsQuery;
import org.opensearch.test.OpenSearchTestCase;

public class MatchQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithBasicMatchQuery() {
        FieldValue queryValue = FieldValue.newBuilder().setString("search text").build();
        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Field name should match", "message", matchQueryBuilder.fieldName());
        assertEquals("Query should match", "search text", matchQueryBuilder.value());
        assertEquals("Operator should be default OR", Operator.OR, matchQueryBuilder.operator());
    }

    public void testFromProtoWithAllParameters() {
        FieldValue queryValue = FieldValue.newBuilder().setString("search text").build();
        org.opensearch.protobufs.Fuzziness fuzziness = org.opensearch.protobufs.Fuzziness.newBuilder().setString("AUTO").build();

        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setAnalyzer("standard")
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_AND)
            .setFuzziness(fuzziness)
            .setPrefixLength(2)
            .setMaxExpansions(50)
            .setFuzzyTranspositions(false)
            .setLenient(true)
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_ALL)
            .setAutoGenerateSynonymsPhraseQuery(false)
            .setBoost(1.5f)
            .setXName("test_query")
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Field name should match", "message", matchQueryBuilder.fieldName());
        assertEquals("Query should match", "search text", matchQueryBuilder.value());
        assertEquals("Analyzer should match", "standard", matchQueryBuilder.analyzer());
        assertEquals("Operator should match", Operator.AND, matchQueryBuilder.operator());
        assertEquals("Fuzziness should match", Fuzziness.AUTO, matchQueryBuilder.fuzziness());
        assertEquals("Prefix length should match", 2, matchQueryBuilder.prefixLength());
        assertEquals("Max expansions should match", 50, matchQueryBuilder.maxExpansions());
        assertEquals("Fuzzy transpositions should match", false, matchQueryBuilder.fuzzyTranspositions());
        assertEquals("Lenient should match", true, matchQueryBuilder.lenient());
        assertEquals("Zero terms query should match", MatchQuery.ZeroTermsQuery.ALL, matchQueryBuilder.zeroTermsQuery());
        assertEquals("Auto generate synonyms should match", false, matchQueryBuilder.autoGenerateSynonymsPhraseQuery());
        assertEquals("Boost should match", 1.5f, matchQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_query", matchQueryBuilder.queryName());
    }

    public void testFromProtoWithOperatorOr() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_OR)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Operator should match OR", Operator.OR, matchQueryBuilder.operator());
    }

    public void testFromProtoWithOperatorUnspecified() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_UNSPECIFIED)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Operator should be default OR", Operator.OR, matchQueryBuilder.operator());
    }

    public void testFromProtoWithFuzziness() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();

        // Test 1: Fuzziness with string value
        org.opensearch.protobufs.Fuzziness fuzzinessString = org.opensearch.protobufs.Fuzziness.newBuilder().setString("AUTO").build();
        org.opensearch.protobufs.MatchQuery queryString = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setFuzziness(fuzzinessString)
            .build();
        MatchQueryBuilder builderString = MatchQueryBuilderProtoUtils.fromProto(queryString);
        assertNotNull("Builder should not be null (STRING)", builderString);
        assertEquals("Fuzziness should match (STRING)", Fuzziness.AUTO, builderString.fuzziness());

        // Test 2: Fuzziness with int32 value
        org.opensearch.protobufs.Fuzziness fuzzinessInt = org.opensearch.protobufs.Fuzziness.newBuilder().setInt32(2).build();
        org.opensearch.protobufs.MatchQuery queryInt = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setFuzziness(fuzzinessInt)
            .build();
        MatchQueryBuilder builderInt = MatchQueryBuilderProtoUtils.fromProto(queryInt);
        assertNotNull("Builder should not be null (INT32)", builderInt);
        assertEquals("Fuzziness should match (INT32)", Fuzziness.fromEdits(2), builderInt.fuzziness());

        // Test 3: Fuzziness with empty/neither string nor int32
        org.opensearch.protobufs.Fuzziness fuzzinessEmpty = org.opensearch.protobufs.Fuzziness.newBuilder().build();
        org.opensearch.protobufs.MatchQuery queryEmpty = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setFuzziness(fuzzinessEmpty)
            .build();
        MatchQueryBuilder builderEmpty = MatchQueryBuilderProtoUtils.fromProto(queryEmpty);
        assertNotNull("Builder should not be null (EMPTY)", builderEmpty);
        assertNull("Fuzziness should be null (EMPTY)", builderEmpty.fuzziness());
    }

    public void testFromProtoWithMinimumShouldMatch() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();

        // Test 1: MinimumShouldMatch with string value
        org.opensearch.protobufs.MinimumShouldMatch minimumShouldMatchString = org.opensearch.protobufs.MinimumShouldMatch.newBuilder()
            .setString("75%")
            .build();
        org.opensearch.protobufs.MatchQuery queryString = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setMinimumShouldMatch(minimumShouldMatchString)
            .build();
        MatchQueryBuilder builderString = MatchQueryBuilderProtoUtils.fromProto(queryString);
        assertNotNull("Builder should not be null (STRING)", builderString);
        assertEquals("Minimum should match (STRING)", "75%", builderString.minimumShouldMatch());

        // Test 2: MinimumShouldMatch with int32 value
        org.opensearch.protobufs.MinimumShouldMatch minimumShouldMatchInt = org.opensearch.protobufs.MinimumShouldMatch.newBuilder()
            .setInt32(2)
            .build();
        org.opensearch.protobufs.MatchQuery queryInt = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setMinimumShouldMatch(minimumShouldMatchInt)
            .build();
        MatchQueryBuilder builderInt = MatchQueryBuilderProtoUtils.fromProto(queryInt);
        assertNotNull("Builder should not be null (INT32)", builderInt);
        assertEquals("Minimum should match (INT32)", "2", builderInt.minimumShouldMatch());

        // Test 3: MinimumShouldMatch with empty/neither
        org.opensearch.protobufs.MinimumShouldMatch minimumShouldMatchEmpty = org.opensearch.protobufs.MinimumShouldMatch.newBuilder()
            .build();
        org.opensearch.protobufs.MatchQuery queryEmpty = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setMinimumShouldMatch(minimumShouldMatchEmpty)
            .build();
        MatchQueryBuilder builderEmpty = MatchQueryBuilderProtoUtils.fromProto(queryEmpty);
        assertNotNull("Builder should not be null (EMPTY)", builderEmpty);
        assertNull("Minimum should match should be null (EMPTY)", builderEmpty.minimumShouldMatch());
    }

    public void testFromProtoWithFuzzyRewrite() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();

        // Test 1: FuzzyRewrite with CONSTANT_SCORE value
        org.opensearch.protobufs.MatchQuery queryWithRewrite = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_CONSTANT_SCORE)
            .build();
        MatchQueryBuilder builderWithRewrite = MatchQueryBuilderProtoUtils.fromProto(queryWithRewrite);
        assertNotNull("Builder should not be null (CONSTANT_SCORE)", builderWithRewrite);
        assertNotNull("Fuzzy rewrite should not be null (CONSTANT_SCORE)", builderWithRewrite.fuzzyRewrite());
        assertEquals("Fuzzy rewrite should match (CONSTANT_SCORE)", "constant_score", builderWithRewrite.fuzzyRewrite());

        // Test 2: FuzzyRewrite with UNSPECIFIED value
        org.opensearch.protobufs.MatchQuery queryUnspecified = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setFuzzyRewrite(org.opensearch.protobufs.MultiTermQueryRewrite.MULTI_TERM_QUERY_REWRITE_UNSPECIFIED)
            .build();
        MatchQueryBuilder builderUnspecified = MatchQueryBuilderProtoUtils.fromProto(queryUnspecified);
        assertNotNull("Builder should not be null (UNSPECIFIED)", builderUnspecified);
        assertNull("Fuzzy rewrite should be null (UNSPECIFIED)", builderUnspecified.fuzzyRewrite());
    }

    public void testFromProtoWithZeroTermsQueryAll() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_ALL)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Zero terms query should match", MatchQuery.ZeroTermsQuery.ALL, matchQueryBuilder.zeroTermsQuery());
    }

    public void testFromProtoWithLenient() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setLenient(true)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Lenient should match", true, matchQueryBuilder.lenient());
    }

    public void testFromProtoWithAutoGenerateSynonymsPhraseQueryFalse() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setAutoGenerateSynonymsPhraseQuery(false)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Auto generate synonyms should match", false, matchQueryBuilder.autoGenerateSynonymsPhraseQuery());
    }

}
