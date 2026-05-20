/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.lucene.search.FuzzyQuery;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.protobufs.MatchBoolPrefixQuery;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.test.OpenSearchTestCase;

public class MatchBoolPrefixQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithBasicMatchBoolPrefixQuery() {
        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder().setField("message").setQuery("quick brown f").build();

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(matchBoolPrefixQuery);

        assertNotNull("MatchBoolPrefixQueryBuilder should not be null", matchBoolPrefixQueryBuilder);
        assertEquals("Field name should match", "message", matchBoolPrefixQueryBuilder.fieldName());
        assertEquals("Query should match", "quick brown f", matchBoolPrefixQueryBuilder.value());
        assertEquals("Operator should be default OR", Operator.OR, matchBoolPrefixQueryBuilder.operator());
    }

    public void testFromProtoWithAllParameters() {
        MinimumShouldMatch minimumShouldMatch = MinimumShouldMatch.newBuilder().setString("75%").build();
        org.opensearch.protobufs.Fuzziness fuzziness = org.opensearch.protobufs.Fuzziness.newBuilder().setString("AUTO").build();

        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("quick brown f")
            .setAnalyzer("standard")
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_AND)
            .setMinimumShouldMatch(minimumShouldMatch)
            .setFuzziness(fuzziness)
            .setPrefixLength(2)
            .setMaxExpansions(100)
            .setFuzzyTranspositions(false)
            .setBoost(1.5f)
            .setXName("test_query")
            .build();

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(matchBoolPrefixQuery);

        assertNotNull("MatchBoolPrefixQueryBuilder should not be null", matchBoolPrefixQueryBuilder);
        assertEquals("Field name should match", "message", matchBoolPrefixQueryBuilder.fieldName());
        assertEquals("Query should match", "quick brown f", matchBoolPrefixQueryBuilder.value());
        assertEquals("Analyzer should match", "standard", matchBoolPrefixQueryBuilder.analyzer());
        assertEquals("Operator should match", Operator.AND, matchBoolPrefixQueryBuilder.operator());
        assertEquals("Minimum should match", "75%", matchBoolPrefixQueryBuilder.minimumShouldMatch());
        assertEquals("Fuzziness should match", Fuzziness.AUTO, matchBoolPrefixQueryBuilder.fuzziness());
        assertEquals("Prefix length should match", 2, matchBoolPrefixQueryBuilder.prefixLength());
        assertEquals("Max expansions should match", 100, matchBoolPrefixQueryBuilder.maxExpansions());
        assertEquals("Fuzzy transpositions should match", false, matchBoolPrefixQueryBuilder.fuzzyTranspositions());
        assertEquals("Boost should match", 1.5f, matchBoolPrefixQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_query", matchBoolPrefixQueryBuilder.queryName());
    }

    public void testFromProtoWithOperatorOr() {
        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_OR)
            .build();

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(matchBoolPrefixQuery);

        assertNotNull("MatchBoolPrefixQueryBuilder should not be null", matchBoolPrefixQueryBuilder);
        assertEquals("Operator should match OR", Operator.OR, matchBoolPrefixQueryBuilder.operator());
    }

    public void testFromProtoWithOperatorUnspecified() {
        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setOperator(org.opensearch.protobufs.Operator.OPERATOR_UNSPECIFIED)
            .build();

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(matchBoolPrefixQuery);

        assertNotNull("MatchBoolPrefixQueryBuilder should not be null", matchBoolPrefixQueryBuilder);
        assertEquals("Operator should be default OR", Operator.OR, matchBoolPrefixQueryBuilder.operator());
    }

    public void testFromProtoWithMinimumShouldMatch() {
        // Test 1: MinimumShouldMatch with string value
        MinimumShouldMatch minimumShouldMatchString = MinimumShouldMatch.newBuilder().setString("75%").build();
        MatchBoolPrefixQuery queryString = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setMinimumShouldMatch(minimumShouldMatchString)
            .build();
        MatchBoolPrefixQueryBuilder builderString = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(queryString);
        assertNotNull("Builder should not be null (STRING)", builderString);
        assertEquals("Minimum should match (STRING)", "75%", builderString.minimumShouldMatch());

        // Test 2: MinimumShouldMatch with int32 value
        MinimumShouldMatch minimumShouldMatchInt = MinimumShouldMatch.newBuilder().setInt32(2).build();
        MatchBoolPrefixQuery queryInt = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setMinimumShouldMatch(minimumShouldMatchInt)
            .build();
        MatchBoolPrefixQueryBuilder builderInt = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(queryInt);
        assertNotNull("Builder should not be null (INT32)", builderInt);
        assertEquals("Minimum should match (INT32)", "2", builderInt.minimumShouldMatch());

        // Test 3: MinimumShouldMatch with empty/neither string nor int32
        MinimumShouldMatch minimumShouldMatchEmpty = MinimumShouldMatch.newBuilder().build();
        MatchBoolPrefixQuery queryEmpty = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setMinimumShouldMatch(minimumShouldMatchEmpty)
            .build();
        MatchBoolPrefixQueryBuilder builderEmpty = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(queryEmpty);
        assertNotNull("Builder should not be null (EMPTY)", builderEmpty);
        assertNull("Minimum should match should be null (EMPTY)", builderEmpty.minimumShouldMatch());
    }

    public void testFromProtoWithFuzziness() {
        // Test 1: Fuzziness with string value
        org.opensearch.protobufs.Fuzziness fuzzinessString = org.opensearch.protobufs.Fuzziness.newBuilder().setString("AUTO").build();
        MatchBoolPrefixQuery queryString = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setFuzziness(fuzzinessString)
            .build();
        MatchBoolPrefixQueryBuilder builderString = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(queryString);
        assertNotNull("Builder should not be null (STRING)", builderString);
        assertEquals("Fuzziness should match (STRING)", Fuzziness.AUTO, builderString.fuzziness());

        // Test 2: Fuzziness with int32 value
        org.opensearch.protobufs.Fuzziness fuzzinessInt = org.opensearch.protobufs.Fuzziness.newBuilder().setInt32(2).build();
        MatchBoolPrefixQuery queryInt = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setFuzziness(fuzzinessInt)
            .build();
        MatchBoolPrefixQueryBuilder builderInt = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(queryInt);
        assertNotNull("Builder should not be null (INT32)", builderInt);
        assertEquals("Fuzziness should match (INT32)", Fuzziness.fromEdits(2), builderInt.fuzziness());

        // Test 3: Fuzziness with empty/neither string nor int32
        org.opensearch.protobufs.Fuzziness fuzzinessEmpty = org.opensearch.protobufs.Fuzziness.newBuilder().build();
        MatchBoolPrefixQuery queryEmpty = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setFuzziness(fuzzinessEmpty)
            .build();
        MatchBoolPrefixQueryBuilder builderEmpty = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(queryEmpty);
        assertNotNull("Builder should not be null (EMPTY)", builderEmpty);
        assertNull("Fuzziness should be null (EMPTY)", builderEmpty.fuzziness());
    }

    public void testFromProtoWithDefaults() {
        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder().setField("message").setQuery("test").build();

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(matchBoolPrefixQuery);

        assertNotNull("MatchBoolPrefixQueryBuilder should not be null", matchBoolPrefixQueryBuilder);
        assertEquals("Prefix length should be default", FuzzyQuery.defaultPrefixLength, matchBoolPrefixQueryBuilder.prefixLength());
        assertEquals("Max expansions should be default", FuzzyQuery.defaultMaxExpansions, matchBoolPrefixQueryBuilder.maxExpansions());
        assertEquals(
            "Fuzzy transpositions should be default",
            FuzzyQuery.defaultTranspositions,
            matchBoolPrefixQueryBuilder.fuzzyTranspositions()
        );
    }

    public void testFromProtoWithFuzzyRewrite() {
        // Test 1: Fuzzy rewrite not set (null)
        MatchBoolPrefixQuery queryNoRewrite = MatchBoolPrefixQuery.newBuilder().setField("message").setQuery("test").build();
        MatchBoolPrefixQueryBuilder builderNoRewrite = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(queryNoRewrite);
        assertNotNull("Builder should not be null (no fuzzyRewrite)", builderNoRewrite);
        assertNull("Fuzzy rewrite should be null (not set)", builderNoRewrite.fuzzyRewrite());

        // Test 2: Fuzzy rewrite with CONSTANT_SCORE value
        MatchBoolPrefixQuery queryWithRewrite = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setFuzzyRewrite("constant_score")
            .build();
        MatchBoolPrefixQueryBuilder builderWithRewrite = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(queryWithRewrite);
        assertNotNull("Builder should not be null (CONSTANT_SCORE)", builderWithRewrite);
        assertNotNull("Fuzzy rewrite should not be null (CONSTANT_SCORE)", builderWithRewrite.fuzzyRewrite());
        assertEquals("Fuzzy rewrite should match (CONSTANT_SCORE)", "constant_score", builderWithRewrite.fuzzyRewrite());

    }

}
