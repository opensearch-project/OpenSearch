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

    public void testFromProtoWithMinimumShouldMatchInt() {
        MinimumShouldMatch minimumShouldMatch = MinimumShouldMatch.newBuilder().setInt32(2).build();

        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setMinimumShouldMatch(minimumShouldMatch)
            .build();

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(matchBoolPrefixQuery);

        assertNotNull("MatchBoolPrefixQueryBuilder should not be null", matchBoolPrefixQueryBuilder);
        assertEquals("Minimum should match", "2", matchBoolPrefixQueryBuilder.minimumShouldMatch());
    }

    public void testFromProtoWithFuzzinessInt() {
        org.opensearch.protobufs.Fuzziness fuzziness = org.opensearch.protobufs.Fuzziness.newBuilder().setInt32(2).build();

        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setFuzziness(fuzziness)
            .build();

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = MatchBoolPrefixQueryBuilderProtoUtils.fromProto(matchBoolPrefixQuery);

        assertNotNull("MatchBoolPrefixQueryBuilder should not be null", matchBoolPrefixQueryBuilder);
        assertEquals("Fuzziness should match", Fuzziness.fromEdits(2), matchBoolPrefixQueryBuilder.fuzziness());
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
}
