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

    public void testFromProtoWithFuzzinessInt() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        org.opensearch.protobufs.Fuzziness fuzziness = org.opensearch.protobufs.Fuzziness.newBuilder().setInt32(2).build();

        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setFuzziness(fuzziness)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Fuzziness should match", Fuzziness.fromEdits(2), matchQueryBuilder.fuzziness());
    }

    public void testFromProtoWithZeroTermsQueryNone() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_NONE)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals("Zero terms query should match", MatchQuery.ZeroTermsQuery.NONE, matchQueryBuilder.zeroTermsQuery());
    }

    public void testFromProtoWithZeroTermsQueryUnspecified() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        org.opensearch.protobufs.MatchQuery matchQuery = org.opensearch.protobufs.MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_UNSPECIFIED)
            .build();

        MatchQueryBuilder matchQueryBuilder = MatchQueryBuilderProtoUtils.fromProto(matchQuery);

        assertNotNull("MatchQueryBuilder should not be null", matchQueryBuilder);
        assertEquals(
            "Zero terms query should be default when UNSPECIFIED",
            MatchQuery.DEFAULT_ZERO_TERMS_QUERY,
            matchQueryBuilder.zeroTermsQuery()
        );
    }
}
