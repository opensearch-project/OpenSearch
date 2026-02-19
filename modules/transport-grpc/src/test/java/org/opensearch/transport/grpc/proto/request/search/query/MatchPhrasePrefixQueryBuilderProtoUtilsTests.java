/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.lucene.search.FuzzyQuery;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.protobufs.MatchPhrasePrefixQuery;
import org.opensearch.protobufs.ZeroTermsQuery;
import org.opensearch.test.OpenSearchTestCase;

public class MatchPhrasePrefixQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithBasicMatchPhrasePrefixQuery() {
        MatchPhrasePrefixQuery matchPhrasePrefixQuery = MatchPhrasePrefixQuery.newBuilder()
            .setField("message")
            .setQuery("quick bro")
            .build();

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = MatchPhrasePrefixQueryBuilderProtoUtils.fromProto(
            matchPhrasePrefixQuery
        );

        assertNotNull("MatchPhrasePrefixQueryBuilder should not be null", matchPhrasePrefixQueryBuilder);
        assertEquals("Field name should match", "message", matchPhrasePrefixQueryBuilder.fieldName());
        assertEquals("Query should match", "quick bro", matchPhrasePrefixQueryBuilder.value());
        assertEquals("Slop should be default", MatchQuery.DEFAULT_PHRASE_SLOP, matchPhrasePrefixQueryBuilder.slop());
        assertEquals("Max expansions should be default", FuzzyQuery.defaultMaxExpansions, matchPhrasePrefixQueryBuilder.maxExpansions());
        assertEquals(
            "Zero terms query should be default",
            MatchQuery.DEFAULT_ZERO_TERMS_QUERY,
            matchPhrasePrefixQueryBuilder.zeroTermsQuery()
        );
    }

    public void testFromProtoWithAllParameters() {
        MatchPhrasePrefixQuery matchPhrasePrefixQuery = MatchPhrasePrefixQuery.newBuilder()
            .setField("message")
            .setQuery("quick bro")
            .setAnalyzer("standard")
            .setSlop(2)
            .setMaxExpansions(10)
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_ALL)
            .setBoost(1.5f)
            .setXName("test_query")
            .build();

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = MatchPhrasePrefixQueryBuilderProtoUtils.fromProto(
            matchPhrasePrefixQuery
        );

        assertNotNull("MatchPhrasePrefixQueryBuilder should not be null", matchPhrasePrefixQueryBuilder);
        assertEquals("Field name should match", "message", matchPhrasePrefixQueryBuilder.fieldName());
        assertEquals("Query should match", "quick bro", matchPhrasePrefixQueryBuilder.value());
        assertEquals("Analyzer should match", "standard", matchPhrasePrefixQueryBuilder.analyzer());
        assertEquals("Slop should match", 2, matchPhrasePrefixQueryBuilder.slop());
        assertEquals("Max expansions should match", 10, matchPhrasePrefixQueryBuilder.maxExpansions());
        assertEquals("Zero terms query should match", MatchQuery.ZeroTermsQuery.ALL, matchPhrasePrefixQueryBuilder.zeroTermsQuery());
        assertEquals("Boost should match", 1.5f, matchPhrasePrefixQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_query", matchPhrasePrefixQueryBuilder.queryName());
    }

    public void testFromProtoWithZeroTermsQueryNone() {
        MatchPhrasePrefixQuery matchPhrasePrefixQuery = MatchPhrasePrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_NONE)
            .build();

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = MatchPhrasePrefixQueryBuilderProtoUtils.fromProto(
            matchPhrasePrefixQuery
        );

        assertNotNull("MatchPhrasePrefixQueryBuilder should not be null", matchPhrasePrefixQueryBuilder);
        assertEquals("Zero terms query should match", MatchQuery.ZeroTermsQuery.NONE, matchPhrasePrefixQueryBuilder.zeroTermsQuery());
    }

    public void testFromProtoWithZeroTermsQueryUnspecified() {
        MatchPhrasePrefixQuery matchPhrasePrefixQuery = MatchPhrasePrefixQuery.newBuilder()
            .setField("message")
            .setQuery("test")
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_UNSPECIFIED)
            .build();

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = MatchPhrasePrefixQueryBuilderProtoUtils.fromProto(
            matchPhrasePrefixQuery
        );

        assertNotNull("MatchPhrasePrefixQueryBuilder should not be null", matchPhrasePrefixQueryBuilder);
        assertEquals(
            "Zero terms query should be default when UNSPECIFIED",
            MatchQuery.DEFAULT_ZERO_TERMS_QUERY,
            matchPhrasePrefixQueryBuilder.zeroTermsQuery()
        );
    }

    public void testFromProtoWithIntegerQueryValue() {
        MatchPhrasePrefixQuery matchPhrasePrefixQuery = MatchPhrasePrefixQuery.newBuilder().setField("age").setQuery("42").build();

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = MatchPhrasePrefixQueryBuilderProtoUtils.fromProto(
            matchPhrasePrefixQuery
        );

        assertNotNull("MatchPhrasePrefixQueryBuilder should not be null", matchPhrasePrefixQueryBuilder);
        assertEquals("Field name should match", "age", matchPhrasePrefixQueryBuilder.fieldName());
        assertEquals("Query value should match", "42", matchPhrasePrefixQueryBuilder.value());
    }
}
