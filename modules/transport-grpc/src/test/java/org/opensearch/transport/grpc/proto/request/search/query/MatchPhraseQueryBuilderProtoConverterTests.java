/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.protobufs.MatchPhraseQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.ZeroTermsQuery;
import org.opensearch.test.OpenSearchTestCase;

public class MatchPhraseQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private MatchPhraseQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MatchPhraseQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals(
            "Converter should handle MATCH_PHRASE case",
            QueryContainer.QueryContainerCase.MATCH_PHRASE,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        // Create a QueryContainer with MatchPhraseQuery
        MatchPhraseQuery matchPhraseQuery = MatchPhraseQuery.newBuilder()
            .setField("message")
            .setQuery("hello world")
            .setAnalyzer("standard")
            .setSlop(2)
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_ALL)
            .setBoost(2.0f)
            .setXName("test_match_phrase_query")
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchPhrase(matchPhraseQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchPhraseQueryBuilder", queryBuilder instanceof MatchPhraseQueryBuilder);
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = (MatchPhraseQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "message", matchPhraseQueryBuilder.fieldName());
        assertEquals("Query value should match", "hello world", matchPhraseQueryBuilder.value());
        assertEquals("Analyzer should match", "standard", matchPhraseQueryBuilder.analyzer());
        assertEquals("Slop should match", 2, matchPhraseQueryBuilder.slop());
        assertEquals("Zero terms query should match", MatchQuery.ZeroTermsQuery.ALL, matchPhraseQueryBuilder.zeroTermsQuery());
        assertEquals("Boost should match", 2.0f, matchPhraseQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_match_phrase_query", matchPhraseQueryBuilder.queryName());
    }

    public void testFromProtoWithMinimalFields() {
        // Create a QueryContainer with minimal MatchPhraseQuery
        MatchPhraseQuery matchPhraseQuery = MatchPhraseQuery.newBuilder().setField("title").setQuery("test phrase").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchPhrase(matchPhraseQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchPhraseQueryBuilder", queryBuilder instanceof MatchPhraseQueryBuilder);
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = (MatchPhraseQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "title", matchPhraseQueryBuilder.fieldName());
        assertEquals("Query value should match", "test phrase", matchPhraseQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, matchPhraseQueryBuilder.boost(), 0.0f);
        assertEquals("Default slop should match", MatchQuery.DEFAULT_PHRASE_SLOP, matchPhraseQueryBuilder.slop());
        assertEquals(
            "Default zero terms query should match",
            MatchQuery.DEFAULT_ZERO_TERMS_QUERY,
            matchPhraseQueryBuilder.zeroTermsQuery()
        );
        assertNull("Query name should be null", matchPhraseQueryBuilder.queryName());
        assertNull("Analyzer should be null", matchPhraseQueryBuilder.analyzer());
    }

    public void testFromProtoWithZeroTermsQueryNone() {
        // Create a QueryContainer with ZeroTermsQuery set to NONE
        MatchPhraseQuery matchPhraseQuery = MatchPhraseQuery.newBuilder()
            .setField("content")
            .setQuery("example text")
            .setZeroTermsQuery(ZeroTermsQuery.ZERO_TERMS_QUERY_NONE)
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchPhrase(matchPhraseQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchPhraseQueryBuilder", queryBuilder instanceof MatchPhraseQueryBuilder);
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = (MatchPhraseQueryBuilder) queryBuilder;
        assertEquals("Zero terms query should be NONE", MatchQuery.ZeroTermsQuery.NONE, matchPhraseQueryBuilder.zeroTermsQuery());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a MatchPhrase query'",
            exception.getMessage().contains("does not contain a MatchPhrase query")
        );
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception with null input
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention null or MatchPhrase query",
            exception.getMessage().contains("null") || exception.getMessage().contains("does not contain a MatchPhrase query")
        );
    }

    public void testFromProtoWithInvalidMatchPhraseQuery() {
        // Create a QueryContainer with an invalid MatchPhraseQuery (empty field)
        MatchPhraseQuery matchPhraseQuery = MatchPhraseQuery.newBuilder().setField("").setQuery("test").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchPhrase(matchPhraseQuery).build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention empty field name",
            exception.getMessage().contains("Field name cannot be null or empty")
        );
    }

    public void testFromProtoWithInvalidQuery() {
        // Create a QueryContainer with an invalid MatchPhraseQuery (empty query)
        MatchPhraseQuery matchPhraseQuery = MatchPhraseQuery.newBuilder().setField("message").setQuery("").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchPhrase(matchPhraseQuery).build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention empty query value",
            exception.getMessage().contains("Query value cannot be null or empty")
        );
    }

    public void testFromProtoWithNegativeSlop() {
        // Create a QueryContainer with an invalid MatchPhraseQuery (negative slop)
        MatchPhraseQuery matchPhraseQuery = MatchPhraseQuery.newBuilder().setField("message").setQuery("test phrase").setSlop(-1).build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchPhrase(matchPhraseQuery).build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        // Verify the exception message
        assertTrue("Exception message should mention negative slop", exception.getMessage().contains("No negative slop allowed"));
    }
}
