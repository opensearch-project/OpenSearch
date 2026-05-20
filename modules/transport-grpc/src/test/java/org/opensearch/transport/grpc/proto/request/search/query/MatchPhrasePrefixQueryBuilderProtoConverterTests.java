/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.MatchPhrasePrefixQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class MatchPhrasePrefixQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private MatchPhrasePrefixQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MatchPhrasePrefixQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals(
            "Converter should handle MATCH_PHRASE_PREFIX case",
            QueryContainer.QueryContainerCase.MATCH_PHRASE_PREFIX,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        MatchPhrasePrefixQuery matchPhrasePrefixQuery = MatchPhrasePrefixQuery.newBuilder()
            .setField("message")
            .setQuery("quick bro")
            .setSlop(2)
            .setMaxExpansions(10)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchPhrasePrefix(matchPhrasePrefixQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchPhrasePrefixQueryBuilder", queryBuilder instanceof MatchPhrasePrefixQueryBuilder);

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = (MatchPhrasePrefixQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "message", matchPhrasePrefixQueryBuilder.fieldName());
        assertEquals("Value should match", "quick bro", matchPhrasePrefixQueryBuilder.value());
        assertEquals("Slop should match", 2, matchPhrasePrefixQueryBuilder.slop());
        assertEquals("Max expansions should match", 10, matchPhrasePrefixQueryBuilder.maxExpansions());
        assertEquals("Boost should match", 2.0f, matchPhrasePrefixQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", matchPhrasePrefixQueryBuilder.queryName());
    }

    public void testFromProtoWithMinimalFields() {
        MatchPhrasePrefixQuery matchPhrasePrefixQuery = MatchPhrasePrefixQuery.newBuilder().setField("title").setQuery("test").build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchPhrasePrefix(matchPhrasePrefixQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchPhrasePrefixQueryBuilder", queryBuilder instanceof MatchPhrasePrefixQueryBuilder);

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = (MatchPhrasePrefixQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "title", matchPhrasePrefixQueryBuilder.fieldName());
        assertEquals("Value should match", "test", matchPhrasePrefixQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, matchPhrasePrefixQueryBuilder.boost(), 0.0f);
    }

    public void testFromProtoWithInvalidContainer() {
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        assertTrue(
            "Exception message should mention 'does not contain a MatchPhrasePrefix query'",
            exception.getMessage().contains("does not contain a MatchPhrasePrefix query")
        );
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertTrue(
            "Exception message should mention 'does not contain a MatchPhrasePrefix query'",
            exception.getMessage().contains("does not contain a MatchPhrasePrefix query")
        );
    }
}
