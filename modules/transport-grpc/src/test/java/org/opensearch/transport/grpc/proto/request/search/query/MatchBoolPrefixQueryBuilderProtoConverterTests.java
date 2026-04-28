/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.MatchBoolPrefixQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class MatchBoolPrefixQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private MatchBoolPrefixQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MatchBoolPrefixQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals(
            "Converter should handle MATCH_BOOL_PREFIX case",
            QueryContainer.QueryContainerCase.MATCH_BOOL_PREFIX,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder()
            .setField("message")
            .setQuery("quick brown f")
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchBoolPrefix(matchBoolPrefixQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchBoolPrefixQueryBuilder", queryBuilder instanceof MatchBoolPrefixQueryBuilder);

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = (MatchBoolPrefixQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "message", matchBoolPrefixQueryBuilder.fieldName());
        assertEquals("Value should match", "quick brown f", matchBoolPrefixQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, matchBoolPrefixQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", matchBoolPrefixQueryBuilder.queryName());
    }

    public void testFromProtoWithMinimalFields() {
        MatchBoolPrefixQuery matchBoolPrefixQuery = MatchBoolPrefixQuery.newBuilder().setField("title").setQuery("test").build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchBoolPrefix(matchBoolPrefixQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchBoolPrefixQueryBuilder", queryBuilder instanceof MatchBoolPrefixQueryBuilder);

        MatchBoolPrefixQueryBuilder matchBoolPrefixQueryBuilder = (MatchBoolPrefixQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "title", matchBoolPrefixQueryBuilder.fieldName());
        assertEquals("Value should match", "test", matchBoolPrefixQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, matchBoolPrefixQueryBuilder.boost(), 0.0f);
    }

    public void testFromProtoWithInvalidContainer() {
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        assertTrue(
            "Exception message should mention 'does not contain a MatchBoolPrefix query'",
            exception.getMessage().contains("does not contain a MatchBoolPrefix query")
        );
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertTrue(
            "Exception message should mention 'does not contain a MatchBoolPrefix query'",
            exception.getMessage().contains("does not contain a MatchBoolPrefix query")
        );
    }
}
