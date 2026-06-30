/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class MatchQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private MatchQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MatchQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals("Converter should handle MATCH case", QueryContainer.QueryContainerCase.MATCH, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        FieldValue queryValue = FieldValue.newBuilder().setString("search text").build();
        MatchQuery matchQuery = MatchQuery.newBuilder()
            .setField("message")
            .setQuery(queryValue)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setMatch(matchQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchQueryBuilder", queryBuilder instanceof MatchQueryBuilder);

        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "message", matchQueryBuilder.fieldName());
        assertEquals("Value should match", "search text", matchQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, matchQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", matchQueryBuilder.queryName());
    }

    public void testFromProtoWithMinimalFields() {
        FieldValue queryValue = FieldValue.newBuilder().setString("test").build();
        MatchQuery matchQuery = MatchQuery.newBuilder().setField("title").setQuery(queryValue).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setMatch(matchQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchQueryBuilder", queryBuilder instanceof MatchQueryBuilder);

        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "title", matchQueryBuilder.fieldName());
        assertEquals("Value should match", "test", matchQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, matchQueryBuilder.boost(), 0.0f);
    }

    public void testFromProtoWithInvalidContainer() {
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        assertTrue(
            "Exception message should mention 'does not contain a Match query'",
            exception.getMessage().contains("does not contain a Match query")
        );
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertTrue(
            "Exception message should mention 'does not contain a Match query'",
            exception.getMessage().contains("does not contain a Match query")
        );
    }
}
