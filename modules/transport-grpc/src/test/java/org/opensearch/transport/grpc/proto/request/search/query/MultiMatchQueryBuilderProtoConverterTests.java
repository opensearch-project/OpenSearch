/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.MultiMatchQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class MultiMatchQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private MultiMatchQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MultiMatchQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals(
            "Converter should handle MULTI_MATCH case",
            QueryContainer.QueryContainerCase.MULTI_MATCH,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        // Create a QueryContainer with MultiMatchQuery
        MultiMatchQuery multiMatchQuery = MultiMatchQuery.newBuilder()
            .setQuery("search text")
            .addFields("field1")
            .addFields("field2")
            .setBoost(2.0f)
            .setXName("test_multi_match_query")
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMultiMatch(multiMatchQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MultiMatchQueryBuilder", queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;
        assertEquals("Query text should match", "search text", multiMatchQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, multiMatchQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_multi_match_query", multiMatchQueryBuilder.queryName());
        assertTrue("Should contain field1", multiMatchQueryBuilder.fields().containsKey("field1"));
        assertTrue("Should contain field2", multiMatchQueryBuilder.fields().containsKey("field2"));
    }

    public void testFromProtoWithMinimalFields() {
        // Create a QueryContainer with minimal MultiMatchQuery
        MultiMatchQuery multiMatchQuery = MultiMatchQuery.newBuilder().setQuery("test").addFields("field").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMultiMatch(multiMatchQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MultiMatchQueryBuilder", queryBuilder instanceof MultiMatchQueryBuilder);
        MultiMatchQueryBuilder multiMatchQueryBuilder = (MultiMatchQueryBuilder) queryBuilder;
        assertEquals("Query text should match", "test", multiMatchQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, multiMatchQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", multiMatchQueryBuilder.queryName());
        assertTrue("Should contain field", multiMatchQueryBuilder.fields().containsKey("field"));
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a MultiMatch query'",
            exception.getMessage().contains("does not contain a MultiMatch query")
        );
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception with null input
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention null",
            exception.getMessage().contains("null") || exception.getMessage().contains("does not contain a MultiMatch query")
        );
    }
}
