/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.BoolQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class BoolQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private BoolQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new BoolQueryBuilderProtoConverter();
        // Set up the registry for nested query conversion
        QueryBuilderProtoConverterRegistryImpl registry = new QueryBuilderProtoConverterRegistryImpl();
        converter.setRegistry(registry);
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals("Converter should handle BOOL case", QueryContainer.QueryContainerCase.BOOL, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        // Create a QueryContainer with BoolQuery
        BoolQuery boolQuery = BoolQuery.newBuilder().setBoost(2.0f).setXName("test_bool_query").setAdjustPureNegative(false).build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setBool(boolQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a BoolQueryBuilder", queryBuilder instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
        assertEquals("Boost should match", 2.0f, boolQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_bool_query", boolQueryBuilder.queryName());
        assertFalse("Adjust pure negative should match", boolQueryBuilder.adjustPureNegative());
    }

    public void testFromProtoWithMinimalFields() {
        // Create a QueryContainer with minimal BoolQuery
        BoolQuery boolQuery = BoolQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setBool(boolQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a BoolQueryBuilder", queryBuilder instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
        assertEquals("Default boost should be 1.0", 1.0f, boolQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", boolQueryBuilder.queryName());
        assertTrue("Default adjust pure negative should be true", boolQueryBuilder.adjustPureNegative());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Bool query'",
            exception.getMessage().contains("does not contain a Bool query")
        );
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception with null input
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention null",
            exception.getMessage().contains("null") || exception.getMessage().contains("does not contain a Bool query")
        );
    }
}
