/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.ExistsQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class ExistsQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private ExistsQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new ExistsQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals("Converter should handle EXISTS case", QueryContainer.QueryContainerCase.EXISTS, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        // Create a QueryContainer with ExistsQuery
        ExistsQuery existsQuery = ExistsQuery.newBuilder().setField("test_field").setBoost(2.0f).setXName("test_exists_query").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setExists(existsQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be an ExistsQueryBuilder", queryBuilder instanceof ExistsQueryBuilder);
        ExistsQueryBuilder existsQueryBuilder = (ExistsQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test_field", existsQueryBuilder.fieldName());
        assertEquals("Boost should match", 2.0f, existsQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_exists_query", existsQueryBuilder.queryName());
    }

    public void testFromProtoWithMinimalFields() {
        // Create a QueryContainer with minimal ExistsQuery
        ExistsQuery existsQuery = ExistsQuery.newBuilder().setField("field_name").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setExists(existsQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be an ExistsQueryBuilder", queryBuilder instanceof ExistsQueryBuilder);
        ExistsQueryBuilder existsQueryBuilder = (ExistsQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "field_name", existsQueryBuilder.fieldName());
        assertEquals("Default boost should be 1.0", 1.0f, existsQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", existsQueryBuilder.queryName());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain an Exists query'",
            exception.getMessage().contains("does not contain an Exists query")
        );
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception with null input
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention null",
            exception.getMessage().contains("null") || exception.getMessage().contains("does not contain an Exists query")
        );
    }
}
