/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.WildcardQuery;
import org.opensearch.test.OpenSearchTestCase;

public class WildcardQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private WildcardQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new WildcardQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals("Converter should handle WILDCARD case", QueryContainer.QueryContainerCase.WILDCARD, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        // Create a QueryContainer with WildcardQuery
        WildcardQuery wildcardQuery = WildcardQuery.newBuilder()
            .setField("test_field")
            .setValue("test*pattern")
            .setBoost(2.0f)
            .setXName("test_wildcard_query")
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setWildcard(wildcardQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a WildcardQueryBuilder", queryBuilder instanceof WildcardQueryBuilder);
        WildcardQueryBuilder wildcardQueryBuilder = (WildcardQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test_field", wildcardQueryBuilder.fieldName());
        assertEquals("Value should match", "test*pattern", wildcardQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, wildcardQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_wildcard_query", wildcardQueryBuilder.queryName());
    }

    public void testFromProtoWithMinimalFields() {
        // Create a QueryContainer with minimal WildcardQuery
        WildcardQuery wildcardQuery = WildcardQuery.newBuilder().setField("field_name").setValue("pattern*").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setWildcard(wildcardQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a WildcardQueryBuilder", queryBuilder instanceof WildcardQueryBuilder);
        WildcardQueryBuilder wildcardQueryBuilder = (WildcardQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "field_name", wildcardQueryBuilder.fieldName());
        assertEquals("Value should match", "pattern*", wildcardQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, wildcardQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", wildcardQueryBuilder.queryName());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Wildcard query'",
            exception.getMessage().contains("does not contain a Wildcard query")
        );
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception with null input
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention null",
            exception.getMessage().contains("null") || exception.getMessage().contains("does not contain a Wildcard query")
        );
    }
}
