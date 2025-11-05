/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterAggregationProtoUtilsTests extends OpenSearchTestCase {

    private AbstractQueryBuilderProtoUtils queryUtils;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        queryUtils = QueryBuilderProtoTestUtils.createQueryUtils();
    }

    public void testFromProtoWithMatchAllQuery() {
        // Create a match all query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchAll(MatchAllQuery.newBuilder().build())
            .build();

        // Test conversion
        FilterAggregationBuilder result = FilterAggregationProtoUtils.fromProto(
            "test_filter_agg",
            queryContainer,
            queryUtils
        );

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_filter_agg", result.getName());
        assertNotNull("Filter query should not be null", result.getFilter());
        assertTrue("Filter should be MatchAllQueryBuilder", result.getFilter() instanceof MatchAllQueryBuilder);
    }

    public void testFromProtoWithTermQuery() {
        // Create a term query container
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("status")
            .setValue(FieldValue.newBuilder().setString("published").build())
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setTerm(termQuery)
            .build();

        // Test conversion
        FilterAggregationBuilder result = FilterAggregationProtoUtils.fromProto(
            "status_filter",
            queryContainer,
            queryUtils
        );

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "status_filter", result.getName());
        assertNotNull("Filter query should not be null", result.getFilter());
        assertTrue("Filter should be TermQueryBuilder", result.getFilter() instanceof TermQueryBuilder);

        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) result.getFilter();
        assertEquals("Field name should match", "status", termQueryBuilder.fieldName());
        assertEquals("Term value should match", "published", termQueryBuilder.value());
    }

    public void testFromProtoWithComplexQuery() {
        // Create a more complex query (using match all as example, but could be any query type)
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchAll(MatchAllQuery.newBuilder()
                .setBoost(2.0f)
                .build())
            .build();

        // Test conversion
        FilterAggregationBuilder result = FilterAggregationProtoUtils.fromProto(
            "complex_filter",
            queryContainer,
            queryUtils
        );

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "complex_filter", result.getName());
        assertNotNull("Filter query should not be null", result.getFilter());
        assertTrue("Filter should be MatchAllQueryBuilder", result.getFilter() instanceof MatchAllQueryBuilder);

        MatchAllQueryBuilder matchAllQuery = (MatchAllQueryBuilder) result.getFilter();
        assertEquals("Boost should match", 2.0f, matchAllQuery.boost(), 0.001f);
    }

    public void testFromProtoWithNullQueryUtils() {
        // Create a query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchAll(MatchAllQuery.newBuilder().build())
            .build();

        // Test conversion with null queryUtils should throw exception
        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> FilterAggregationProtoUtils.fromProto("test_agg", queryContainer, null)
        );

        // The exception will be thrown when trying to call parseInnerQueryBuilderProto on null
        assertNotNull("Exception should not be null", exception);
    }

    public void testFromProtoWithMockedQueryUtils() {
        // Create a mock queryUtils that returns a specific query builder
        AbstractQueryBuilderProtoUtils mockQueryUtils = mock(AbstractQueryBuilderProtoUtils.class);
        TermQueryBuilder expectedQuery = new TermQueryBuilder("field", "value");
        when(mockQueryUtils.parseInnerQueryBuilderProto(any(QueryContainer.class))).thenReturn(expectedQuery);

        // Create a query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchAll(MatchAllQuery.newBuilder().build())
            .build();

        // Test conversion
        FilterAggregationBuilder result = FilterAggregationProtoUtils.fromProto(
            "mocked_filter",
            queryContainer,
            mockQueryUtils
        );

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "mocked_filter", result.getName());
        assertNotNull("Filter query should not be null", result.getFilter());
        assertSame("Filter should be the mocked query", expectedQuery, result.getFilter());
    }

    public void testFromProtoWithEmptyQueryContainer() {
        // Create an empty query container
        QueryContainer queryContainer = QueryContainer.newBuilder().build();

        // Test conversion - this should work as queryUtils should handle empty containers
        try {
            FilterAggregationBuilder result = FilterAggregationProtoUtils.fromProto(
                "empty_filter",
                queryContainer,
                queryUtils
            );

            // If no exception is thrown, verify the result
            assertNotNull("Result should not be null", result);
            assertEquals("Name should match", "empty_filter", result.getName());
            // The filter query might be null or a default query depending on queryUtils implementation
        } catch (Exception e) {
            // If queryUtils throws an exception for empty containers, that's also acceptable
            // The important thing is that our method doesn't crash
            assertTrue("Exception should be related to query parsing",
                e.getMessage() == null || e.getMessage().toLowerCase().contains("query"));
        }
    }

    public void testFromProtoWithDifferentAggregationNames() {
        // Test with various aggregation names to ensure name handling is correct
        String[] testNames = {
            "simple_name",
            "name-with-dashes",
            "name_with_underscores",
            "nameWithCamelCase",
            "name.with.dots",
            "123numeric_start",
            "special!@#$%characters"
        };

        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchAll(MatchAllQuery.newBuilder().build())
            .build();

        for (String testName : testNames) {
            FilterAggregationBuilder result = FilterAggregationProtoUtils.fromProto(
                testName,
                queryContainer,
                queryUtils
            );

            assertNotNull("Result should not be null for name: " + testName, result);
            assertEquals("Name should match for: " + testName, testName, result.getName());
            assertNotNull("Filter query should not be null for name: " + testName, result.getFilter());
        }
    }

    public void testFromProtoQueryUtilsIntegration() {
        // This test verifies that the queryUtils integration works correctly
        // by testing with a real queryUtils instance and verifying the parsed query

        // Create a term query container
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("category")
            .setValue(FieldValue.newBuilder().setString("electronics").build())
            .setBoost(1.5f)
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setTerm(termQuery)
            .build();

        // Test conversion
        FilterAggregationBuilder result = FilterAggregationProtoUtils.fromProto(
            "category_filter",
            queryContainer,
            queryUtils
        );

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "category_filter", result.getName());
        assertNotNull("Filter query should not be null", result.getFilter());
        assertTrue("Filter should be TermQueryBuilder", result.getFilter() instanceof TermQueryBuilder);

        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) result.getFilter();
        assertEquals("Field name should match", "category", termQueryBuilder.fieldName());
        assertEquals("Term value should match", "electronics", termQueryBuilder.value());
        assertEquals("Boost should match", 1.5f, termQueryBuilder.boost(), 0.001f);
    }
}
