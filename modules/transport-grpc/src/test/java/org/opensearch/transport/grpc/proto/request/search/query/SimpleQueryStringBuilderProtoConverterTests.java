/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.SimpleQueryStringQuery;
import org.opensearch.test.OpenSearchTestCase;

public class SimpleQueryStringBuilderProtoConverterTests extends OpenSearchTestCase {

    private SimpleQueryStringBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new SimpleQueryStringBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals(
            "Converter should handle SIMPLE_QUERY_STRING case",
            QueryContainer.QueryContainerCase.SIMPLE_QUERY_STRING,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        // Create a QueryContainer with SimpleQueryStringQuery
        SimpleQueryStringQuery simpleQueryString = SimpleQueryStringQuery.newBuilder()
            .setQuery("search text")
            .setBoost(2.0f)
            .setXName("test_query")
            .setAnalyzer("standard")
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setSimpleQueryString(simpleQueryString).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a SimpleQueryStringBuilder", queryBuilder instanceof SimpleQueryStringBuilder);
        SimpleQueryStringBuilder simpleQueryStringBuilder = (SimpleQueryStringBuilder) queryBuilder;
        assertEquals("Query text should match", "search text", simpleQueryStringBuilder.value());
        assertEquals("Boost should match", 2.0f, simpleQueryStringBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", simpleQueryStringBuilder.queryName());
        assertEquals("Analyzer should match", "standard", simpleQueryStringBuilder.analyzer());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a SimpleQueryString query'",
            exception.getMessage().contains("does not contain a SimpleQueryString query")
        );
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception with null container
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a SimpleQueryString query'",
            exception.getMessage().contains("does not contain a SimpleQueryString query")
        );
    }
}
