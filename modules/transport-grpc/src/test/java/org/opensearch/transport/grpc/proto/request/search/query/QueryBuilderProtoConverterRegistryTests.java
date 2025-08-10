/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Test class for QueryBuilderProtoConverterRegistry to verify the map-based optimization.
 */
public class QueryBuilderProtoConverterRegistryTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistry();
    }

    public void testMatchAllQueryConversion() {
        // Create a MatchAll query container
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals(
            "Should be a MatchAllQueryBuilder",
            "org.opensearch.index.query.MatchAllQueryBuilder",
            queryBuilder.getClass().getName()
        );
    }

    public void testTermQueryConversion() {
        // Create a Term query container
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setTerm(
                TermQuery.newBuilder().setField("test_field").setValue(FieldValue.newBuilder().setString("test_value").build()).build()
            )
            .build();

        // Convert using the registry
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertEquals("Should be a TermQueryBuilder", "org.opensearch.index.query.TermQueryBuilder", queryBuilder.getClass().getName());
    }

    public void testNullQueryContainer() {
        expectThrows(IllegalArgumentException.class, () -> registry.fromProto(null));
    }

    public void testUnsupportedQueryType() {
        // Create an empty query container (no query type set)
        QueryContainer queryContainer = QueryContainer.newBuilder().build();
        expectThrows(IllegalArgumentException.class, () -> registry.fromProto(queryContainer));
    }

    public void testConverterRegistration() {
        // Create a custom converter for testing
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.MATCH_ALL;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                // Return a mock QueryBuilder for testing
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        // Register the custom converter
        registry.registerConverter(customConverter);

        // Test that it works
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();

        QueryBuilder result = registry.fromProto(queryContainer);
        assertNotNull("Result should not be null", result);
    }

    public void testNullConverter() {
        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(null));
    }

    public void testNullHandledQueryCase() {
        // Create a custom converter that returns null for getHandledQueryCase
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return null;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(customConverter));
    }

    public void testNotSetHandledQueryCase() {
        // Create a custom converter that returns QUERYCONTAINER_NOT_SET for getHandledQueryCase
        QueryBuilderProtoConverter customConverter = new QueryBuilderProtoConverter() {
            @Override
            public QueryContainer.QueryContainerCase getHandledQueryCase() {
                return QueryContainer.QueryContainerCase.QUERYCONTAINER_NOT_SET;
            }

            @Override
            public QueryBuilder fromProto(QueryContainer queryContainer) {
                return new org.opensearch.index.query.MatchAllQueryBuilder();
            }
        };

        expectThrows(IllegalArgumentException.class, () -> registry.registerConverter(customConverter));
    }
}
