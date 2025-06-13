/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class QueryBuilderProtoConverterRegistryTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Create an empty registry for testing
        registry = new EmptyQueryBuilderProtoConverterRegistry();
    }

    public void testRegisterAndConvertQuery() {
        // Create a converter
        MatchAllQueryBuilderProtoConverter converter = new MatchAllQueryBuilderProtoConverter();

        // Register the converter
        registry.registerConverter(converter);

        // Create a QueryContainer with MatchAllQuery
        MatchAllQuery matchAllQuery = MatchAllQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(matchAllQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchAllQueryBuilder", queryBuilder instanceof MatchAllQueryBuilder);
    }

    public void testConvertQueryForUnregisteredType() {
        // Create a QueryContainer with MatchAllQuery (no converter registered)
        MatchAllQuery matchAllQuery = MatchAllQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(matchAllQuery).build();

        // Convert the query, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> registry.fromProto(queryContainer));

        // Verify the exception message
        assertTrue("Exception message should mention 'Unsupported query type'", exception.getMessage().contains("Unsupported query type"));
    }

    public void testFromProto() {
        // Register a converter
        registry.registerConverter(new MatchAllQueryBuilderProtoConverter());

        // Create a QueryContainer with MatchAllQuery
        MatchAllQuery matchAllQuery = MatchAllQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(matchAllQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = registry.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchAllQueryBuilder", queryBuilder instanceof MatchAllQueryBuilder);
    }

    public void testFromProtoWithUnregisteredType() {
        // Create a QueryContainer with MatchAllQuery (no converter registered)
        MatchAllQuery matchAllQuery = MatchAllQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(matchAllQuery).build();

        // Convert the query, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> registry.fromProto(queryContainer));

        // Verify the exception message
        assertTrue("Exception message should mention 'Unsupported query type'", exception.getMessage().contains("Unsupported query type"));
    }
}
