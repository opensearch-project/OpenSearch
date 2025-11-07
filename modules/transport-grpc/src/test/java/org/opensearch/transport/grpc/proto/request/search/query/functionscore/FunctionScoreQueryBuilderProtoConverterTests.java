/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.protobufs.FunctionScoreQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;

/**
 * Tests for {@link FunctionScoreQueryBuilderProtoConverter}.
 */
public class FunctionScoreQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private FunctionScoreQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new FunctionScoreQueryBuilderProtoConverter();
        // Set up the registry with all built-in converters
        QueryBuilderProtoConverterRegistryImpl registry = new QueryBuilderProtoConverterRegistryImpl();
        converter.setRegistry(registry);
    }

    public void testGetHandledQueryCase() {
        assertEquals(QueryContainer.QueryContainerCase.FUNCTION_SCORE, converter.getHandledQueryCase());
    }

    public void testFromProto_ValidFunctionScoreQuery() {
        // Create a simple function score query
        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.newBuilder().build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setFunctionScore(functionScoreQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull(result);
        assertTrue("Should be FunctionScoreQueryBuilder", result instanceof FunctionScoreQueryBuilder);
    }

    public void testFromProto_NullQueryContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { converter.fromProto(null); });
        assertEquals("QueryContainer does not contain a FunctionScore query", exception.getMessage());
    }

    public void testFromProto_MissingFunctionScore() {
        // Create a query container without function score
        QueryContainer queryContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { converter.fromProto(queryContainer); });
        assertEquals("QueryContainer does not contain a FunctionScore query", exception.getMessage());
    }

    public void testFromProto_DifferentQueryType() {
        // Create a query container with a different query type
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchAll(org.opensearch.protobufs.MatchAllQuery.newBuilder().build())
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { converter.fromProto(queryContainer); });
        assertEquals("QueryContainer does not contain a FunctionScore query", exception.getMessage());
    }
}
