/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.MatchNoneQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class MatchNoneQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private MatchNoneQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MatchNoneQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals(
            "Converter should handle MATCH_NONE case",
            QueryContainer.QueryContainerCase.MATCH_NONE,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        // Create a QueryContainer with MatchNoneQuery
        MatchNoneQuery matchNoneQuery = MatchNoneQuery.newBuilder().setBoost(2.0f).setXName("test_query").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchNone(matchNoneQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchNoneQueryBuilder", queryBuilder instanceof MatchNoneQueryBuilder);
        MatchNoneQueryBuilder matchNoneQueryBuilder = (MatchNoneQueryBuilder) queryBuilder;
        assertEquals("Boost should match", 2.0f, matchNoneQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", matchNoneQueryBuilder.queryName());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a MatchNone query'",
            exception.getMessage().contains("does not contain a MatchNone query")
        );
    }
}
