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

public class MatchAllQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private MatchAllQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new MatchAllQueryBuilderProtoConverter();
    }

    public void testCanHandle() {
        // Create a QueryContainer with MatchAllQuery
        MatchAllQuery matchAllQuery = MatchAllQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(matchAllQuery).build();

        // Verify that the converter can handle this query
        assertTrue("Converter should handle MatchAllQuery", converter.canHandle(queryContainer));

        // Create a QueryContainer without MatchAllQuery
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Verify that the converter cannot handle this query
        assertFalse("Converter should not handle empty container", converter.canHandle(emptyContainer));

        // Test with null container
        assertFalse("Converter should not handle null container", converter.canHandle(null));
    }

    public void testFromProto() {
        // Create a QueryContainer with MatchAllQuery
        MatchAllQuery matchAllQuery = MatchAllQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(matchAllQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchAllQueryBuilder", queryBuilder instanceof MatchAllQueryBuilder);
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer without MatchAllQuery
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Convert the query, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertEquals("QueryContainer does not contain a MatchAll query", exception.getMessage());
    }
}
