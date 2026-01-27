/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.BoostingQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

public class BoostingQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private BoostingQueryBuilderProtoConverter converter;
    private QueryBuilderProtoConverterRegistryImpl registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new BoostingQueryBuilderProtoConverter();
        registry = new QueryBuilderProtoConverterRegistryImpl();
        converter.setRegistry(registry);
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals(
            "Converter should handle BOOSTING case",
            QueryContainer.QueryContainerCase.BOOSTING,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        // Create a QueryContainer with BoostingQuery
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .setNegative(
                QueryContainer.newBuilder()
                    .setTerm(
                        TermQuery.newBuilder()
                            .setField("field1")
                            .setValue(FieldValue.newBuilder().setString("value1").build())
                            .build()
                    )
                    .build()
            )
            .setNegativeBoost(0.5f)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setBoosting(boostingQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a BoostingQueryBuilder", queryBuilder instanceof BoostingQueryBuilder);
        BoostingQueryBuilder boostingQueryBuilder = (BoostingQueryBuilder) queryBuilder;
        assertEquals("Boost should match", 2.0f, boostingQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", boostingQueryBuilder.queryName());
        assertEquals("Negative boost should match", 0.5f, boostingQueryBuilder.negativeBoost(), 0.0f);
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Boosting query'",
            exception.getMessage().contains("does not contain a Boosting query")
        );
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception with null container
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Boosting query'",
            exception.getMessage().contains("does not contain a Boosting query")
        );
    }
}
