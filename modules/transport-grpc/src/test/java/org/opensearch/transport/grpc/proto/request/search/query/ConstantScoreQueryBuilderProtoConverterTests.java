/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.ConstantScoreQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

public class ConstantScoreQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private ConstantScoreQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new ConstantScoreQueryBuilderProtoConverter();
        QueryBuilderProtoConverterRegistryImpl registry = new QueryBuilderProtoConverterRegistryImpl();
        converter.setRegistry(registry);
    }

    public void testGetHandledQueryCase() {
        assertEquals(
            "Converter should handle CONSTANT_SCORE case",
            QueryContainer.QueryContainerCase.CONSTANT_SCORE,
            converter.getHandledQueryCase()
        );
    }

    public void testFromProto() {
        FieldValue fieldValue = FieldValue.newBuilder().setString("test-value").build();
        TermQuery termQuery = TermQuery.newBuilder().setField("test-field").setValue(fieldValue).build();
        QueryContainer filterContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        ConstantScoreQuery constantScoreQuery = ConstantScoreQuery.newBuilder()
            .setFilter(filterContainer)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setConstantScore(constantScoreQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a ConstantScoreQueryBuilder", queryBuilder instanceof ConstantScoreQueryBuilder);

        ConstantScoreQueryBuilder constantScoreQueryBuilder = (ConstantScoreQueryBuilder) queryBuilder;
        assertEquals("Boost should match", 2.0f, constantScoreQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", constantScoreQueryBuilder.queryName());

        QueryBuilder innerQuery = constantScoreQueryBuilder.innerQuery();
        assertNotNull("Inner query should not be null", innerQuery);
        assertTrue("Inner query should be a TermQueryBuilder", innerQuery instanceof TermQueryBuilder);

        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) innerQuery;
        assertEquals("Field name should match", "test-field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test-value", termQueryBuilder.value());
    }

    public void testFromProtoWithOnlyFilter() {
        FieldValue fieldValue = FieldValue.newBuilder().setString("test").build();
        TermQuery termQuery = TermQuery.newBuilder().setField("field").setValue(fieldValue).build();
        QueryContainer filterContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        ConstantScoreQuery constantScoreQuery = ConstantScoreQuery.newBuilder().setFilter(filterContainer).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setConstantScore(constantScoreQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a ConstantScoreQueryBuilder", queryBuilder instanceof ConstantScoreQueryBuilder);

        ConstantScoreQueryBuilder constantScoreQueryBuilder = (ConstantScoreQueryBuilder) queryBuilder;
        assertEquals("Default boost should be 1.0", 1.0f, constantScoreQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", constantScoreQueryBuilder.queryName());

        // Verify the inner filter query
        QueryBuilder innerQuery = constantScoreQueryBuilder.innerQuery();
        assertNotNull("Inner query should not be null", innerQuery);
        assertTrue("Inner query should be a TermQueryBuilder", innerQuery instanceof TermQueryBuilder);
    }

    public void testFromProtoWithInvalidContainer() {
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        assertTrue(
            "Exception message should indicate missing ConstantScore query",
            exception.getMessage().contains("does not contain a ConstantScore query")
        );
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertTrue(
            "Exception message should indicate missing ConstantScore query",
            exception.getMessage().contains("does not contain a ConstantScore query")
        );
    }

    public void testFromProtoWithMissingFilter() {
        ConstantScoreQuery constantScoreQuery = ConstantScoreQuery.newBuilder().setBoost(2.0f).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setConstantScore(constantScoreQuery).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        assertTrue("Exception message should indicate missing filter", exception.getMessage().contains("must have a filter query"));
    }
}
