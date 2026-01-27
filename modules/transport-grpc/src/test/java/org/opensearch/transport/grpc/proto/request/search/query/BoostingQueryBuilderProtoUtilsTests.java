/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.BoostingQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

public class BoostingQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistryImpl registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoWithAllFields() {
        // Create a protobuf BoostingQuery with all fields
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(createMatchAllQueryContainer())
            .setNegative(createTermQueryContainer("field1", "value1"))
            .setNegativeBoost(0.5f)
            .setBoost(2.0f)
            .setXName("test_boosting")
            .build();

        // Call the method under test
        BoostingQueryBuilder result = BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry);

        // Verify the result
        assertNotNull("BoostingQueryBuilder should not be null", result);
        assertEquals("Boost should match", 2.0f, result.boost(), 0.0f);
        assertEquals("Query name should match", "test_boosting", result.queryName());
        assertEquals("Negative boost should match", 0.5f, result.negativeBoost(), 0.0f);

        // Verify positive query
        assertNotNull("Positive query should not be null", result.positiveQuery());
        assertTrue("Positive query should be MatchAllQueryBuilder", result.positiveQuery() instanceof MatchAllQueryBuilder);

        // Verify negative query
        assertNotNull("Negative query should not be null", result.negativeQuery());
        assertTrue("Negative query should be TermQueryBuilder", result.negativeQuery() instanceof TermQueryBuilder);
        TermQueryBuilder negativeQuery = (TermQueryBuilder) result.negativeQuery();
        assertEquals("Negative query field should match", "field1", negativeQuery.fieldName());
        assertEquals("Negative query value should match", "value1", negativeQuery.value());
    }

    public void testFromProtoWithMinimalFields() {
        // Create a protobuf BoostingQuery with only required fields
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(createMatchAllQueryContainer())
            .setNegative(createTermQueryContainer("field1", "value1"))
            .setNegativeBoost(0.3f)
            .build();

        // Call the method under test
        BoostingQueryBuilder result = BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry);

        // Verify the result
        assertNotNull("BoostingQueryBuilder should not be null", result);
        assertEquals("Boost should be default", 1.0f, result.boost(), 0.0f);
        assertNull("Query name should be null", result.queryName());
        assertEquals("Negative boost should match", 0.3f, result.negativeBoost(), 0.0f);
        assertNotNull("Positive query should not be null", result.positiveQuery());
        assertNotNull("Negative query should not be null", result.negativeQuery());
    }

    public void testFromProtoWithMissingPositiveQuery() {
        // Create a protobuf BoostingQuery without positive query (empty QueryContainer)
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setNegative(createTermQueryContainer("field1", "value1"))
            .setNegativeBoost(0.5f)
            .build();

        // Call the method under test and expect exception
        // Note: Since positive/negative are required proto fields, they default to empty QueryContainer
        // The registry will throw "Unsupported query type" for QUERYCONTAINER_NOT_SET
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry)
        );

        assertTrue(
            "Exception should mention unsupported query or missing positive",
            exception.getMessage().contains("Unsupported query type") || exception.getMessage().contains("positive")
        );
    }

    public void testFromProtoWithMissingNegativeQuery() {
        // Create a protobuf BoostingQuery without negative query (empty QueryContainer)
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(createMatchAllQueryContainer())
            .setNegativeBoost(0.5f)
            .build();

        // Call the method under test and expect exception
        // Note: Since positive/negative are required proto fields, they default to empty QueryContainer
        // The registry will throw "Unsupported query type" for QUERYCONTAINER_NOT_SET
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry)
        );

        assertTrue(
            "Exception should mention unsupported query or missing negative",
            exception.getMessage().contains("Unsupported query type") || exception.getMessage().contains("negative")
        );
    }

    public void testFromProtoWithZeroNegativeBoost() {
        // Create a protobuf BoostingQuery with negative_boost = 0 (invalid)
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(createMatchAllQueryContainer())
            .setNegative(createTermQueryContainer("field1", "value1"))
            .setNegativeBoost(0.0f)
            .build();

        // Call the method under test - should work as 0 is not < 0
        BoostingQueryBuilder result = BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry);
        assertNotNull("BoostingQueryBuilder should not be null", result);
        assertEquals("Negative boost should be 0", 0.0f, result.negativeBoost(), 0.0f);
    }

    public void testFromProtoWithNegativeNegativeBoost() {
        // Create a protobuf BoostingQuery with negative_boost < 0 (invalid)
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(createMatchAllQueryContainer())
            .setNegative(createTermQueryContainer("field1", "value1"))
            .setNegativeBoost(-0.5f)
            .build();

        // Call the method under test and expect exception
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry)
        );

        assertEquals(
            "Exception message should match constant",
            BoostingQueryBuilder.NEGATIVE_BOOST_POSITIVE_VALUE_REQUIRED,
            exception.getMessage()
        );
    }

    public void testFromProtoWithOnlyBoost() {
        // Create a protobuf BoostingQuery with only boost (no name)
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(createMatchAllQueryContainer())
            .setNegative(createTermQueryContainer("field1", "value1"))
            .setNegativeBoost(0.5f)
            .setBoost(3.5f)
            .build();

        // Call the method under test
        BoostingQueryBuilder result = BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry);

        // Verify the result
        assertNotNull("BoostingQueryBuilder should not be null", result);
        assertEquals("Boost should match", 3.5f, result.boost(), 0.0f);
        assertNull("Query name should be null", result.queryName());
    }

    public void testFromProtoWithOnlyName() {
        // Create a protobuf BoostingQuery with only name (no boost)
        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(createMatchAllQueryContainer())
            .setNegative(createTermQueryContainer("field1", "value1"))
            .setNegativeBoost(0.5f)
            .setXName("my_boosting_query")
            .build();

        // Call the method under test
        BoostingQueryBuilder result = BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry);

        // Verify the result
        assertNotNull("BoostingQueryBuilder should not be null", result);
        assertEquals("Boost should be default", 1.0f, result.boost(), 0.0f);
        assertEquals("Query name should match", "my_boosting_query", result.queryName());
    }

    public void testFromProtoWithComplexNestedQueries() {
        // Create nested queries for both positive and negative
        QueryContainer positiveQuery = QueryContainer.newBuilder()
            .setTerm(
                TermQuery.newBuilder()
                    .setField("status")
                    .setValue(FieldValue.newBuilder().setString("active").build())
                    .build()
            )
            .build();

        QueryContainer negativeQuery = QueryContainer.newBuilder()
            .setTerm(
                TermQuery.newBuilder()
                    .setField("category")
                    .setValue(FieldValue.newBuilder().setString("spam").build())
                    .build()
            )
            .build();

        BoostingQuery boostingQuery = BoostingQuery.newBuilder()
            .setPositive(positiveQuery)
            .setNegative(negativeQuery)
            .setNegativeBoost(0.2f)
            .setBoost(1.5f)
            .setXName("complex_boosting")
            .build();

        // Call the method under test
        BoostingQueryBuilder result = BoostingQueryBuilderProtoUtils.fromProto(boostingQuery, registry);

        // Verify the result
        assertNotNull("BoostingQueryBuilder should not be null", result);
        assertEquals("Boost should match", 1.5f, result.boost(), 0.0f);
        assertEquals("Query name should match", "complex_boosting", result.queryName());
        assertEquals("Negative boost should match", 0.2f, result.negativeBoost(), 0.0f);

        // Verify positive query
        assertTrue("Positive query should be TermQueryBuilder", result.positiveQuery() instanceof TermQueryBuilder);
        TermQueryBuilder positiveTermQuery = (TermQueryBuilder) result.positiveQuery();
        assertEquals("Positive query field should match", "status", positiveTermQuery.fieldName());
        assertEquals("Positive query value should match", "active", positiveTermQuery.value());

        // Verify negative query
        assertTrue("Negative query should be TermQueryBuilder", result.negativeQuery() instanceof TermQueryBuilder);
        TermQueryBuilder negativeTermQuery = (TermQueryBuilder) result.negativeQuery();
        assertEquals("Negative query field should match", "category", negativeTermQuery.fieldName());
        assertEquals("Negative query value should match", "spam", negativeTermQuery.value());
    }

    // Helper methods to create QueryContainers
    private QueryContainer createMatchAllQueryContainer() {
        return QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();
    }

    private QueryContainer createTermQueryContainer(String field, String value) {
        return QueryContainer.newBuilder()
            .setTerm(
                TermQuery.newBuilder()
                    .setField(field)
                    .setValue(FieldValue.newBuilder().setString(value).build())
                    .build()
            )
            .build();
    }
}
