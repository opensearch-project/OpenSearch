/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.RegexpQuery;
import org.opensearch.test.OpenSearchTestCase;

public class RegexpQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private RegexpQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new RegexpQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals("Converter should handle REGEXP case", QueryContainer.QueryContainerCase.REGEXP, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        // Create a QueryContainer with RegexpQuery
        RegexpQuery regexpQuery = RegexpQuery.newBuilder()
            .setField("test_field")
            .setValue("test.*pattern")
            .setBoost(2.0f)
            .setXName("test_regexp_query")
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setRegexp(regexpQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a RegexpQueryBuilder", queryBuilder instanceof RegexpQueryBuilder);
        RegexpQueryBuilder regexpQueryBuilder = (RegexpQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test_field", regexpQueryBuilder.fieldName());
        assertEquals("Value should match", "test.*pattern", regexpQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, regexpQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_regexp_query", regexpQueryBuilder.queryName());
    }

    public void testFromProtoWithMinimalFields() {
        // Create a QueryContainer with minimal RegexpQuery
        RegexpQuery regexpQuery = RegexpQuery.newBuilder().setField("field_name").setValue("pattern.*").build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setRegexp(regexpQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a RegexpQueryBuilder", queryBuilder instanceof RegexpQueryBuilder);
        RegexpQueryBuilder regexpQueryBuilder = (RegexpQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "field_name", regexpQueryBuilder.fieldName());
        assertEquals("Value should match", "pattern.*", regexpQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, regexpQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", regexpQueryBuilder.queryName());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Regexp query'",
            exception.getMessage().contains("does not contain a Regexp query")
        );
    }

    public void testFromProtoWithNullContainer() {
        // Test that the converter throws an exception with null input
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        // Verify the exception message
        assertTrue(
            "Exception message should mention null",
            exception.getMessage().contains("null") || exception.getMessage().contains("does not contain a Regexp query")
        );
    }
}
