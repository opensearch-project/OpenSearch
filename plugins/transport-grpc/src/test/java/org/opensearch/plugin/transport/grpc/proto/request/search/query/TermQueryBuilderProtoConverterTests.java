/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class TermQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private TermQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new TermQueryBuilderProtoConverter();
    }

    public void testCanHandle() {
        // Create a QueryContainer with Term query
        Map<String, TermQuery> termMap = new HashMap<>();
        FieldValue fieldValue = FieldValue.newBuilder().setStringValue("test-value").build();
        TermQuery termQuery = TermQuery.newBuilder().setValue(fieldValue).build();
        termMap.put("test-field", termQuery);
        QueryContainer queryContainer = QueryContainer.newBuilder().putAllTerm(termMap).build();

        // Verify that the converter can handle this query
        assertTrue("Converter should handle Term query", converter.canHandle(queryContainer));

        // Create a QueryContainer without Term query
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Verify that the converter cannot handle this query
        assertFalse("Converter should not handle empty container", converter.canHandle(emptyContainer));

        // Test with null container
        assertFalse("Converter should not handle null container", converter.canHandle(null));
    }

    public void testFromProto() {
        // Create a QueryContainer with Term query
        Map<String, TermQuery> termMap = new HashMap<>();
        FieldValue fieldValue = FieldValue.newBuilder().setStringValue("test-value").build();
        TermQuery termQuery = TermQuery.newBuilder().setValue(fieldValue).build();
        termMap.put("test-field", termQuery);
        QueryContainer queryContainer = QueryContainer.newBuilder().putAllTerm(termMap).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermQueryBuilder", queryBuilder instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test-value", termQueryBuilder.value());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer without Term query
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Convert the query, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertEquals("QueryContainer does not contain a Term query", exception.getMessage());
    }

    public void testFromProtoWithDifferentValueTypes() {
        // Test with different value types
        Map<String, TermQuery> termMap = new HashMap<>();

        // Test with long value (using GeneralNumber)
        FieldValue longValue = FieldValue.newBuilder()
            .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setInt64Value(123L).build())
            .build();
        TermQuery termQueryLong = TermQuery.newBuilder().setValue(longValue).build();
        termMap.put("long-field", termQueryLong);
        QueryContainer queryContainerLong = QueryContainer.newBuilder().putAllTerm(termMap).build();

        QueryBuilder queryBuilderLong = converter.fromProto(queryContainerLong);
        assertTrue("QueryBuilder should be a TermQueryBuilder", queryBuilderLong instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilderLong = (TermQueryBuilder) queryBuilderLong;
        assertEquals("Field name should match", "long-field", termQueryBuilderLong.fieldName());
        assertEquals("Value should match", 123L, termQueryBuilderLong.value());

        // Test with double value (using GeneralNumber)
        termMap.clear();
        FieldValue doubleValue = FieldValue.newBuilder()
            .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setDoubleValue(123.45).build())
            .build();
        TermQuery termQueryDouble = TermQuery.newBuilder().setValue(doubleValue).build();
        termMap.put("double-field", termQueryDouble);
        QueryContainer queryContainerDouble = QueryContainer.newBuilder().putAllTerm(termMap).build();

        QueryBuilder queryBuilderDouble = converter.fromProto(queryContainerDouble);
        assertTrue("QueryBuilder should be a TermQueryBuilder", queryBuilderDouble instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilderDouble = (TermQueryBuilder) queryBuilderDouble;
        assertEquals("Field name should match", "double-field", termQueryBuilderDouble.fieldName());
        assertEquals("Value should match", 123.45, termQueryBuilderDouble.value());

        // Test with boolean value
        termMap.clear();
        FieldValue boolValue = FieldValue.newBuilder().setBoolValue(true).build();
        TermQuery termQueryBool = TermQuery.newBuilder().setValue(boolValue).build();
        termMap.put("bool-field", termQueryBool);
        QueryContainer queryContainerBool = QueryContainer.newBuilder().putAllTerm(termMap).build();

        QueryBuilder queryBuilderBool = converter.fromProto(queryContainerBool);
        assertTrue("QueryBuilder should be a TermQueryBuilder", queryBuilderBool instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilderBool = (TermQueryBuilder) queryBuilderBool;
        assertEquals("Field name should match", "bool-field", termQueryBuilderBool.fieldName());
        assertEquals("Value should match", true, termQueryBuilderBool.value());
    }

    public void testFromProtoWithEmptyValueType() {
        // Create a QueryContainer with Term query but empty value
        Map<String, TermQuery> termMap = new HashMap<>();
        FieldValue emptyValue = FieldValue.newBuilder().build();
        TermQuery termQuery = TermQuery.newBuilder().setValue(emptyValue).build();
        termMap.put("test-field", termQuery);
        QueryContainer queryContainer = QueryContainer.newBuilder().putAllTerm(termMap).build();

        // Convert the query, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'TermQuery field value not recognized'",
            exception.getMessage().contains("TermQuery field value not recognized")
        );
    }
}
