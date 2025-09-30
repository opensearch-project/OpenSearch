/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermsQuery;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TermsQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private TermsQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new TermsQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals("Converter should handle TERMS case", QueryContainer.QueryContainerCase.TERMS, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("v1").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();
        Map<String, TermsQueryField> termsMap = new HashMap<>();
        termsMap.put("test-field", termsQueryField);
        TermsQuery termsQuery = TermsQuery.newBuilder().putTerms("test-field", termsQueryField).build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermsQueryBuilder", queryBuilder instanceof TermsQueryBuilder);
        TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;

        assertEquals("Field name should match map key", "test-field", termsQueryBuilder.fieldName());
        assertEquals("Values should have 1 entry", 1, termsQueryBuilder.values().size());
        assertEquals("First value should match", "v1", termsQueryBuilder.values().get(0));
        assertEquals("Boost should be default", 1.0f, termsQueryBuilder.boost(), 0.0f);
        assertTrue("Query name should be null", termsQueryBuilder.queryName() == null);
        assertEquals("Value type should be default", TermsQueryBuilder.ValueType.DEFAULT, termsQueryBuilder.valueType());
    }

    public void testFromProtoWithMultipleFields() {
        TermsQueryField field1 = TermsQueryField.newBuilder().build();
        TermsQueryField field2 = TermsQueryField.newBuilder().build();

        Map<String, TermsQueryField> termsMap = new HashMap<>();
        termsMap.put("field1", field1);
        termsMap.put("field2", field2);

        TermsQuery termsQuery = TermsQuery.newBuilder().putAllTerms(termsMap).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        // The converter delegates to utils, which will handle the validation
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
        assertTrue("Exception message should mention 'exactly one field'", exception.getMessage().contains("exactly one field"));
    }

    public void testFromProtoWithEmptyTermsMap() {
        TermsQuery termsQuery = TermsQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        // The converter delegates to utils, which will handle the validation
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
        assertTrue("Exception message should mention 'exactly one field'", exception.getMessage().contains("exactly one field"));
    }

    public void testFromProtoWithBoostAndQueryName() {
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("test_value").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();

        TermsQuery termsQuery = TermsQuery.newBuilder()
            .putTerms("test_field", termsQueryField)
            .setBoost(2.5f)
            .setXName("test_query_name")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermsQueryBuilder", queryBuilder instanceof TermsQueryBuilder);
        TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;

        assertEquals("Field name should match", "test_field", termsQueryBuilder.fieldName());
        assertEquals("Boost should be set", 2.5f, termsQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should be set", "test_query_name", termsQueryBuilder.queryName());
        assertEquals("Values should have 1 entry", 1, termsQueryBuilder.values().size());
        assertEquals("First value should match", "test_value", termsQueryBuilder.values().get(0));
    }

    public void testFromProtoWithValueType() {
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("AQI=").build(); // base64 for
                                                                                                                             // {1,2}
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();

        TermsQuery termsQuery = TermsQuery.newBuilder()
            .putTerms("bitmap_field", termsQueryField)
            .setValueType(org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_BITMAP)
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermsQueryBuilder", queryBuilder instanceof TermsQueryBuilder);
        TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;

        assertEquals("Field name should match", "bitmap_field", termsQueryBuilder.fieldName());
        assertEquals("Value type should be BITMAP", TermsQueryBuilder.ValueType.BITMAP, termsQueryBuilder.valueType());
        assertEquals("Values should have 1 entry", 1, termsQueryBuilder.values().size());
        assertTrue("Value should be BytesArray", termsQueryBuilder.values().get(0) instanceof org.opensearch.core.common.bytes.BytesArray);
    }

    public void testFromProtoWithTermsLookup() {
        org.opensearch.protobufs.TermsLookup lookup = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .setRouting("test_routing")
            .build();

        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setLookup(lookup).build();

        TermsQuery termsQuery = TermsQuery.newBuilder()
            .putTerms("lookup_field", termsQueryField)
            .setBoost(1.5f)
            .setXName("lookup_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermsQueryBuilder", queryBuilder instanceof TermsQueryBuilder);
        TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;

        assertEquals("Field name should match", "lookup_field", termsQueryBuilder.fieldName());
        assertEquals("Boost should be set", 1.5f, termsQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should be set", "lookup_query", termsQueryBuilder.queryName());
        assertNotNull("TermsLookup should be set", termsQueryBuilder.termsLookup());
        assertEquals("Index should match", "test_index", termsQueryBuilder.termsLookup().index());
        assertEquals("ID should match", "test_id", termsQueryBuilder.termsLookup().id());
        assertEquals("Path should match", "test_path", termsQueryBuilder.termsLookup().path());
        assertEquals("Routing should match", "test_routing", termsQueryBuilder.termsLookup().routing());
    }

    public void testFromProtoWithDefaultValues() {
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("default_value").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();

        TermsQuery termsQuery = TermsQuery.newBuilder().putTerms("default_field", termsQueryField).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermsQueryBuilder", queryBuilder instanceof TermsQueryBuilder);
        TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;

        assertEquals("Field name should match", "default_field", termsQueryBuilder.fieldName());
        assertEquals("Boost should be default", 1.0f, termsQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", termsQueryBuilder.queryName());
        assertEquals("Value type should be default", TermsQueryBuilder.ValueType.DEFAULT, termsQueryBuilder.valueType());
        assertEquals("Values should have 1 entry", 1, termsQueryBuilder.values().size());
        assertEquals("First value should match", "default_value", termsQueryBuilder.values().get(0));
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception for basic validation
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Terms query'",
            exception.getMessage().contains("does not contain a Terms query")
        );
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));
        assertTrue(
            "Exception message should mention 'does not contain a Terms query'",
            exception.getMessage().contains("does not contain a Terms query")
        );
    }
}
