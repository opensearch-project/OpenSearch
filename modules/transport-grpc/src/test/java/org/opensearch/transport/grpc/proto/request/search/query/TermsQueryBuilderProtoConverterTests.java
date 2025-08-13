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

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
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

    public void testFromProtoWithMultipleFields() {
        TermsQueryField field1 = TermsQueryField.newBuilder().build();
        TermsQueryField field2 = TermsQueryField.newBuilder().build();

        Map<String, TermsQueryField> termsMap = new HashMap<>();
        termsMap.put("field1", field1);
        termsMap.put("field2", field2);

        TermsQuery termsQuery = TermsQuery.newBuilder().putAllTerms(termsMap).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
        assertTrue("Exception message should mention 'exactly one field'", exception.getMessage().contains("exactly one field"));
    }

    public void testFromProtoWithEmptyTermsMap() {
        TermsQuery termsQuery = TermsQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
        assertTrue("Exception message should mention 'exactly one field'", exception.getMessage().contains("exactly one field"));
    }

    public void testFromProtoAppliesBoostAndName() {
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("x").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();
        TermsQuery termsQuery = TermsQuery.newBuilder()
            .putTerms("title", termsQueryField)
            .setBoost(2.5f)
            .setUnderscoreName("qname")
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        QueryBuilder qb = converter.fromProto(queryContainer);
        assertTrue(qb instanceof TermsQueryBuilder);
        TermsQueryBuilder tb = (TermsQueryBuilder) qb;

        // Field name will be whatever util returns; here we assert the applied boost and name
        assertEquals(2.5f, tb.boost(), 0.0f);
        assertEquals("qname", tb.queryName());
        assertEquals("title", tb.fieldName());
    }

    public void testFromProtoPropagatesFieldNameAndDefaultValueType() {
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("y").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();
        TermsQuery termsQuery = TermsQuery.newBuilder().putTerms("author", termsQueryField).build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        QueryBuilder qb = converter.fromProto(queryContainer);
        assertTrue(qb instanceof TermsQueryBuilder);
        TermsQueryBuilder tb = (TermsQueryBuilder) qb;

        // ValueType should default to DEFAULT
        assertEquals(TermsQueryBuilder.ValueType.DEFAULT, tb.valueType());
        assertEquals("author", tb.fieldName());
    }
}
