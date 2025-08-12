/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.FieldValueArray;
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
        assertEquals("Converter should handle TERMS case", QueryContainer.QueryContainerCase.TERMS, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        FieldValueArray fieldValueArray = FieldValueArray.newBuilder()
            .addFieldValueArray(FieldValue.newBuilder().setString("value1").build())
            .addFieldValueArray(FieldValue.newBuilder().setString("value2").build())
            .build();

        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fieldValueArray).build();

        Map<String, TermsQueryField> termsMap = new HashMap<>();
        termsMap.put("test-field", termsQueryField);

        TermsQuery termsQuery = TermsQuery.newBuilder().putAllTerms(termsMap).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQuery).build();

        TermsQueryBuilder queryBuilder = (TermsQueryBuilder) converter.fromProto(queryContainer);

        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermsQueryBuilder", queryBuilder instanceof TermsQueryBuilder);
        assertEquals("Field name should match", "simplified_field", queryBuilder.fieldName());
        assertEquals("Values size should match", 2, queryBuilder.values().size());
        assertEquals("First value should match", "value1", queryBuilder.values().get(0));
        assertEquals("Second value should match", "value2", queryBuilder.values().get(1));
    }

    public void testFromProtoWithInvalidContainer() {
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

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
        TermsQueryField field1 = TermsQueryField.newBuilder()
            .setFieldValueArray(
                FieldValueArray.newBuilder().addFieldValueArray(FieldValue.newBuilder().setString("value1").build()).build()
            )
            .build();
        TermsQueryField field2 = TermsQueryField.newBuilder()
            .setFieldValueArray(
                FieldValueArray.newBuilder().addFieldValueArray(FieldValue.newBuilder().setString("value2").build()).build()
            )
            .build();

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
}
