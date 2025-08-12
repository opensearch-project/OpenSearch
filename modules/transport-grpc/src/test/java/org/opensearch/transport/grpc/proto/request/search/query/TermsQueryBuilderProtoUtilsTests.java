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
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class TermsQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithStringValues() {
        FieldValueArray fieldValueArray = FieldValueArray.newBuilder()
            .addFieldValueArray(FieldValue.newBuilder().setString("value1").build())
            .addFieldValueArray(FieldValue.newBuilder().setString("value2").build())
            .addFieldValueArray(FieldValue.newBuilder().setString("value3").build())
            .build();

        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fieldValueArray).build();

        // Call the method under test
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        // Verify the result
        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "simplified_field", termsQueryBuilder.fieldName());
        List<Object> values = termsQueryBuilder.values();
        assertNotNull("Values should not be null", values);
        assertEquals("Values size should match", 3, values.size());
        assertEquals("First value should match", "value1", values.get(0));
        assertEquals("Second value should match", "value2", values.get(1));
        assertEquals("Third value should match", "value3", values.get(2));
    }

    public void testFromProtoWithTermsLookup() {
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .setLookup(
                org.opensearch.protobufs.TermsLookup.newBuilder().setIndex("test_index").setId("test_id").setPath("test_path").build()
            )
            .build();

        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "simplified_field", termsQueryBuilder.fieldName());

        org.opensearch.indices.TermsLookup termsLookup = termsQueryBuilder.termsLookup();
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("TermsLookup index should match", "test_index", termsLookup.index());
        assertEquals("TermsLookup id should match", "test_id", termsLookup.id());
        assertEquals("TermsLookup path should match", "test_path", termsLookup.path());
    }

    public void testFromProtoWithDefaultValues() {
        FieldValueArray fieldValueArray = FieldValueArray.newBuilder()
            .addFieldValueArray(FieldValue.newBuilder().setString("value1").build())
            .build();

        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fieldValueArray).build();

        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "simplified_field", termsQueryBuilder.fieldName());
        assertEquals("Values size should match", 1, termsQueryBuilder.values().size());
        assertEquals("First value should match", "value1", termsQueryBuilder.values().get(0));
    }

    public void testFromProtoWithNullTermsQueryField() {
        assertThrows(IllegalArgumentException.class, () -> { TermsQueryBuilderProtoUtils.fromProto(null); });
    }

    public void testFromProtoWithEmptyValues() {
        org.opensearch.protobufs.FieldValueArray fieldValueArray = org.opensearch.protobufs.FieldValueArray.newBuilder().build();

        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setFieldValueArray(fieldValueArray)
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);
        assertNotNull(result);
        assertEquals("simplified_field", result.fieldName());
    }

    public void testFromProtoWithBooleanValue() {
        org.opensearch.protobufs.FieldValueArray fieldValueArray = org.opensearch.protobufs.FieldValueArray.newBuilder()
            .addFieldValueArray(org.opensearch.protobufs.FieldValue.newBuilder().setBool(true).build())
            .build();

        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setFieldValueArray(fieldValueArray)
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);
        assertNotNull(result);
        assertEquals("simplified_field", result.fieldName());
    }

    public void testFromProtoWithFloatValue() {
        org.opensearch.protobufs.FieldValueArray fieldValueArray = org.opensearch.protobufs.FieldValueArray.newBuilder()
            .addFieldValueArray(org.opensearch.protobufs.FieldValue.newBuilder().setFloat(3.14f).build())
            .build();

        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setFieldValueArray(fieldValueArray)
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);
        assertNotNull(result);
        assertEquals("simplified_field", result.fieldName());
    }
}
