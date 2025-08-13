/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.protobufs.ValueType;
import org.opensearch.test.OpenSearchTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TermsQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithDefaultBehavior() {

        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("v").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(
            "field",
            termsQueryField,
            org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT
        );
        assertNotNull(termsQueryBuilder);
        assertEquals("field", termsQueryBuilder.fieldName());
        assertEquals(1, termsQueryBuilder.values().size());
        assertEquals("v", termsQueryBuilder.values().get(0));
    }

    public void testFromProtoWithNullInput() {

        assertThrows(
            IllegalArgumentException.class,
            () -> TermsQueryBuilderProtoUtils.fromProto(
                "",
                TermsQueryField.newBuilder().build(),
                org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT
            )
        );
    }

    public void testFromProtoWithEmptyTermsQueryField() {
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().build();
        assertThrows(
            IllegalArgumentException.class,
            () -> TermsQueryBuilderProtoUtils.fromProto(
                "field",
                termsQueryField,
                org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT
            )
        );
    }

    public void testParseValueTypeWithBitmap() {
        // Test the parseValueType method with BITMAP
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_BITMAP);

        assertEquals("Value type should be BITMAP", TermsQueryBuilder.ValueType.BITMAP, valueType);
    }

    public void testParseValueTypeWithDefault() {
        // Test the parseValueType method with DEFAULT
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_DEFAULT);

        assertEquals("Value type should be DEFAULT", TermsQueryBuilder.ValueType.DEFAULT, valueType);
    }

    public void testParseValueTypeWithUnspecified() {
        // Test the parseValueType method with UNSPECIFIED
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_UNSPECIFIED);

        assertEquals("Value type should be DEFAULT for UNSPECIFIED", TermsQueryBuilder.ValueType.DEFAULT, valueType);
    }

    public void testParseValueTypeWithNull() {

        assertThrows(NullPointerException.class, () -> TermsQueryBuilderProtoUtils.parseValueType((ValueType) null));
    }

    public void testFromProtoWithTermsLookupField() {
        org.opensearch.protobufs.TermsLookup lookup = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("i")
            .setId("1")
            .setPath("p")
            .build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setLookup(lookup).build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(
            "field",
            termsQueryField,
            org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT
        );

        assertNotNull("Result should not be null", result);
        assertEquals("field", result.fieldName());
    }

    public void testFromProtoWithValueTypeField() {

        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("x").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setFieldValueArray(fva).build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(
            "field",
            termsQueryField,
            org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT
        );

        assertNotNull("Result should not be null", result);
        assertEquals("field", result.fieldName());
    }

    public void testFromProtoNewOverloadWithFieldValueArray() {
        String fieldName = "test-field";

        org.opensearch.protobufs.FieldValue fv1 = org.opensearch.protobufs.FieldValue.newBuilder().setString("a").build();
        org.opensearch.protobufs.FieldValue fv2 = org.opensearch.protobufs.FieldValue.newBuilder().setBool(true).build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder()
            .addFieldValueArray(fv1)
            .addFieldValueArray(fv2)
            .build();

        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setFieldValueArray(fva)
            .build();

        org.opensearch.protobufs.TermsQueryValueType vt = org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT;

        TermsQueryBuilder builder = TermsQueryBuilderProtoUtils.fromProto(fieldName, termsQueryField, vt);

        assertNotNull(builder);
        assertEquals(fieldName, builder.fieldName());
        assertEquals(2, builder.values().size());
        assertEquals("a", builder.values().get(0));
        assertEquals(true, builder.values().get(1));
        assertEquals(TermsQueryBuilder.ValueType.DEFAULT, builder.valueType());
    }

    public void testFromProtoNewOverloadWithLookup() {
        String fieldName = "tags";

        org.opensearch.protobufs.TermsLookup lookup = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("idx")
            .setId("1")
            .setPath("tags")
            .setRouting("r")
            .setStore(true)
            .build();

        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setLookup(lookup)
            .build();

        TermsQueryBuilder builder = TermsQueryBuilderProtoUtils.fromProto(
            fieldName,
            termsQueryField,
            org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT
        );

        assertNotNull(builder);
        assertEquals(fieldName, builder.fieldName());
        assertEquals(TermsQueryBuilder.ValueType.DEFAULT, builder.valueType());
        assertNotNull("termsLookup should be set for lookup path", builder.termsLookup());
    }

    public void testFromProtoNewOverloadBitmapDecoding() {
        String fieldName = "bitmap_field";
        // base64 for bytes {1,2}
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("AQI=").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setFieldValueArray(fva)
            .build();

        TermsQueryBuilder builder = TermsQueryBuilderProtoUtils.fromProto(
            fieldName,
            termsQueryField,
            org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_BITMAP
        );

        assertNotNull(builder);
        assertEquals(fieldName, builder.fieldName());
        assertEquals(TermsQueryBuilder.ValueType.BITMAP, builder.valueType());
        assertEquals(1, builder.values().size());
        assertTrue(builder.values().get(0) instanceof org.opensearch.core.common.bytes.BytesArray);
    }

    public void testFromProtoNewOverloadBitmapInvalidMultipleValues() {
        String fieldName = "bitmap_field";
        org.opensearch.protobufs.FieldValue fv1 = org.opensearch.protobufs.FieldValue.newBuilder().setString("AQI=").build();
        org.opensearch.protobufs.FieldValue fv2 = org.opensearch.protobufs.FieldValue.newBuilder().setString("AQI=").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder()
            .addFieldValueArray(fv1)
            .addFieldValueArray(fv2)
            .build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setFieldValueArray(fva)
            .build();

        assertThrows(
            IllegalArgumentException.class,
            () -> TermsQueryBuilderProtoUtils.fromProto(
                fieldName,
                termsQueryField,
                org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_BITMAP
            )
        );
    }

    public void testFromProtoNewOverloadMissingValuesAndLookup() {
        String fieldName = "f";
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder().build();
        assertThrows(
            IllegalArgumentException.class,
            () -> TermsQueryBuilderProtoUtils.fromProto(
                fieldName,
                termsQueryField,
                org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT
            )
        );
    }

    public void testParseValueTypeTermsQueryValueTypeMapping() {
        assertEquals(
            TermsQueryBuilder.ValueType.BITMAP,
            TermsQueryBuilderProtoUtils.parseValueType(org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_BITMAP)
        );
        assertEquals(
            TermsQueryBuilder.ValueType.DEFAULT,
            TermsQueryBuilderProtoUtils.parseValueType(org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_DEFAULT)
        );
        assertEquals(
            TermsQueryBuilder.ValueType.DEFAULT,
            TermsQueryBuilderProtoUtils.parseValueType(org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_UNSPECIFIED)
        );
    }
}
