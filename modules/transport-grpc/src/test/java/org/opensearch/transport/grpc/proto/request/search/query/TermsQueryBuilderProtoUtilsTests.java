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
import org.opensearch.test.OpenSearchTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TermsQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithDefaultBehavior() {

        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("v").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setValue(fva).build();
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
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().setValue(fva).build();

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
            .setValue(fva)
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
        assertEquals("idx", builder.termsLookup().index());
        assertEquals("1", builder.termsLookup().id());
        assertEquals("tags", builder.termsLookup().path());
        assertEquals("r", builder.termsLookup().routing());
        assertTrue("store should be true", builder.termsLookup().store());
    }

    public void testFromProtoWithLookupStoreDefaultFalse() {
        String fieldName = "tags";

        // Test without setting store (should default to false)
        org.opensearch.protobufs.TermsLookup lookup = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("idx")
            .setId("1")
            .setPath("tags")
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
        assertNotNull("termsLookup should be set for lookup path", builder.termsLookup());
        assertFalse("store should default to false", builder.termsLookup().store());
    }

    public void testFromProtoNewOverloadBitmapDecoding() {
        String fieldName = "bitmap_field";
        // base64 for bytes {1,2}
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("AQI=").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setValue(fva)
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
            .setValue(fva)
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

    public void testFromProtoWithTermsQuery() {
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("test_value").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setValue(fva)
            .build();

        org.opensearch.protobufs.TermsQuery termsQuery = org.opensearch.protobufs.TermsQuery.newBuilder()
            .putTerms("test_field", termsQueryField)
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQuery);

        assertNotNull("Result should not be null", result);
        assertEquals("Field name should match", "test_field", result.fieldName());
        assertEquals("Boost should be set", 2.0f, result.boost(), 0.0f);
        assertEquals("Query name should be set", "test_query", result.queryName());
        assertEquals("Value type should be DEFAULT", TermsQueryBuilder.ValueType.DEFAULT, result.valueType());
        assertEquals("Values should have 1 entry", 1, result.values().size());
        assertEquals("First value should match", "test_value", result.values().get(0));
    }

    public void testFromProtoWithTermsQueryNullInput() {
        assertThrows(
            IllegalArgumentException.class,
            () -> TermsQueryBuilderProtoUtils.fromProto((org.opensearch.protobufs.TermsQuery) null)
        );
    }

    public void testFromProtoWithTermsQueryEmptyMap() {
        org.opensearch.protobufs.TermsQuery termsQuery = org.opensearch.protobufs.TermsQuery.newBuilder().build();

        assertThrows(IllegalArgumentException.class, () -> TermsQueryBuilderProtoUtils.fromProto(termsQuery));
    }

    public void testFromProtoWithTermsQueryMultipleFields() {
        org.opensearch.protobufs.FieldValue fv1 = org.opensearch.protobufs.FieldValue.newBuilder().setString("value1").build();
        org.opensearch.protobufs.FieldValue fv2 = org.opensearch.protobufs.FieldValue.newBuilder().setString("value2").build();
        org.opensearch.protobufs.FieldValueArray fva1 = org.opensearch.protobufs.FieldValueArray.newBuilder()
            .addFieldValueArray(fv1)
            .build();
        org.opensearch.protobufs.FieldValueArray fva2 = org.opensearch.protobufs.FieldValueArray.newBuilder()
            .addFieldValueArray(fv2)
            .build();

        org.opensearch.protobufs.TermsQuery termsQuery = org.opensearch.protobufs.TermsQuery.newBuilder()
            .putTerms("field1", org.opensearch.protobufs.TermsQueryField.newBuilder().setValue(fva1).build())
            .putTerms("field2", org.opensearch.protobufs.TermsQueryField.newBuilder().setValue(fva2).build())
            .build();

        assertThrows(IllegalArgumentException.class, () -> TermsQueryBuilderProtoUtils.fromProto(termsQuery));
    }

    public void testFromProtoWithTermsQueryLookup() {
        // Test the fromProto(TermsQuery) method with lookup
        org.opensearch.protobufs.TermsLookup lookup = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .setRouting("test_routing")
            .build();

        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setLookup(lookup)
            .build();

        org.opensearch.protobufs.TermsQuery termsQuery = org.opensearch.protobufs.TermsQuery.newBuilder()
            .putTerms("lookup_field", termsQueryField)
            .setBoost(1.5f)
            .setXName("lookup_query")
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQuery);

        assertNotNull("Result should not be null", result);
        assertEquals("Field name should match", "lookup_field", result.fieldName());
        assertEquals("Boost should be set", 1.5f, result.boost(), 0.0f);
        assertEquals("Query name should be set", "lookup_query", result.queryName());
        assertNotNull("TermsLookup should be set", result.termsLookup());
        assertEquals("Index should match", "test_index", result.termsLookup().index());
        assertEquals("ID should match", "test_id", result.termsLookup().id());
        assertEquals("Path should match", "test_path", result.termsLookup().path());
        assertEquals("Routing should match", "test_routing", result.termsLookup().routing());
    }

    public void testFromProtoWithTermsQueryDefaultValues() {
        // Test the fromProto(TermsQuery) method with default values
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("default_value").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setValue(fva)
            .build();

        org.opensearch.protobufs.TermsQuery termsQuery = org.opensearch.protobufs.TermsQuery.newBuilder()
            .putTerms("default_field", termsQueryField)
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQuery);

        assertNotNull("Result should not be null", result);
        assertEquals("Field name should match", "default_field", result.fieldName());
        assertEquals("Boost should be default", 1.0f, result.boost(), 0.0f);
        assertNull("Query name should be null", result.queryName());
        assertEquals("Value type should be default", TermsQueryBuilder.ValueType.DEFAULT, result.valueType());
    }

    public void testFromProtoWithTermsQueryBitmapValueType() {
        // Test the fromProto(TermsQuery) method with bitmap value type
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("AQI=").build(); // base64 for
                                                                                                                             // {1,2}
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setValue(fva)
            .build();

        org.opensearch.protobufs.TermsQuery termsQuery = org.opensearch.protobufs.TermsQuery.newBuilder()
            .putTerms("bitmap_field", termsQueryField)
            .setValueType(org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_BITMAP)
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQuery);

        assertNotNull("Result should not be null", result);
        assertEquals("Field name should match", "bitmap_field", result.fieldName());
        assertEquals("Value type should be BITMAP", TermsQueryBuilder.ValueType.BITMAP, result.valueType());
        assertEquals("Values should have 1 entry", 1, result.values().size());
        assertTrue("Value should be BytesArray", result.values().get(0) instanceof org.opensearch.core.common.bytes.BytesArray);
    }

    public void testFromProtoWithTermsQueryNullValueType() {
        // Test the fromProto(TermsQuery) method with null value type (should default to DEFAULT)
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("test_value").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setValue(fva)
            .build();

        org.opensearch.protobufs.TermsQuery termsQuery = org.opensearch.protobufs.TermsQuery.newBuilder()
            .putTerms("test_field", termsQueryField)
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQuery);

        assertNotNull("Result should not be null", result);
        assertEquals("Value type should default to DEFAULT", TermsQueryBuilder.ValueType.DEFAULT, result.valueType());
    }

    public void testFromProtoWithTermsQueryUnspecifiedValueType() {
        // Test the fromProto(TermsQuery) method with UNSPECIFIED value type (should default to DEFAULT)
        org.opensearch.protobufs.FieldValue fv = org.opensearch.protobufs.FieldValue.newBuilder().setString("test_value").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder().addFieldValueArray(fv).build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setValue(fva)
            .build();

        org.opensearch.protobufs.TermsQuery termsQuery = org.opensearch.protobufs.TermsQuery.newBuilder()
            .putTerms("test_field", termsQueryField)
            .setValueType(org.opensearch.protobufs.TermsQueryValueType.TERMS_QUERY_VALUE_TYPE_UNSPECIFIED)
            .build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQuery);

        assertNotNull("Result should not be null", result);
        assertEquals("Value type should default to DEFAULT for UNSPECIFIED", TermsQueryBuilder.ValueType.DEFAULT, result.valueType());
    }

    public void testFromProtoWithTermsQueryBitmapInvalidMultipleValues() {
        String fieldName = "bitmap_field";
        org.opensearch.protobufs.FieldValue fv1 = org.opensearch.protobufs.FieldValue.newBuilder().setString("AQI=").build();
        org.opensearch.protobufs.FieldValue fv2 = org.opensearch.protobufs.FieldValue.newBuilder().setString("AQI=").build();
        org.opensearch.protobufs.FieldValueArray fva = org.opensearch.protobufs.FieldValueArray.newBuilder()
            .addFieldValueArray(fv1)
            .addFieldValueArray(fv2)
            .build();
        org.opensearch.protobufs.TermsQueryField termsQueryField = org.opensearch.protobufs.TermsQueryField.newBuilder()
            .setValue(fva)
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

}
