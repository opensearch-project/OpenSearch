/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.common;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.GeneralNumber;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class FieldValueProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithInteger() {
        Integer intValue = 42;
        FieldValue fieldValue = FieldValueProtoUtils.toProto(intValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());

        GeneralNumber generalNumber = fieldValue.getGeneralNumber();
        assertTrue("GeneralNumber should have int32 value", generalNumber.hasInt32Value());
        assertEquals("Int32 value should match", 42, generalNumber.getInt32Value());
    }

    public void testToProtoWithLong() {
        Long longValue = 9223372036854775807L; // Max long value
        FieldValue fieldValue = FieldValueProtoUtils.toProto(longValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());

        GeneralNumber generalNumber = fieldValue.getGeneralNumber();
        assertTrue("GeneralNumber should have int64 value", generalNumber.hasInt64Value());
        assertEquals("Int64 value should match", 9223372036854775807L, generalNumber.getInt64Value());
    }

    public void testToProtoWithDouble() {
        Double doubleValue = 3.14159;
        FieldValue fieldValue = FieldValueProtoUtils.toProto(doubleValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());

        GeneralNumber generalNumber = fieldValue.getGeneralNumber();
        assertTrue("GeneralNumber should have double value", generalNumber.hasDoubleValue());
        assertEquals("Double value should match", 3.14159, generalNumber.getDoubleValue(), 0.0);
    }

    public void testToProtoWithFloat() {
        Float floatValue = 2.71828f;
        FieldValue fieldValue = FieldValueProtoUtils.toProto(floatValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have general number", fieldValue.hasGeneralNumber());

        GeneralNumber generalNumber = fieldValue.getGeneralNumber();
        assertTrue("GeneralNumber should have float value", generalNumber.hasFloatValue());
        assertEquals("Float value should match", 2.71828f, generalNumber.getFloatValue(), 0.0f);
    }

    public void testToProtoWithString() {
        String stringValue = "test string";
        FieldValue fieldValue = FieldValueProtoUtils.toProto(stringValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have string value", fieldValue.hasStringValue());
        assertEquals("String value should match", "test string", fieldValue.getStringValue());
    }

    public void testToProtoWithBoolean() {
        // Test with true
        Boolean trueValue = true;
        FieldValue trueFieldValue = FieldValueProtoUtils.toProto(trueValue);

        assertNotNull("FieldValue should not be null", trueFieldValue);
        assertTrue("FieldValue should have bool value", trueFieldValue.hasBoolValue());
        assertTrue("Bool value should be true", trueFieldValue.getBoolValue());

        // Test with false
        Boolean falseValue = false;
        FieldValue falseFieldValue = FieldValueProtoUtils.toProto(falseValue);

        assertNotNull("FieldValue should not be null", falseFieldValue);
        assertTrue("FieldValue should have bool value", falseFieldValue.hasBoolValue());
        assertFalse("Bool value should be false", falseFieldValue.getBoolValue());
    }

    public void testToProtoWithEnum() {
        // Use a test enum
        TestEnum enumValue = TestEnum.TEST_VALUE;
        FieldValue fieldValue = FieldValueProtoUtils.toProto(enumValue);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have string value", fieldValue.hasStringValue());
        assertEquals("String value should match enum toString", "TEST_VALUE", fieldValue.getStringValue());
    }

    public void testToProtoWithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("string", "value");
        map.put("integer", 42);
        map.put("boolean", true);

        FieldValue fieldValue = FieldValueProtoUtils.toProto(map);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have object map", fieldValue.hasObjectMap());

        org.opensearch.protobufs.ObjectMap objectMap = fieldValue.getObjectMap();
        assertEquals("ObjectMap should have 3 fields", 3, objectMap.getFieldsCount());

        // Check string field
        assertTrue("String field should exist", objectMap.containsFields("string"));
        assertTrue("String field should have string value", objectMap.getFieldsOrThrow("string").hasString());
        assertEquals("String field should match", "value", objectMap.getFieldsOrThrow("string").getString());

        // Check integer field
        assertTrue("Integer field should exist", objectMap.containsFields("integer"));
        assertTrue("Integer field should have int32 value", objectMap.getFieldsOrThrow("integer").hasInt32());
        assertEquals("Integer field should match", 42, objectMap.getFieldsOrThrow("integer").getInt32());

        // Check boolean field
        assertTrue("Boolean field should exist", objectMap.containsFields("boolean"));
        assertTrue("Boolean field should have bool value", objectMap.getFieldsOrThrow("boolean").hasBool());
        assertTrue("Boolean field should be true", objectMap.getFieldsOrThrow("boolean").getBool());
    }

    public void testToProtoWithNestedMap() {
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("nested_string", "nested value");
        nestedMap.put("nested_integer", 99);

        Map<String, Object> outerMap = new HashMap<>();
        outerMap.put("outer_string", "outer value");
        outerMap.put("nested_map", nestedMap);

        FieldValue fieldValue = FieldValueProtoUtils.toProto(outerMap);

        assertNotNull("FieldValue should not be null", fieldValue);
        assertTrue("FieldValue should have object map", fieldValue.hasObjectMap());

        org.opensearch.protobufs.ObjectMap outerObjectMap = fieldValue.getObjectMap();
        assertEquals("Outer object map should have 2 fields", 2, outerObjectMap.getFieldsCount());

        // Check outer string field
        assertTrue("Outer string field should exist", outerObjectMap.containsFields("outer_string"));
        assertTrue("Outer string field should have string value", outerObjectMap.getFieldsOrThrow("outer_string").hasString());
        assertEquals("Outer string field should match", "outer value", outerObjectMap.getFieldsOrThrow("outer_string").getString());

        // Check nested map field
        assertTrue("Nested map field should exist", outerObjectMap.containsFields("nested_map"));
        assertTrue("Nested map field should have object map", outerObjectMap.getFieldsOrThrow("nested_map").hasObjectMap());

        org.opensearch.protobufs.ObjectMap nestedObjectMap = outerObjectMap.getFieldsOrThrow("nested_map").getObjectMap();
        assertEquals("Nested object map should have 2 fields", 2, nestedObjectMap.getFieldsCount());

        // Check nested string field
        assertTrue("Nested string field should exist", nestedObjectMap.containsFields("nested_string"));
        assertTrue("Nested string field should have string value", nestedObjectMap.getFieldsOrThrow("nested_string").hasString());
        assertEquals("Nested string field should match", "nested value", nestedObjectMap.getFieldsOrThrow("nested_string").getString());

        // Check nested integer field
        assertTrue("Nested integer field should exist", nestedObjectMap.containsFields("nested_integer"));
        assertTrue("Nested integer field should have int32 value", nestedObjectMap.getFieldsOrThrow("nested_integer").hasInt32());
        assertEquals("Nested integer field should match", 99, nestedObjectMap.getFieldsOrThrow("nested_integer").getInt32());
    }

    public void testToProtoWithUnsupportedType() {
        // Create an object of an unsupported type
        Object unsupportedObject = new StringBuilder("unsupported");

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> FieldValueProtoUtils.toProto(unsupportedObject)
        );

        assertTrue("Exception message should mention cannot convert", exception.getMessage().contains("Cannot convert"));
    }

    // Test enum for testing enum conversion
    private enum TestEnum {
        TEST_VALUE
    }
}
