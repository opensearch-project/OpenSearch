/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.common;

import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

public class ObjectMapProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithNull() {
        // Convert null to Protocol Buffer
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(null);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have null value", value.hasNullValue());
        assertEquals("Null value should be NULL_VALUE_NULL", NullValue.NULL_VALUE_NULL, value.getNullValue());
    }

    public void testToProtoWithInteger() {
        // Convert Integer to Protocol Buffer
        Integer intValue = 42;
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(intValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have int32 value", value.hasInt32());
        assertEquals("Int32 value should match", intValue.intValue(), value.getInt32());
    }

    public void testToProtoWithLong() {
        // Convert Long to Protocol Buffer
        Long longValue = 9223372036854775807L;
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(longValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have int64 value", value.hasInt64());
        assertEquals("Int64 value should match", longValue.longValue(), value.getInt64());
    }

    public void testToProtoWithDouble() {
        // Convert Double to Protocol Buffer
        Double doubleValue = 3.14159;
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(doubleValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have double value", value.hasDouble());
        assertEquals("Double value should match", doubleValue, value.getDouble(), 0.0);
    }

    public void testToProtoWithFloat() {
        // Convert Float to Protocol Buffer
        Float floatValue = 2.71828f;
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(floatValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have float value", value.hasFloat());
        assertEquals("Float value should match", floatValue, value.getFloat(), 0.0f);
    }

    public void testToProtoWithString() {
        // Convert String to Protocol Buffer
        String stringValue = "test string";
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(stringValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have string value", value.hasString());
        assertEquals("String value should match", stringValue, value.getString());
    }

    public void testToProtoWithBoolean() {
        // Convert Boolean to Protocol Buffer
        Boolean boolValue = true;
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(boolValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have bool value", value.hasBool());
        assertEquals("Bool value should match", boolValue, value.getBool());
    }

    public void testToProtoWithEnum() {
        // Convert Enum to Protocol Buffer
        TestEnum enumValue = TestEnum.VALUE_2;
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(enumValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have string value", value.hasString());
        assertEquals("String value should match enum name", enumValue.toString(), value.getString());
    }

    public void testToProtoWithList() {
        // Convert List to Protocol Buffer
        List<Object> listValue = Arrays.asList("string", 42, true);
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(listValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have list value", value.hasListValue());
        assertEquals("List should have correct size", 3, value.getListValue().getValueCount());

        // Verify list elements
        assertTrue("First element should be string", value.getListValue().getValue(0).hasString());
        assertEquals("First element should match", "string", value.getListValue().getValue(0).getString());

        assertTrue("Second element should be int32", value.getListValue().getValue(1).hasInt32());
        assertEquals("Second element should match", 42, value.getListValue().getValue(1).getInt32());

        assertTrue("Third element should be bool", value.getListValue().getValue(2).hasBool());
        assertEquals("Third element should match", true, value.getListValue().getValue(2).getBool());
    }

    public void testToProtoWithEmptyList() {
        // Convert empty List to Protocol Buffer
        List<Object> listValue = Arrays.asList();
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(listValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have list value", value.hasListValue());
        assertEquals("List should be empty", 0, value.getListValue().getValueCount());
    }

    // TODO: ObjectMap functionality changed in protobufs 0.8.0
    /*
    public void testToProtoWithMap() {
        // Convert Map to Protocol Buffer
        Map<String, Object> mapValue = new HashMap<>();
        mapValue.put("string", "value");
        mapValue.put("int", 42);
        mapValue.put("bool", true);

        ObjectMap.Value value = ObjectMapProtoUtils.toProto(mapValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have object map", value.hasObjectMap());
        assertEquals("Map should have correct size", 3, value.getObjectMap().getFieldsCount());

        // Verify map entries
        assertTrue("String entry should exist", value.getObjectMap().containsFields("string"));
        assertTrue("String entry should be string", value.getObjectMap().getFieldsOrThrow("string").hasStringValue());
        assertEquals("String entry should match", "value", value.getObjectMap().getFieldsOrThrow("string").getStringValue());

        assertTrue("Int entry should exist", value.getObjectMap().containsFields("int"));
        assertTrue("Int entry should be int32", value.getObjectMap().getFieldsOrThrow("int").hasInt32());
        assertEquals("Int entry should match", 42, value.getObjectMap().getFieldsOrThrow("int").getInt32());

        assertTrue("Bool entry should exist", value.getObjectMap().containsFields("bool"));
        assertTrue("Bool entry should be bool", value.getObjectMap().getFieldsOrThrow("bool").hasBoolValue());
        assertEquals("Bool entry should match", true, value.getObjectMap().getFieldsOrThrow("bool").getBoolValue());
    }
    */

    /*
    public void testToProtoWithEmptyMap() {
        // Convert empty Map to Protocol Buffer
        Map<String, Object> mapValue = new HashMap<>();
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(mapValue);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have object map", value.hasObjectMap());
        assertEquals("Map should be empty", 0, value.getObjectMap().getFieldsCount());
    }
    */

    /*
    public void testToProtoWithNestedStructures() {
        // Create a nested structure
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("key", "value");

        List<Object> innerList = Arrays.asList(1, 2, 3);

        Map<String, Object> outerMap = new HashMap<>();
        outerMap.put("map", innerMap);
        outerMap.put("list", innerList);

        // Convert to Protocol Buffer
        ObjectMap.Value value = ObjectMapProtoUtils.toProto(outerMap);

        // Verify the conversion
        assertNotNull("Value should not be null", value);
        assertTrue("Should have object map", value.hasObjectMap());
        assertEquals("Map should have correct size", 2, value.getObjectMap().getFieldsCount());

        // Verify nested map
        assertTrue("Nested map should exist", value.getObjectMap().containsFields("map"));
        assertTrue("Nested map should be object map", value.getObjectMap().getFieldsOrThrow("map").hasObjectMap());
        assertEquals(
            "Nested map should have correct size",
            1,
            value.getObjectMap().getFieldsOrThrow("map").getObjectMap().getFieldsCount()
        );
        assertTrue("Nested map key should exist", value.getObjectMap().getFieldsOrThrow("map").getObjectMap().containsFields("key"));
        assertEquals(
            "Nested map value should match",
            "value",
            value.getObjectMap().getFieldsOrThrow("map").getObjectMap().getFieldsOrThrow("key").getStringValue()
        );

        // Verify nested list
        assertTrue("Nested list should exist", value.getObjectMap().containsFields("list"));
        assertTrue("Nested list should be list value", value.getObjectMap().getFieldsOrThrow("list").hasListValue());
        assertEquals(
            "Nested list should have correct size",
            3,
            value.getObjectMap().getFieldsOrThrow("list").getListValue().getValueCount()
        );
        assertEquals(
            "Nested list first element should match",
            1,
            value.getObjectMap().getFieldsOrThrow("list").getListValue().getValue(0).getInt32()
        );
        assertEquals(
            "Nested list second element should match",
            2,
            value.getObjectMap().getFieldsOrThrow("list").getListValue().getValue(1).getInt32()
        );
        assertEquals(
            "Nested list third element should match",
            3,
            value.getObjectMap().getFieldsOrThrow("list").getListValue().getValue(2).getInt32()
        );
    }
    */

    public void testToProtoWithUnsupportedType() {
        // Create an unsupported type (a custom class)
        UnsupportedType unsupportedValue = new UnsupportedType();

        // Attempt to convert to Protocol Buffer, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectMapProtoUtils.toProto(unsupportedValue)
        );

        // Verify the exception message contains the object's toString
        assertTrue("Exception message should contain object's toString", exception.getMessage().contains(unsupportedValue.toString()));
    }

    // Helper enum for testing
    private enum TestEnum {
        VALUE_1,
        VALUE_2,
        VALUE_3
    }

    // Helper class for testing unsupported types
    private static class UnsupportedType {
        @Override
        public String toString() {
            return "UnsupportedType";
        }
    }
}
