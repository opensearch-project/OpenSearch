/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.common;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StructProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithNull() {
        // Convert null to protobuf Value
        Value value = StructProtoUtils.toProto(null);

        // Verify the result
        assertNotNull("Value should not be null", value);
        assertTrue("Value should have null_value set", value.hasNullValue());
        assertEquals("Null value should be NULL_VALUE", NullValue.NULL_VALUE, value.getNullValue());
    }

    public void testToProtoWithInteger() {
        // Convert Integer to protobuf Value
        Integer intValue = 42;
        Value value = StructProtoUtils.toProto(intValue);

        // Verify the result
        assertNotNull("Value should not be null", value);
        assertTrue("Value should have number_value set", value.hasNumberValue());
        assertEquals("Number value should match", 42.0, value.getNumberValue(), 0.0);
    }

    public void testToProtoWithDouble() {
        // Convert Double to protobuf Value
        Double doubleValue = 3.14159;
        Value value = StructProtoUtils.toProto(doubleValue);

        // Verify the result
        assertNotNull("Value should not be null", value);
        assertTrue("Value should have number_value set", value.hasNumberValue());
        assertEquals("Number value should match", 3.14159, value.getNumberValue(), 0.0);
    }

    public void testToProtoWithString() {
        // Convert String to protobuf Value
        String stringValue = "Hello, World!";
        Value value = StructProtoUtils.toProto(stringValue);

        // Verify the result
        assertNotNull("Value should not be null", value);
        assertTrue("Value should have string_value set", value.hasStringValue());
        assertEquals("String value should match", "Hello, World!", value.getStringValue());
    }

    public void testToProtoWithBoolean() {
        // Convert Boolean to protobuf Value
        Boolean boolValue = true;
        Value value = StructProtoUtils.toProto(boolValue);

        // Verify the result
        assertNotNull("Value should not be null", value);
        assertTrue("Value should have bool_value set", value.hasBoolValue());
        assertEquals("Boolean value should match", true, value.getBoolValue());
    }

    public void testToProtoWithList() {
        // Create a list with mixed types
        List<Object> list = new ArrayList<>();
        list.add("string");
        list.add(42);
        list.add(true);
        list.add(null);

        // Convert List to protobuf Value
        Value value = StructProtoUtils.toProto(list);

        // Verify the result
        assertNotNull("Value should not be null", value);
        assertTrue("Value should have list_value set", value.hasListValue());

        ListValue listValue = value.getListValue();
        assertEquals("List should have 4 elements", 4, listValue.getValuesCount());

        // Verify each element
        assertTrue("First element should be a string", listValue.getValues(0).hasStringValue());
        assertEquals("First element should match", "string", listValue.getValues(0).getStringValue());

        assertTrue("Second element should be a number", listValue.getValues(1).hasNumberValue());
        assertEquals("Second element should match", 42.0, listValue.getValues(1).getNumberValue(), 0.0);

        assertTrue("Third element should be a boolean", listValue.getValues(2).hasBoolValue());
        assertEquals("Third element should match", true, listValue.getValues(2).getBoolValue());

        assertTrue("Fourth element should be null", listValue.getValues(3).hasNullValue());
        assertEquals("Fourth element should be NULL_VALUE", NullValue.NULL_VALUE, listValue.getValues(3).getNullValue());
    }

    public void testToProtoWithMap() {
        // Create a map with mixed types
        Map<String, Object> map = new HashMap<>();
        map.put("string", "value");
        map.put("number", 42);
        map.put("boolean", true);
        map.put("null", null);

        // Convert Map to protobuf Value
        Value value = StructProtoUtils.toProto(map);

        // Verify the result
        assertNotNull("Value should not be null", value);
        assertTrue("Value should have struct_value set", value.hasStructValue());

        Struct struct = value.getStructValue();
        assertEquals("Struct should have 4 fields", 4, struct.getFieldsCount());

        // Verify each field
        assertTrue("string field should be a string", struct.getFieldsOrThrow("string").hasStringValue());
        assertEquals("string field should match", "value", struct.getFieldsOrThrow("string").getStringValue());

        assertTrue("number field should be a number", struct.getFieldsOrThrow("number").hasNumberValue());
        assertEquals("number field should match", 42.0, struct.getFieldsOrThrow("number").getNumberValue(), 0.0);

        assertTrue("boolean field should be a boolean", struct.getFieldsOrThrow("boolean").hasBoolValue());
        assertEquals("boolean field should match", true, struct.getFieldsOrThrow("boolean").getBoolValue());

        assertTrue("null field should be null", struct.getFieldsOrThrow("null").hasNullValue());
        assertEquals("null field should be NULL_VALUE", NullValue.NULL_VALUE, struct.getFieldsOrThrow("null").getNullValue());
    }

    public void testToProtoWithNestedMap() {
        // Create a nested map
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("nestedKey", "nestedValue");

        Map<String, Object> map = new HashMap<>();
        map.put("nested", nestedMap);

        // Convert Map to protobuf Value
        Value value = StructProtoUtils.toProto(map);

        // Verify the result
        assertNotNull("Value should not be null", value);
        assertTrue("Value should have struct_value set", value.hasStructValue());

        Struct struct = value.getStructValue();
        assertEquals("Struct should have 1 field", 1, struct.getFieldsCount());

        // Verify nested field
        assertTrue("nested field should be a struct", struct.getFieldsOrThrow("nested").hasStructValue());

        Struct nestedStruct = struct.getFieldsOrThrow("nested").getStructValue();
        assertEquals("Nested struct should have 1 field", 1, nestedStruct.getFieldsCount());
        assertTrue("nestedKey field should be a string", nestedStruct.getFieldsOrThrow("nestedKey").hasStringValue());
        assertEquals("nestedKey field should match", "nestedValue", nestedStruct.getFieldsOrThrow("nestedKey").getStringValue());
    }

    public void testToProtoWithUnsupportedType() {
        // Create an unsupported type (e.g., a custom class)
        class CustomClass {}
        CustomClass customObject = new CustomClass();

        // Attempt to convert to protobuf Value, should throw IllegalArgumentException
        expectThrows(IllegalArgumentException.class, () -> StructProtoUtils.toProto(customObject));
    }
}
