/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.ObjectMap.ListValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class ObjectMapProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithEmptyMap() {
        // Create an empty ObjectMap
        ObjectMap objectMap = ObjectMap.newBuilder().build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertTrue("Map should be empty", map.isEmpty());
    }

    public void testFromProtoWithStringValue() {
        // Create an ObjectMap with a string value
        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().setString("value").build()).build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertEquals("Map should have 1 entry", 1, map.size());
        assertTrue("Map should contain the key", map.containsKey("key"));
        assertEquals("Value should be a string", "value", map.get("key"));
    }

    public void testFromProtoWithBooleanValue() {
        // Create an ObjectMap with a boolean value
        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().setBool(true).build()).build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertEquals("Map should have 1 entry", 1, map.size());
        assertTrue("Map should contain the key", map.containsKey("key"));
        assertEquals("Value should be a boolean", true, map.get("key"));
    }

    public void testFromProtoWithDoubleValue() {
        // Create an ObjectMap with a double value
        double value = 123.456;
        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().setDouble(value).build()).build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertEquals("Map should have 1 entry", 1, map.size());
        assertTrue("Map should contain the key", map.containsKey("key"));
        assertEquals("Value should be a double", value, map.get("key"));
    }

    public void testFromProtoWithFloatValue() {
        // Create an ObjectMap with a float value
        float value = 123.456f;
        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().setDouble(value).build()).build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertEquals("Map should have 1 entry", 1, map.size());
        assertTrue("Map should contain the key", map.containsKey("key"));
        assertEquals("Value should be a float", value, (double) map.get("key"), 1e-6);
    }

    public void testFromProtoWithInt32Value() {
        // Create an ObjectMap with an int32 value
        int value = 123;
        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().setInt32(value).build()).build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertEquals("Map should have 1 entry", 1, map.size());
        assertTrue("Map should contain the key", map.containsKey("key"));
        assertEquals("Value should be an int32", value, map.get("key"));
    }

    public void testFromProtoWithInt64Value() {
        // Create an ObjectMap with an int64 value
        long value = 123456789L;
        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().setInt64(value).build()).build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertEquals("Map should have 1 entry", 1, map.size());
        assertTrue("Map should contain the key", map.containsKey("key"));
        assertEquals("Value should be an int64", value, map.get("key"));
    }

    public void testFromProtoWithListValue() {
        // Create an ObjectMap with a list value
        ListValue listValue = ListValue.newBuilder()
            .addValue(ObjectMap.Value.newBuilder().setString("value1").build())
            .addValue(ObjectMap.Value.newBuilder().setInt32(123).build())
            .addValue(ObjectMap.Value.newBuilder().setBool(true).build())
            .build();

        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().setListValue(listValue).build()).build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertEquals("Map should have 1 entry", 1, map.size());
        assertTrue("Map should contain the key", map.containsKey("key"));
        assertTrue("Value should be a List", map.get("key") instanceof List);

        List<?> list = (List<?>) map.get("key");
        assertEquals("List should have 3 elements", 3, list.size());
        assertEquals("First element should be a string", "value1", list.get(0));
        assertEquals("Second element should be an int", 123, list.get(1));
        assertEquals("Third element should be a boolean", true, list.get(2));
    }

    public void testFromProtoWithNestedObjectMap() {
        // Create a nested ObjectMap
        ObjectMap nestedMap = ObjectMap.newBuilder()
            .putFields("nestedKey", ObjectMap.Value.newBuilder().setString("nestedValue").build())
            .build();

        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().setObjectMap(nestedMap).build()).build();

        // Convert to Java Map
        Map<String, Object> map = ObjectMapProtoUtils.fromProto(objectMap);

        // Verify the result
        assertNotNull("Map should not be null", map);
        assertEquals("Map should have 1 entry", 1, map.size());
        assertTrue("Map should contain the key", map.containsKey("key"));
        assertTrue("Value should be a Map", map.get("key") instanceof Map);

        Map<?, ?> nested = (Map<?, ?>) map.get("key");
        assertEquals("Nested map should have 1 entry", 1, nested.size());
        assertTrue("Nested map should contain the key", nested.containsKey("nestedKey"));
        assertEquals("Nested value should be a string", "nestedValue", nested.get("nestedKey"));
    }

    public void testFromProtoWithNullValueThrowsException() {
        // Create an ObjectMap with a null value
        ObjectMap objectMap = ObjectMap.newBuilder()
            .putFields("key", ObjectMap.Value.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build())
            .build();

        // Attempt to convert to Java Map, should throw UnsupportedOperationException
        expectThrows(UnsupportedOperationException.class, () -> ObjectMapProtoUtils.fromProto(objectMap));
    }

    public void testFromProtoWithInvalidValueTypeThrowsException() {
        // Create an ObjectMap with an unset value type
        ObjectMap objectMap = ObjectMap.newBuilder().putFields("key", ObjectMap.Value.newBuilder().build()).build();

        // Attempt to convert to Java Map, should throw IllegalArgumentException
        expectThrows(IllegalArgumentException.class, () -> ObjectMapProtoUtils.fromProto(objectMap));
    }
}
