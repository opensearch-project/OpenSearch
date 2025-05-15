/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.common;

import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.ObjectMap;

import java.util.List;
import java.util.Map;

/**
 * Utility class for converting generic Java objects to google.protobuf.Struct Protobuf type.
 */
public class ObjectMapProtoUtils {

    private ObjectMapProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a generic Java Object to its Protocol Buffer representation.
     *
     * @param javaObject The java object to convert
     * @return A Protobuf ObjectMap.Value representation
     */
    public static ObjectMap.Value toProto(Object javaObject) {
        ObjectMap.Value.Builder valueBuilder = ObjectMap.Value.newBuilder();
        toProto(javaObject, valueBuilder);
        return valueBuilder.build();
    }

    /**
     * Converts a generic Java Object to its Protocol Buffer representation.
     *
     * @param javaObject The java object to convert
     * @param valueBuilder The builder to populate with the java object data
     */
    public static void toProto(Object javaObject, ObjectMap.Value.Builder valueBuilder) {
        if (javaObject == null) {
            // Null
            valueBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
            return;
        }

        // Use instanceof checks in order of most common types first for better performance
        if (javaObject instanceof String) {
            valueBuilder.setString((String) javaObject);
        } else if (javaObject instanceof Integer) {
            // Integer
            valueBuilder.setInt32((int) javaObject);
        } else if (javaObject instanceof Long) {
            // Long
            valueBuilder.setInt64((long) javaObject);
        } else if (javaObject instanceof Double) {
            // Double
            valueBuilder.setDouble((double) javaObject);
        } else if (javaObject instanceof Float) {
            // Float
            valueBuilder.setFloat((float) javaObject);
        } else if (javaObject instanceof Boolean) {
            // Boolean
            valueBuilder.setBool((Boolean) javaObject);
        } else if (javaObject instanceof Enum) {
            // Enum
            valueBuilder.setString(javaObject.toString());
        } else if (javaObject instanceof List) {
            // List
            handleListValue((List<?>) javaObject, valueBuilder);
        } else if (javaObject instanceof Map) {
            // Map
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) javaObject;
            handleMapValue(map, valueBuilder);
        } else {
            throw new IllegalArgumentException("Cannot convert " + javaObject.toString() + " to google.protobuf.Struct");
        }
    }

    /**
     * Helper method to handle List values.
     *
     * @param list The list to convert
     * @param valueBuilder The builder to populate with the list data
     */
    private static void handleListValue(List<?> list, ObjectMap.Value.Builder valueBuilder) {
        ObjectMap.ListValue.Builder listBuilder = ObjectMap.ListValue.newBuilder();

        // Process each list entry
        for (Object listEntry : list) {
            // Create a new builder for each list entry
            ObjectMap.Value.Builder entryBuilder = ObjectMap.Value.newBuilder();
            toProto(listEntry, entryBuilder);
            listBuilder.addValue(entryBuilder.build());
        }

        valueBuilder.setListValue(listBuilder.build());
    }

    /**
     * Helper method to handle Map values.
     *
     * @param map The map to convert
     * @param valueBuilder The builder to populate with the map data
     */
    @SuppressWarnings("unchecked")
    private static void handleMapValue(Map<String, Object> map, ObjectMap.Value.Builder valueBuilder) {
        ObjectMap.Builder objectMapBuilder = ObjectMap.newBuilder();

        // Process each map entry
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            // Create a new builder for each map value
            ObjectMap.Value.Builder entryValueBuilder = ObjectMap.Value.newBuilder();
            toProto(entry.getValue(), entryValueBuilder);
            objectMapBuilder.putFields(entry.getKey(), entryValueBuilder.build());
        }

        valueBuilder.setObjectMap(objectMapBuilder.build());
    }
}
