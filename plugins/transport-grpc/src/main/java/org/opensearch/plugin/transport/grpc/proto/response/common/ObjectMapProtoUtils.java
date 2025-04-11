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
     * @return A Protobuf builder .google.protobuf.Struct representation
     */
    public static ObjectMap.Value toProto(Object javaObject) {
        ObjectMap.Value.Builder valueBuilder = ObjectMap.Value.newBuilder();

        if (javaObject == null) {
            // Null
            valueBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
        }
        // TODO does the order we set int, long, double, float, matter?
        else if (javaObject instanceof Integer) {
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
        } else if (javaObject instanceof String) {
            // String
            valueBuilder.setString((String) javaObject);
        } else if (javaObject instanceof Boolean) {
            // Boolean
            valueBuilder.setBool((Boolean) javaObject);
        } else if (javaObject instanceof Enum) {
            // Enum
            valueBuilder.setString(javaObject.toString());
        } else if (javaObject instanceof List) {
            // List
            ObjectMap.ListValue.Builder listBuilder = ObjectMap.ListValue.newBuilder();
            for (Object listEntry : (List) javaObject) {
                listBuilder.addValue(toProto(listEntry));
            }
            valueBuilder.setListValue(listBuilder.build());
        } else if (javaObject instanceof Map) {
            // Map
            ObjectMap.Builder objectMapBuilder = ObjectMap.newBuilder();

            @SuppressWarnings("unchecked")
            Map<String, Object> fieldMap = (Map<String, Object>) javaObject;
            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                objectMapBuilder.putFields(entry.getKey(), toProto(entry.getValue()));
            }
            valueBuilder.setObjectMap(objectMapBuilder.build());
        } else {
            throw new IllegalArgumentException("Cannot convert " + javaObject.toString() + " to google.protobuf.Struct");
        }

        return valueBuilder.build();
    }
}
