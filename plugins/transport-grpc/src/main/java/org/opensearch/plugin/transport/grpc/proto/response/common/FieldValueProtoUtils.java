/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.common;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.GeneralNumber;
import org.opensearch.protobufs.ObjectMap;

import java.util.Map;

/**
 * Utility class for converting generic Java objects to Protocol Buffer FieldValue type.
 * This class provides methods to transform Java objects of various types (primitives, strings,
 * maps, etc.) into their corresponding Protocol Buffer representations for gRPC communication.
 */
public class FieldValueProtoUtils {

    private FieldValueProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a generic Java Object to its Protocol Buffer FieldValue representation.
     * This method handles various Java types (Integer, Long, Double, Float, String, Boolean, Enum, Map)
     * and converts them to the appropriate FieldValue type.
     *
     * @param javaObject The Java object to convert
     * @return A Protocol Buffer FieldValue representation of the Java object
     * @throws IllegalArgumentException if the Java object type cannot be converted
     */
    public static FieldValue toProto(Object javaObject) {
        FieldValue.Builder fieldValueBuilder = FieldValue.newBuilder();
        toProto(javaObject, fieldValueBuilder);
        return fieldValueBuilder.build();
    }

    /**
     * Converts a generic Java Object to its Protocol Buffer FieldValue representation.
     * It handles various Java types (Integer, Long, Double, Float, String, Boolean, Enum, Map)
     * and converts them to the appropriate FieldValue type.
     *
     * @param javaObject The Java object to convert
     * @param fieldValueBuilder The builder to populate with the Java object data
     * @throws IllegalArgumentException if the Java object type cannot be converted
     */
    public static void toProto(Object javaObject, FieldValue.Builder fieldValueBuilder) {
        // Use instanceof checks in order of most common types first for better performance
        if (javaObject instanceof String) {
            fieldValueBuilder.setStringValue((String) javaObject);
        } else if (javaObject instanceof Integer) {
            // Integer
            GeneralNumber generalNumber = GeneralNumber.newBuilder().setInt32Value((int) javaObject).build();
            fieldValueBuilder.setGeneralNumber(generalNumber);
        } else if (javaObject instanceof Long) {
            // Long
            GeneralNumber generalNumber = GeneralNumber.newBuilder().setInt64Value((long) javaObject).build();
            fieldValueBuilder.setGeneralNumber(generalNumber);
        } else if (javaObject instanceof Double) {
            // Double
            GeneralNumber generalNumber = GeneralNumber.newBuilder().setDoubleValue((double) javaObject).build();
            fieldValueBuilder.setGeneralNumber(generalNumber);
        } else if (javaObject instanceof Float) {
            // Float
            GeneralNumber generalNumber = GeneralNumber.newBuilder().setFloatValue((float) javaObject).build();
            fieldValueBuilder.setGeneralNumber(generalNumber);
        } else if (javaObject instanceof Boolean) {
            // Boolean
            fieldValueBuilder.setBoolValue((Boolean) javaObject);
        } else if (javaObject instanceof Enum) {
            // Enum
            fieldValueBuilder.setStringValue(javaObject.toString());
        } else if (javaObject instanceof Map) {
            // Map
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) javaObject;
            handleMapValue(map, fieldValueBuilder);
        } else if (javaObject == null) {
            throw new IllegalArgumentException("Cannot convert null to FieldValue");
        } else {
            throw new IllegalArgumentException("Cannot convert " + javaObject.toString() + " to FieldValue");
        }
    }

    /**
     * Helper method to handle Map values.
     *
     * @param map The map to convert
     * @param fieldValueBuilder The builder to populate with the map data
     */
    @SuppressWarnings("unchecked")
    private static void handleMapValue(Map<String, Object> map, FieldValue.Builder fieldValueBuilder) {
        ObjectMap.Builder objectMapBuilder = ObjectMap.newBuilder();

        // Process each map entry
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            objectMapBuilder.putFields(entry.getKey(), ObjectMapProtoUtils.toProto(entry.getValue()));
        }

        fieldValueBuilder.setObjectMap(objectMapBuilder.build());
    }
}
