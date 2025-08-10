/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.common;

import org.opensearch.protobufs.FieldValue;

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
        if (javaObject == null) {
            throw new IllegalArgumentException("Cannot convert null to FieldValue");
        }

        switch (javaObject) {
            case String s -> fieldValueBuilder.setString(s);
            case Integer i -> fieldValueBuilder.setFloat(i.floatValue());
            case Long l -> fieldValueBuilder.setFloat(l.floatValue());
            case Double d -> fieldValueBuilder.setFloat(d.floatValue());
            case Float f -> fieldValueBuilder.setFloat(f);
            case Boolean b -> fieldValueBuilder.setBool(b);
            case Enum<?> e -> fieldValueBuilder.setString(e.toString());
            case Map<?, ?> m -> {
                // For maps, we'll convert to string representation since FieldValue doesn't support complex objects
                fieldValueBuilder.setString(m.toString());
            }
            default -> throw new IllegalArgumentException("Cannot convert " + javaObject + " to FieldValue");
        }
    }

    // Note: The simplified FieldValue structure in 0.8.0-SNAPSHOT only supports
    // bool, float, string, and null_value fields. Complex objects like maps
    // are converted to string representations.
}
