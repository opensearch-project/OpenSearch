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
 * Utility class for converting generic Java objects to google.protobuf.Struct Protobuf type.
 */
public class FieldValueProtoUtils {

    private FieldValueProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a generic Java Object to its Protocol Buffer representation.
     *
     * @param javaObject The java object to convert
     * @return A Protobuf builder .google.protobuf.Struct representation
     */
    public static FieldValue toProto(Object javaObject) {
        FieldValue.Builder fieldValueBuilder = FieldValue.newBuilder();

        if (javaObject instanceof Integer) {
            // Integer
            fieldValueBuilder.setGeneralNumber(GeneralNumber.newBuilder().setInt32Value((int) javaObject).build());
        } else if (javaObject instanceof Long) {
            // Long
            fieldValueBuilder.setGeneralNumber(GeneralNumber.newBuilder().setInt64Value((long) javaObject).build());
        } else if (javaObject instanceof Double) {
            // Double
            fieldValueBuilder.setGeneralNumber(GeneralNumber.newBuilder().setDoubleValue((double) javaObject).build());
        } else if (javaObject instanceof Float) {
            // Float
            fieldValueBuilder.setGeneralNumber(GeneralNumber.newBuilder().setFloatValue((float) javaObject).build());
        } else if (javaObject instanceof String) {
            // String
            fieldValueBuilder.setStringValue((String) javaObject);
        } else if (javaObject instanceof Boolean) {
            // Boolean
            fieldValueBuilder.setBoolValue((Boolean) javaObject);
        } else if (javaObject instanceof Enum) {
            // Enum
            fieldValueBuilder.setStringValue(javaObject.toString());
        } else if (javaObject instanceof Map) {
            // Map
            ObjectMap.Builder objectMapBuilder = ObjectMap.newBuilder();

            @SuppressWarnings("unchecked")
            Map<String, Object> fieldMap = (Map<String, Object>) javaObject;
            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                objectMapBuilder.putFields(entry.getKey(), ObjectMapProtoUtils.toProto(entry.getValue()));
            }
            fieldValueBuilder.setObjectMap(objectMapBuilder.build());
        } else {
            throw new IllegalArgumentException("Cannot convert " + javaObject.toString() + " to google.protobuf.Struct");
        }

        return fieldValueBuilder.build();
    }
}
