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

import java.util.List;
import java.util.Map;

/**
 * Utility class for converting generic Java objects to google.protobuf.Struct Protobuf type.
 */
public class StructProtoUtils {

    private StructProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a generic Java Object to its Protocol Buffer representation.
     *
     * @param javaObject The java object to convert
     * @return A Protobuf builder .google.protobuf.Struct representation
     */
    public static Value toProto(Object javaObject) {
        Value.Builder valueBuilder = Value.newBuilder();

        if (javaObject == null) {
            // Null
            valueBuilder.setNullValue(NullValue.NULL_VALUE);
        } else if (javaObject instanceof Number) {
            // Number - use doubleValue() to handle all numeric types
            valueBuilder.setNumberValue(((Number) javaObject).doubleValue());
        } else if (javaObject instanceof String) {
            // String
            valueBuilder.setStringValue((String) javaObject);
        } else if (javaObject instanceof Boolean) {
            // Boolean
            valueBuilder.setBoolValue((Boolean) javaObject);
        } else if (javaObject instanceof List) {
            // List
            ListValue.Builder listBuilder = ListValue.newBuilder();
            for (Object listEntry : (List) javaObject) {
                listBuilder.addValues(toProto(listEntry));
            }
            valueBuilder.setListValue(listBuilder.build());
        } else if (javaObject instanceof Map) {
            // Map

            Struct.Builder structBuilder = Struct.newBuilder();

            @SuppressWarnings("unchecked")
            Map<String, Object> fieldMap = (Map<String, Object>) javaObject;
            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                structBuilder.putFields(entry.getKey(), toProto(entry.getValue()));
            }
            valueBuilder.setStructValue(structBuilder.build());
        } else {
            throw new IllegalArgumentException("Cannot convert " + javaObject.toString() + " to google.protobuf.Struct");
        }

        return valueBuilder.build();
    }
}
