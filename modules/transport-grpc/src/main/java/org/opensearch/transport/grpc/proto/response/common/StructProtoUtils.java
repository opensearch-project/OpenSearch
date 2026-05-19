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

        switch (javaObject) {
            case null -> valueBuilder.setNullValue(NullValue.NULL_VALUE);
            case Number number -> valueBuilder.setNumberValue(number.doubleValue());
            case String string -> valueBuilder.setStringValue(string);
            case Boolean bool -> valueBuilder.setBoolValue(bool);
            case List<?> list -> {
                ListValue.Builder listBuilder = ListValue.newBuilder();
                for (Object listEntry : list) {
                    listBuilder.addValues(toProto(listEntry));
                }
                valueBuilder.setListValue(listBuilder.build());
            }
            case Map<?, ?> map -> {
                Struct.Builder structBuilder = Struct.newBuilder();

                @SuppressWarnings("unchecked")
                Map<String, Object> fieldMap = (Map<String, Object>) map;
                for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                    structBuilder.putFields(entry.getKey(), toProto(entry.getValue()));
                }
                valueBuilder.setStructValue(structBuilder.build());
            }
            default -> throw new IllegalArgumentException("Cannot convert " + javaObject.toString() + " to google.protobuf.Struct");
        }

        return valueBuilder.build();
    }
}
