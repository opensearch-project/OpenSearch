/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.common;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.ObjectMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting ObjectMap Protobuf type to a Java object.
 */
public class ObjectMapProtoUtils {

    private ObjectMapProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a ObjectMap to Java POJO representation.
     * Similar to {@link XContentParser#map()}
     *
     * @param objectMap The generic protobuf objectMap to convert
     * @return A Protobuf builder .google.protobuf.Struct representation
     */
    public static Map<String, Object> fromProto(ObjectMap objectMap) {

        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, ObjectMap.Value> entry : objectMap.getFieldsMap().entrySet()) {
            map.put(entry.getKey(), fromProto(entry.getValue()));
            // TODO how to keep the type of the map values, instead of having them all as generic 'Object' types'?
        }

        return map;
    }

    /**
     * Converts a ObjectMap.Value to Java POJO representation.
     * Similar to {@link XContentParser#map()}
     *
     * @param value The generic protobuf ObjectMap.Value to convert
     * @return A Protobuf builder .google.protobuf.Struct representation
     */
    private static Object fromProto(ObjectMap.Value value) {
        if (value.hasNullValue()) {
            // Null
            throw new UnsupportedOperationException("Cannot add null value in ObjectMap.value " + value.toString() + " to a Java map.");
        } else if (value.hasDouble()) {
            // Numbers
            return value.getDouble();
        } else if (value.hasFloat()) {
            return value.getFloat();
        } else if (value.hasInt32()) {
            return value.getInt32();
        } else if (value.hasInt64()) {
            return value.getInt64();
        } else if (value.hasString()) {
            // String
            return value.getString();
        } else if (value.hasBool()) {
            // Boolean
            return value.getBool();
        } else if (value.hasListValue()) {
            // List
            List<Object> list = new ArrayList<>();
            for (ObjectMap.Value listEntry : value.getListValue().getValueList()) {
                list.add(fromProto(listEntry));
            }
            return list;
        } else if (value.hasObjectMap()) {
            // Map
            return fromProto(value.getObjectMap());
        } else {
            throw new IllegalArgumentException("Cannot convert " + value.toString() + " to protobuf Object.Value");
        }
    }
}
