/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.ObjectMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting ObjectMap Protocol Buffer types to standard Java objects.
 * This class provides methods to transform Protocol Buffer representations of object maps
 * into their corresponding Java Map, List, and primitive type equivalents.
 */
public class ObjectMapProtoUtils {

    private ObjectMapProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer ObjectMap to a Java Map representation.
     * Similar to {@link XContentParser#map()}, this method transforms the structured
     * Protocol Buffer data into a standard Java Map with appropriate value types.
     *
     * @param objectMap The Protocol Buffer ObjectMap to convert
     * @return A Java Map containing the key-value pairs from the Protocol Buffer ObjectMap
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
     * Converts a Protocol Buffer ObjectMap.Value to an appropriate Java object representation.
     * This method handles various value types (numbers, strings, booleans, lists, nested maps)
     * and converts them to their Java equivalents.
     *
     * @param value The Protocol Buffer ObjectMap.Value to convert
     * @return A Java object representing the value (could be a primitive type, String, List, or Map)
     * @throws UnsupportedOperationException if the value is null, which cannot be added to a Java map
     * @throws IllegalArgumentException if the value type cannot be converted
     */
    public static Object fromProto(ObjectMap.Value value) {
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
