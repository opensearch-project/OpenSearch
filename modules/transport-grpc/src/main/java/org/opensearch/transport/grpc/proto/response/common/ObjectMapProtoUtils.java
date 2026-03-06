/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.common;

import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.ObjectMap;

import java.util.List;
import java.util.Map;

/**
 * Utility class for converting generic Java objects to google.protobuf.Struct Protobuf type.
 *
 * <p>This utility mirrors the REST API serialization logic found in {@link org.opensearch.core.xcontent.XContentBuilder#unknownValue}
 * but produces Protocol Buffer messages instead of XContent (JSON/YAML). It handles generic object
 * serialization for aggregation metadata conversion:
 * <ul>
 *   <li>Map&lt;String, Object&gt; → ObjectMap (via handleMapValue)</li>
 *   <li>List&lt;?&gt; → ListValue (via handleListValue)</li>
 *   <li>Primitive types (String, Integer, Long, Double, Float, Boolean) → corresponding protobuf scalar types</li>
 *   <li>Enums → String representation</li>
 *   <li>null → NULL_VALUE</li>
 * </ul>
 *
 * <p>Related REST serialization:
 * <ul>
 *   <li>Aggregation metadata is serialized in {@link org.opensearch.search.aggregations.InternalAggregation#toXContent}
 *       at line 372 via {@code builder.map(this.metadata)}</li>
 *   <li>Generic object handling in {@link org.opensearch.core.xcontent.XContentBuilder#unknownValue}</li>
 *   <li>Map serialization in {@link org.opensearch.core.xcontent.XContentBuilder#map}</li>
 * </ul>
 *
 * @see org.opensearch.core.xcontent.XContentBuilder#unknownValue
 * @see org.opensearch.core.xcontent.XContentBuilder#map
 * @see org.opensearch.search.aggregations.InternalAggregation#toXContent
 */
public class ObjectMapProtoUtils {

    private ObjectMapProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a generic Java Object to its Protocol Buffer representation.
     *
     * <p>This method parallels {@link org.opensearch.core.xcontent.XContentBuilder#unknownValue}
     * from the REST API layer, which handles generic object serialization to XContent (JSON/YAML).
     * Like the REST counterpart, this method uses pattern matching to determine the appropriate
     * protobuf representation for each Java type.
     *
     * <p>Type conversion mapping:
     * <pre>
     * Java Type                REST (XContent)          gRPC (Protobuf)
     * ---------                ---------------          ---------------
     * null                 →   JSON null            →   NULL_VALUE
     * String               →   string               →   string
     * Integer              →   number               →   int32
     * Long                 →   number               →   int64
     * Double               →   number               →   double
     * Float                →   number               →   float
     * Boolean              →   boolean              →   bool
     * Enum                 →   string (toString)    →   string (toString)
     * List&lt;?&gt;              →   array                →   ListValue
     * Map&lt;String, Object&gt;  →   object               →   ObjectMap
     * </pre>
     *
     * @param javaObject The java object to convert
     * @return A Protobuf ObjectMap.Value representation
     * @throws IllegalArgumentException if the object type is not supported
     * @see org.opensearch.core.xcontent.XContentBuilder#unknownValue
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

        switch (javaObject) {
            case String s -> valueBuilder.setString(s);
            case Integer i -> valueBuilder.setInt32(i);
            case Long l -> valueBuilder.setInt64(l);
            case Double d -> valueBuilder.setDouble(d);
            case Float f -> valueBuilder.setFloat(f);
            case Boolean b -> valueBuilder.setBool(b);
            case Enum<?> e -> valueBuilder.setString(e.toString());
            case List<?> list -> handleListValue(list, valueBuilder);
            case Map<?, ?> m -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) m;
                handleMapValue(map, valueBuilder);
            }
            default -> throw new IllegalArgumentException("Cannot convert " + javaObject + " to google.protobuf.Struct");
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
