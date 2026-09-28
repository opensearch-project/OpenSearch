/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.script.ScriptException;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting ScriptException objects to Protocol Buffers.
 * This class specifically handles the conversion of ScriptException instances
 * to their Protocol Buffer representation, preserving metadata about script errors
 * including script stack traces, language information, and position details.
 */
public class ScriptExceptionProtoUtils {

    private ScriptExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a ScriptException to a Protocol Buffer Struct.
     * Similar to {@link ScriptException#metadataToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param exception The ScriptException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(ScriptException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();

        map.put("script_stack", ObjectMapProtoUtils.toProto(exception.getScriptStack()));
        map.put("script", ObjectMapProtoUtils.toProto(exception.getScript()));
        map.put("lang", ObjectMapProtoUtils.toProto(exception.getLang()));
        if (exception.getPos() != null) {
            map = toProto(map, exception.getPos());
        }
        return map;
    }

    /**
     * Converts a ScriptException.Position to Protocol Buffer format and adds it to the given builder.
     * Similar to {@link ScriptException.Position#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param map The map to add position information to
     * @param pos The ScriptException.Position containing position details
     * @return The updated map with position information added
     */
    public static Map<String, ObjectMap.Value> toProto(Map<String, ObjectMap.Value> map, ScriptException.Position pos) {
        // Create a map for position information
        ObjectMap.Builder positionMapBuilder = ObjectMap.newBuilder();

        // Add position fields
        positionMapBuilder.putFields("offset", ObjectMapProtoUtils.toProto(pos.offset));
        positionMapBuilder.putFields("start", ObjectMapProtoUtils.toProto(pos.start));
        positionMapBuilder.putFields("end", ObjectMapProtoUtils.toProto(pos.end));

        // Create a value with the position map
        ObjectMap.Value positionValue = ObjectMap.Value.newBuilder().setObjectMap(positionMapBuilder.build()).build();

        // Add the position value to the main map
        map.put("position", positionValue);

        return map;
    }
}
