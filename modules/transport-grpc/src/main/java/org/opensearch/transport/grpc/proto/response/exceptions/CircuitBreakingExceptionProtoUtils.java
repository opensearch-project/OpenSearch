/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting CircuitBreakingException objects to Protocol Buffers.
 * This class specifically handles the conversion of CircuitBreakingException instances
 * to their Protocol Buffer representation, preserving metadata about memory limits
 * and circuit breaker durability.
 */
public class CircuitBreakingExceptionProtoUtils {

    private CircuitBreakingExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a CircuitBreakingException to a Protocol Buffer Struct.
     * Similar to {@link CircuitBreakingException#metadataToXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The CircuitBreakingException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(CircuitBreakingException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();
        map.put("bytes_wanted", ObjectMapProtoUtils.toProto(exception.getBytesWanted()));
        map.put("bytes_limit", ObjectMapProtoUtils.toProto(exception.getByteLimit()));
        map.put("durability", ObjectMapProtoUtils.toProto(exception.getDurability()));
        return map;
    }
}
