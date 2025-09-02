/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.common.breaker.ResponseLimitBreachedException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting ResponseLimitBreachedException objects to Protocol Buffers.
 * This class specifically handles the conversion of ResponseLimitBreachedException instances
 * to their Protocol Buffer representation, preserving metadata about response size limits
 * that were exceeded during query execution.
 */
public class ResponseLimitBreachedExceptionProtoUtils {

    private ResponseLimitBreachedExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a ResponseLimitBreachedException to a Protocol Buffer Struct.
     *
     * Similar to {@link ResponseLimitBreachedException#metadataToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param exception The ResponseLimitBreachedException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(ResponseLimitBreachedException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();
        map.put("response_limit", ObjectMapProtoUtils.toProto(exception.getResponseLimit()));
        map.put("limit_entity", ObjectMapProtoUtils.toProto(exception.getLimitEntity()));
        return map;
    }
}
