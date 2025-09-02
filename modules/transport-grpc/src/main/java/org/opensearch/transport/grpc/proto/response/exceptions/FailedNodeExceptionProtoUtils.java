/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.action.FailedNodeException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting FailedNodeException objects to Protocol Buffers.
 * This class specifically handles the conversion of FailedNodeException instances
 * to their Protocol Buffer representation, preserving metadata about node failures
 * in a distributed OpenSearch cluster.
 */
public class FailedNodeExceptionProtoUtils {

    private FailedNodeExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a FailedNodeException to a Protocol Buffer Struct.
     * Similar to {@link FailedNodeException#metadataToXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The FailedNodeException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(FailedNodeException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();
        map.put("node_id", ObjectMapProtoUtils.toProto(exception.nodeId()));
        return map;
    }
}
