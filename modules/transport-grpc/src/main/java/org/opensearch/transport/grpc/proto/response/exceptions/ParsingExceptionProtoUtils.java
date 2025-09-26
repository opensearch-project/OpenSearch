/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.core.common.ParsingException;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.core.common.ParsingException.UNKNOWN_POSITION;

/**
 * Utility class for converting ParsingException objects to Protocol Buffers.
 * This class specifically handles the conversion of ParsingException instances
 * to their Protocol Buffer representation, preserving metadata about parsing errors
 * including line and column position information when available.
 */
public class ParsingExceptionProtoUtils {

    private ParsingExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a ParsingException to a Protocol Buffer Struct.
     * This method extracts line and column position information from the exception
     * when available (when not equal to UNKNOWN_POSITION), which helps identify
     * the exact location of parsing errors in the input content.
     *
     * @param exception The ParsingException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(ParsingException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();
        if (exception.getLineNumber() != UNKNOWN_POSITION) {
            map.put("line", ObjectMapProtoUtils.toProto(exception.getLineNumber()));
            map.put("col", ObjectMapProtoUtils.toProto(exception.getColumnNumber()));
        }
        return map;
    }
}
