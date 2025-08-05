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
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting TooManyBucketsException objects to Protocol Buffers.
 * This class specifically handles the conversion of MultiBucketConsumerService.TooManyBucketsException
 * instances to their Protocol Buffer representation, preserving metadata about aggregation
 * bucket limits that were exceeded during query execution.
 */
public class TooManyBucketsExceptionProtoUtils {

    private TooManyBucketsExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a TooManyBucketsException to a Protocol Buffer Struct.
     * Similar to {@link MultiBucketConsumerService.TooManyBucketsException#metadataToXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The TooManyBucketsException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(MultiBucketConsumerService.TooManyBucketsException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();
        map.put("max_buckets", ObjectMapProtoUtils.toProto(exception.getMaxBuckets()));
        return map;
    }
}
