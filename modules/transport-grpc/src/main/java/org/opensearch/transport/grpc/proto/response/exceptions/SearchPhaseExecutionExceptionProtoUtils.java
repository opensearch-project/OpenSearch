/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting SearchPhaseExecutionException objects to Protocol Buffers.
 * This class specifically handles the conversion of SearchPhaseExecutionException instances
 * to their Protocol Buffer representation, preserving metadata about search phase failures
 * and associated shard operation failures.
 */
public class SearchPhaseExecutionExceptionProtoUtils {

    private SearchPhaseExecutionExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a SearchPhaseExecutionException to a Protocol Buffer Struct.
     * Similar to {@link SearchPhaseExecutionException#metadataToXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The SearchPhaseExecutionException to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static Map<String, ObjectMap.Value> metadataToProto(SearchPhaseExecutionException exception) {
        Map<String, ObjectMap.Value> map = new HashMap<>();
        map.put("phase", ObjectMapProtoUtils.toProto(exception.getPhaseName()));
        map.put("grouped", ObjectMapProtoUtils.toProto(true));

        ObjectMap.ListValue.Builder listBuilder = ObjectMap.ListValue.newBuilder();
        ShardOperationFailedException[] failures = ExceptionsHelper.groupBy(exception.shardFailures());
        for (ShardOperationFailedException failure : failures) {
            listBuilder.addValue(ShardOperationFailedExceptionProtoUtils.toProto(failure));
        }
        map.put("failed_shards", ObjectMap.Value.newBuilder().setListValue(listBuilder.build()).build());

        return map;
    }
}
