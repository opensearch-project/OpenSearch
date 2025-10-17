/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.transport.grpc.proto.response.exceptions.opensearchexception.OpenSearchExceptionProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting ShardSearchFailure objects to Protocol Buffers.
 */
public class ShardSearchFailureProtoUtils {

    private ShardSearchFailureProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a ShardSearchFailure to a Protocol Buffer Struct.
     * Similar to {@link ShardSearchFailure#toXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The ShardSearchFailure to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static ShardFailure toProto(ShardSearchFailure exception) throws IOException {
        ShardFailure.Builder shardFailure = ShardFailure.newBuilder();
        shardFailure.setShard(exception.shardId());
        shardFailure.setIndex(exception.index());
        if (exception.shard() != null && exception.shard().getNodeId() != null) {
            shardFailure.setNode(exception.shard().getNodeId());
        }
        shardFailure.setReason(OpenSearchExceptionProtoUtils.generateThrowableProto(exception.getCause()));
        return shardFailure.build();
    }
}
