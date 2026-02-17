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
     * Converts the metadata from a ShardSearchFailure to a Protocol Buffer ShardSearchFailure.
     * Similar to {@link ShardSearchFailure#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param exception The ShardSearchFailure to convert
     * @return A Protocol Buffer ShardSearchFailure containing the exception metadata
     */
    public static org.opensearch.protobufs.ShardSearchFailure toProto(ShardSearchFailure exception) throws IOException {
        org.opensearch.protobufs.ShardSearchFailure.Builder shardSearchFailure = org.opensearch.protobufs.ShardSearchFailure.newBuilder();
        shardSearchFailure.setShard(exception.shardId());
        if (exception.index() != null) {
            shardSearchFailure.setIndex(exception.index());
        }
        if (exception.shard() != null && exception.shard().getNodeId() != null) {
            shardSearchFailure.setNode(exception.shard().getNodeId());
        }
        shardSearchFailure.setReason(OpenSearchExceptionProtoUtils.generateThrowableProto(exception.getCause()));
        return shardSearchFailure.build();
    }

    /**
     * Converts the metadata from a ShardSearchFailure to the legacy ShardFailure proto type.
     * This is for backward compatibility with older clients.
     * @deprecated Use {@link #toProto(ShardSearchFailure)} which returns ShardSearchFailure proto type
     *
     * @param exception The ShardSearchFailure to convert
     * @return A Protocol Buffer ShardFailure containing the exception metadata (without primary field)
     */
    @Deprecated
    public static ShardFailure toLegacyProto(ShardSearchFailure exception) throws IOException {
        ShardFailure.Builder shardFailure = ShardFailure.newBuilder();
        shardFailure.setShard(exception.shardId());
        if (exception.index() != null) {
            shardFailure.setIndex(exception.index());
        }
        if (exception.shard() != null && exception.shard().getNodeId() != null) {
            shardFailure.setNode(exception.shard().getNodeId());
        }
        shardFailure.setReason(OpenSearchExceptionProtoUtils.generateThrowableProto(exception.getCause()));
        // Note: primary field is not set for search failures as it's not applicable
        return shardFailure.build();
    }
}
