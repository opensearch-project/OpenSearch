/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import java.io.IOException;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.snapshots.SnapshotShardFailure;
import org.opensearch.transport.grpc.proto.response.exceptions.ResponseHandlingParams;
import org.opensearch.transport.grpc.proto.response.exceptions.opensearchexception.OpenSearchExceptionProtoUtils;

/**
 * Utility class for converting SnapshotShardFailure objects to Protocol Buffers.
 */
public class SnapshotShardFailureProtoUtils {

    private SnapshotShardFailureProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a SnapshotShardFailure to a Protocol Buffer Struct.
     * Similar to {@link SnapshotShardFailure#toXContent(XContentBuilder, ToXContent.Params)}     *
     *
     * @param exception The SnapshotShardFailure to convert
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static ShardFailure toProto(SnapshotShardFailure exception, ResponseHandlingParams params) throws IOException {
        ShardFailure.Builder shardFailure = ShardFailure.newBuilder();
        if (exception.index() != null) {
            shardFailure.setIndex(exception.index());
        }
        // shardFailure.setIndexUuid(exception.index()); // TODO no field called index_uuid in ShardFailure protos
        shardFailure.setShard(exception.shardId());
        if (exception.nodeId() != null) {
            shardFailure.setNode(exception.nodeId());
        }
        shardFailure.setStatus(exception.status().name());
        shardFailure.setReason(OpenSearchExceptionProtoUtils.generateThrowableProto(exception.getCause(), params));
        return shardFailure.build();
    }
}
