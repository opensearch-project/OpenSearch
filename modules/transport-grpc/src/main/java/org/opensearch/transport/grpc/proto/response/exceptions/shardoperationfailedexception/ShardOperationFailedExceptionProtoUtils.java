/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.GlobalParams;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.snapshots.SnapshotShardFailure;

import java.io.IOException;

/**
 * Utility class for converting ShardOperationFailedException objects to Protocol Buffers.
 */
public class ShardOperationFailedExceptionProtoUtils {

    private ShardOperationFailedExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * This method is similar to {@link org.opensearch.core.action.ShardOperationFailedException#toXContent(XContentBuilder, ToXContent.Params)}
     * This method is overridden by various exception classes, which are hardcoded here.
     *
     * @param exception The ShardOperationFailedException to convert metadata from
     * @param params The global gRPC request parameters
     * @return ShardFailure
     */
    public static ShardFailure toProto(ShardOperationFailedException exception, GlobalParams params) throws IOException {
        if (exception instanceof ShardSearchFailure) {
            return ShardSearchFailureProtoUtils.toProto((ShardSearchFailure) exception, params);
        } else if (exception instanceof SnapshotShardFailure) {
            return SnapshotShardFailureProtoUtils.toProto((SnapshotShardFailure) exception, params);
        } else if (exception instanceof DefaultShardOperationFailedException) {
            return DefaultShardOperationFailedExceptionProtoUtils.toProto((DefaultShardOperationFailedException) exception, params);
        } else if (exception instanceof ReplicationResponse.ShardInfo.Failure) {
            return ReplicationResponseShardInfoFailureProtoUtils.toProto((ReplicationResponse.ShardInfo.Failure) exception, params);
        } else {
            throw new UnsupportedOperationException(
                "Unsupported ShardOperationFailedException " + exception.getClass().getName() + "cannot be converted to proto."
            );
        }
    }
}
