/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.transport.grpc.proto.response.exceptions.opensearchexception.OpenSearchExceptionProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting Exception objects to Protocol Buffers.
 */
public class ReplicationResponseShardInfoFailureProtoUtils {

    private ReplicationResponseShardInfoFailureProtoUtils() {
        // Utility class, no instances
    }

    /**
     * This method is similar to {@link ReplicationResponse.ShardInfo.Failure#toXContent(XContentBuilder, ToXContent.Params)}
     * This method is overridden by various exception classes, which are hardcoded here.
     *
     * @param exception The ReplicationResponse.ShardInfo.Failure to convert metadata from
     * @return A map containing the exception's metadata as ObjectMap.Value objects
     */
    public static ShardFailure toProto(ReplicationResponse.ShardInfo.Failure exception) throws IOException {
        ShardFailure.Builder shardFailure = ShardFailure.newBuilder();
        if (exception.index() != null) {
            shardFailure.setIndex(exception.index());
        }
        shardFailure.setShard(exception.shardId());
        if (exception.nodeId() != null) {
            shardFailure.setNode(exception.nodeId());
        }
        shardFailure.setReason(OpenSearchExceptionProtoUtils.generateThrowableProto(exception.getCause()));
        shardFailure.setStatus(exception.status().name());
        shardFailure.setPrimary(exception.primary());
        return shardFailure.build();
    }
}
