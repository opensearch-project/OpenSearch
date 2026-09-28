/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.document.common;

import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.protobufs.ShardInfo;
import org.opensearch.transport.grpc.proto.response.exceptions.opensearchexception.OpenSearchExceptionProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting ReplicationResponse.ShardInfo objects to Protocol Buffers.
 */
public class ShardInfoProtoUtils {

    private ShardInfoProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a ReplicationResponse.ShardInfo Java object to a protobuf ShardStatistics.
     * Similar to {@link ReplicationResponse.ShardInfo#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param shardInfo The shard information to convert to protobuf format
     * @return The protobuf representation of the shard information
     * @throws IOException If there's an error during conversion
     */
    public static ShardInfo toProto(ReplicationResponse.ShardInfo shardInfo) throws IOException {
        ShardInfo.Builder shardInfoBuilder = ShardInfo.newBuilder();
        shardInfoBuilder.setTotal(shardInfo.getTotal());
        shardInfoBuilder.setSuccessful(shardInfo.getSuccessful());
        shardInfoBuilder.setFailed(shardInfo.getFailed());

        // Add any shard failures
        for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
            shardInfoBuilder.addFailures(toProto(failure));
        }

        return shardInfoBuilder.build();
    }

    /**
     * Converts a ReplicationResponse.ShardInfo.Failure Java object to a protobuf ShardFailure.
     * Similar to {@link ReplicationResponse.ShardInfo.Failure#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param failure The shard failure to convert to protobuf format
     * @return The protobuf representation of the shard failure
     * @throws IOException If there's an error during conversion
     */
    private static ShardFailure toProto(ReplicationResponse.ShardInfo.Failure failure) throws IOException {
        ShardFailure.Builder shardFailure = ShardFailure.newBuilder();
        shardFailure.setIndex(failure.index());
        shardFailure.setShard(failure.shardId());
        shardFailure.setNode(failure.nodeId());
        shardFailure.setReason(OpenSearchExceptionProtoUtils.generateThrowableProto(failure.getCause()));
        shardFailure.setStatus(failure.status().name());
        shardFailure.setPrimary(failure.primary());
        return shardFailure.build();
    }
}
