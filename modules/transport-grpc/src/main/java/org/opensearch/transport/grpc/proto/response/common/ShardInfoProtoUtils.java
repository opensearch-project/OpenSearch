/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.common;

import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.protobufs.ShardInfo;

/**
 * Utility class for converting ReplicationResponse.ShardInfo to protobuf.
 */
public class ShardInfoProtoUtils {

    private ShardInfoProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a ReplicationResponse.ShardInfo to protobuf ShardInfo.
     *
     * @param shardInfo The ReplicationResponse.ShardInfo
     * @return The corresponding protobuf ShardInfo
     */
    public static ShardInfo toProto(ReplicationResponse.ShardInfo shardInfo) {
        ShardInfo.Builder builder = ShardInfo.newBuilder()
            .setTotal(shardInfo.getTotal())
            .setSuccessful(shardInfo.getSuccessful())
            .setFailed(shardInfo.getFailed());

        // Add failure details if any
        if (shardInfo.getFailures() != null && shardInfo.getFailures().length > 0) {
            // For now, we'll skip detailed failure conversion
            // This would require additional proto utils for failure types
        }

        return builder.build();
    }
}
