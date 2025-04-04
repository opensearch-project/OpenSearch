/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.WaitForActiveShards;

/**
 * Handler for bulk requests in gRPC.
 */
public class ActiveShardCountProtoUtils {
    // protected final Settings settings;

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    protected ActiveShardCountProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Sets the active shard count on the bulk request based on the protobuf request.
     * Similar to {@link ActiveShardCount#parseString(String)}
     *
     * @param bulkRequest The bulk request to modify
     * @param request The protobuf request containing the active shard count
     * @return The modified bulk request
     */
    public static org.opensearch.action.bulk.BulkRequest getActiveShardCount(
        org.opensearch.action.bulk.BulkRequest bulkRequest,
        BulkRequest request
    ) {
        if (!request.hasWaitForActiveShards()) {
            return bulkRequest;
        }
        WaitForActiveShards waitForActiveShards = request.getWaitForActiveShards();
        switch (waitForActiveShards.getWaitForActiveShardsCase()) {
            case WaitForActiveShards.WaitForActiveShardsCase.WAIT_FOR_ACTIVE_SHARD_OPTIONS:
                switch (waitForActiveShards.getWaitForActiveShardOptions()) {
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED:
                        throw new UnsupportedOperationException("No mapping for WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED");
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL:
                        bulkRequest.waitForActiveShards(ActiveShardCount.ALL);
                        break;
                    default:
                        bulkRequest.waitForActiveShards(ActiveShardCount.DEFAULT);
                        break;
                }
                break;
            case WaitForActiveShards.WaitForActiveShardsCase.INT32_VALUE:
                bulkRequest.waitForActiveShards(waitForActiveShards.getInt32Value());
                break;
            default:
                throw new UnsupportedOperationException("No mapping for WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED");
        }
        return bulkRequest;
    }
}
