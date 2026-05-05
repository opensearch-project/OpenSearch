/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.protobufs.WaitForActiveShards;

/**
 * Utility class for handling active shard count settings in gRPC bulk requests.
 * This class provides methods to convert between Protocol Buffer representations
 * and OpenSearch ActiveShardCount objects.
 */
public class ActiveShardCountProtoUtils {
    // protected final Settings settings;

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private ActiveShardCountProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Sets the active shard count on the bulk request based on the protobuf request.
     * Similar to {@link ActiveShardCount#parseString(String)}, this method interprets
     * the wait_for_active_shards parameter from the Protocol Buffer request and applies
     * the appropriate ActiveShardCount setting to the OpenSearch bulk request.
     *
     * @param waitForActiveShards The protobuf object containing the active shard count
     * @return The modified bulk request
     */
    public static ActiveShardCount parseProto(WaitForActiveShards waitForActiveShards) {

        switch (waitForActiveShards.getWaitForActiveShardsCase()) {
            case OPTION:
                switch (waitForActiveShards.getOption()) {
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL:
                        return ActiveShardCount.ALL;
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_NULL:
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED:
                    default:
                        return ActiveShardCount.DEFAULT;
                }
            case COUNT:
                return ActiveShardCount.from(waitForActiveShards.getCount());
            case WAITFORACTIVESHARDS_NOT_SET:
            default:
                return ActiveShardCount.DEFAULT;
        }
    }
}
