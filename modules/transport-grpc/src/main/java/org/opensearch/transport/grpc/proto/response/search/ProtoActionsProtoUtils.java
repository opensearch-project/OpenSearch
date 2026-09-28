/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.rest.action.RestActions;

import java.io.IOException;

/**
 * Utility class for converting REST-like actions between OpenSearch and Protocol Buffers formats.
 * This class provides methods to transform response components such as shard statistics and
 * broadcast headers to ensure proper communication between the OpenSearch server and gRPC clients.
 */
public class ProtoActionsProtoUtils {

    private ProtoActionsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link RestActions#buildBroadcastShardsHeader(XContentBuilder, ToXContent.Params, int, int, int, int, ShardOperationFailedException[])}
     *
     * @param searchResponseProtoBuilder the response builder to populate with shard statistics
     * @param total the total number of shards
     * @param successful the number of successful shards
     * @param skipped the number of skipped shards
     * @param failed the number of failed shards
     * @param shardFailures the array of shard operation failures
     * @throws IOException if there's an error during conversion
     */
    protected static void buildBroadcastShardsHeader(
        SearchResponse.Builder searchResponseProtoBuilder,
        int total,
        int successful,
        int skipped,
        int failed,
        ShardOperationFailedException[] shardFailures
    ) throws IOException {
        searchResponseProtoBuilder.setXShards(ShardStatisticsProtoUtils.getShardStats(total, successful, skipped, failed, shardFailures));
    }
}
