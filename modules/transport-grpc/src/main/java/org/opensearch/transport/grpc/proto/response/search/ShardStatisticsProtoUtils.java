/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ShardStatistics;
import org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception.ShardOperationFailedExceptionProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting ShardStatistics objects to Protocol Buffers.
 * This class handles the conversion of search operation responses to their
 * Protocol Buffer representation.
 */
public class ShardStatisticsProtoUtils {

    private ShardStatisticsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts shard statistics information to its Protocol Buffer representation.
     * This method is equivalent to {@link ShardStats#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param total the total number of shards
     * @param successful the number of successful shards
     * @param skipped the number of skipped shards
     * @param failed the number of failed shards
     * @param shardFailures the array of shard operation failures
     * @return A Protocol Buffer ShardStatistics representation
     * @throws IOException if there's an error during conversion
     */
    protected static ShardStatistics getShardStats(
        int total,
        int successful,
        int skipped,
        int failed,
        ShardOperationFailedException[] shardFailures
    ) throws IOException {
        ShardStatistics.Builder shardStats = ShardStatistics.newBuilder();
        shardStats.setTotal(total);
        shardStats.setSuccessful(successful);
        if (skipped >= 0) {
            shardStats.setSkipped(skipped);
        }
        shardStats.setFailed(failed);
        if (CollectionUtils.isEmpty(shardFailures) == false) {
            for (ShardOperationFailedException shardFailure : ExceptionsHelper.groupBy(shardFailures)) {
                shardStats.addFailures(ShardOperationFailedExceptionProtoUtils.toProto(shardFailure));
            }
        }
        return shardStats.build();
    }

}
