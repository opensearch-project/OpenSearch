/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.CreatePITResponse;

import java.io.IOException;

/**
 * Utility class for converting PIT creation responses to protobuf.
 */
public class CreatePitResponseProtoUtils {

    private CreatePitResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an OpenSearch create PIT response to protobuf.
     * This method is equivalent to {@link CreatePitResponse#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param response the OpenSearch response
     * @return the protobuf response
     * @throws IOException if conversion fails
     */
    public static CreatePITResponse toProto(CreatePitResponse response) throws IOException {
        CreatePITResponse.Builder builder = CreatePITResponse.newBuilder();
        builder.setPitId(response.getId());
        builder.setCreationTime(response.getCreationTime());
        builder.setXShards(
            ShardStatisticsProtoUtils.getShardStats(
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getSkippedShards(),
                response.getFailedShards(),
                response.getShardFailures()
            )
        );
        return builder.build();
    }
}
