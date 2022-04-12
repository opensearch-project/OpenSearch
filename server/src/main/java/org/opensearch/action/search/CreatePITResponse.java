/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestActions;

import java.io.IOException;

public class CreatePITResponse extends ActionResponse implements StatusToXContentObject {
    private final String id;
    private final int totalShards;
    private final int successfulShards;
    private final int failedShards;
    private final int skippedShards;
    private final ShardSearchFailure[] shardFailures;

    public CreatePITResponse(SearchResponse searchResponse) {
        this.id = searchResponse.pointInTimeId();
        this.totalShards = searchResponse.getTotalShards();
        this.successfulShards = searchResponse.getSuccessfulShards();
        this.failedShards = searchResponse.getFailedShards();
        this.skippedShards = searchResponse.getSkippedShards();
        this.shardFailures = searchResponse.getShardFailures();
    }

    public CreatePITResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        skippedShards = in.readVInt();
        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = ShardSearchFailure.readShardSearchFailure(in);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        RestActions.buildBroadcastShardsHeader(
            builder,
            params,
            getTotalShards(),
            getSuccessfulShards(),
            getSkippedShards(),
            getFailedShards(),
            getShardFailures()
        );
        builder.endObject();
        return builder;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        return this.shardFailures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }
        out.writeString(id);
    }

    public String getId() {
        return id;
    }

    /**
     * The total number of shards the create pit operation was executed on.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the create pit operation was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    public int getSkippedShards() {
        return skippedShards;
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }
}
