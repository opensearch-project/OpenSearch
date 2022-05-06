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

/**
 * Create point in time response with point in time id and shard success / failures
 */
public class CreatePitResponse extends ActionResponse implements StatusToXContentObject {
    // point in time id
    private final String id;
    private final int totalShards;
    private final int successfulShards;
    private final int failedShards;
    private final int skippedShards;
    private final ShardSearchFailure[] shardFailures;
    private final long creationTime;

    public CreatePitResponse(SearchResponse searchResponse, long creationTime) {
        if (searchResponse.pointInTimeId() == null || searchResponse.pointInTimeId().isEmpty()) {
            throw new IllegalArgumentException("Point in time ID is empty");
        }
        this.id = searchResponse.pointInTimeId();
        this.totalShards = searchResponse.getTotalShards();
        this.successfulShards = searchResponse.getSuccessfulShards();
        this.failedShards = searchResponse.getFailedShards();
        this.skippedShards = searchResponse.getSkippedShards();
        this.shardFailures = searchResponse.getShardFailures();
        this.creationTime = creationTime;
    }

    public CreatePitResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        skippedShards = in.readVInt();
        creationTime = in.readLong();
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
        builder.field("creationTime", creationTime);
        builder.endObject();
        return builder;
    }

    public long getCreationTime() {
        return creationTime;
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
        out.writeString(id);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeVInt(skippedShards);
        out.writeLong(creationTime);
        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }
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
