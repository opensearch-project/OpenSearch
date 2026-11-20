/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.action.RestActions;

import java.io.IOException;
import java.util.List;

/**
 * The terms found across all target shards, with the shard accounting and whether the list is complete.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TermsEnumResponse extends ActionResponse implements ToXContentObject {

    /**
     * A shard that failed or never answered.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class ShardFailure extends DefaultShardOperationFailedException {

        /**
         * Creates a failure.
         *
         * @param index the index of the shard
         * @param shardId the shard number
         * @param cause why the shard failed
         */
        public ShardFailure(String index, int shardId, Throwable cause) {
            super(index, shardId, cause);
        }

        /**
         * Reads a failure from the wire.
         *
         * @param in the stream to read from
         * @throws IOException if reading fails
         */
        public ShardFailure(StreamInput in) throws IOException {
            super(in);
        }
    }

    private final int totalShards;
    private final int skippedShards;
    private final ShardFailure[] shardFailures;
    private final List<String> terms;
    private final boolean complete;

    /**
     * Creates a response.
     *
     * @param totalShards the number of shards targeted
     * @param skippedShards the number of shards ruled out by the index filter
     * @param shardFailures the shards that failed or never answered
     * @param terms the sorted, distinct terms
     * @param complete false when a shard failed, timed out or never answered, so terms may be missing
     */
    public TermsEnumResponse(int totalShards, int skippedShards, ShardFailure[] shardFailures, List<String> terms, boolean complete) {
        this.totalShards = totalShards;
        this.skippedShards = skippedShards;
        this.shardFailures = shardFailures;
        this.terms = terms;
        this.complete = complete;
    }

    /**
     * Reads a response from the wire.
     *
     * @param in the stream to read from
     * @throws IOException if reading fails
     */
    public TermsEnumResponse(StreamInput in) throws IOException {
        super(in);
        totalShards = in.readVInt();
        skippedShards = in.readVInt();
        shardFailures = in.readArray(ShardFailure::new, ShardFailure[]::new);
        terms = in.readStringList();
        complete = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalShards);
        out.writeVInt(skippedShards);
        out.writeArray(shardFailures);
        out.writeStringCollection(terms);
        out.writeBoolean(complete);
    }

    /** @return the number of shards targeted */
    public int getTotalShards() {
        return totalShards;
    }

    /** @return the number of shards that answered, including skipped ones */
    public int getSuccessfulShards() {
        return totalShards - shardFailures.length;
    }

    /** @return the number of shards ruled out by the index filter */
    public int getSkippedShards() {
        return skippedShards;
    }

    /** @return the number of shards that failed or never answered */
    public int getFailedShards() {
        return shardFailures.length;
    }

    /** @return the shards that failed or never answered */
    public ShardFailure[] getShardFailures() {
        return shardFailures;
    }

    /** @return the sorted, distinct terms */
    public List<String> getTerms() {
        return terms;
    }

    /** @return false when terms may be missing anywhere in the range */
    public boolean isComplete() {
        return complete;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        RestActions.buildBroadcastShardsHeader(
            builder,
            params,
            totalShards,
            getSuccessfulShards(),
            skippedShards,
            getFailedShards(),
            shardFailures
        );
        builder.startArray("terms");
        for (String term : terms) {
            builder.value(term);
        }
        builder.endArray();
        builder.field("complete", complete);
        builder.endObject();
        return builder;
    }
}
