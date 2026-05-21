/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.action;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response for parquet analyze containing per-shard results.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetAnalyzeResponse extends BroadcastResponse {

    private final List<ParquetAnalyzeShardResult> shardResults;
    private final boolean shardLevel;

    public ParquetAnalyzeResponse(
        List<ParquetAnalyzeShardResult> shardResults,
        boolean shardLevel,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardResults = shardResults;
        this.shardLevel = shardLevel;
    }

    public ParquetAnalyzeResponse(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new ParquetAnalyzeShardResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (ParquetAnalyzeShardResult result : shardResults) {
            result.writeTo(out);
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        if (shardLevel) {
            builder.startArray("shards");
            for (ParquetAnalyzeShardResult result : shardResults) {
                builder.startObject();
                builder.field("shard", result.getShardRouting().shardId().id());
                builder.field("primary", result.getShardRouting().primary());
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(
                            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                            org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                            BytesReference.toBytes(result.getAnalyzeBytes())
                        )
                ) {
                    builder.field("analyze");
                    builder.copyCurrentStructure(parser);
                }
                builder.endObject();
            }
            builder.endArray();
        } else {
            // Index-level: merge all shard results into one
            if (!shardResults.isEmpty()) {
                // Use the first shard result for index-level (aggregation done in transport)
                ParquetAnalyzeShardResult result = shardResults.get(0);
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(
                            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                            org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                            BytesReference.toBytes(result.getAnalyzeBytes())
                        )
                ) {
                    builder.field("analyze");
                    builder.copyCurrentStructure(parser);
                }
            }
        }
    }
}
