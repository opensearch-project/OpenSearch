/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Response for dataformat stats containing per-shard results.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatStatsResponse extends BroadcastResponse {

    private final List<DataFormatStatsShardResult> shardResults;
    private final boolean shardLevel;

    public DataFormatStatsResponse(
        List<DataFormatStatsShardResult> shardResults,
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

    public DataFormatStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new DataFormatStatsShardResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (DataFormatStatsShardResult result : shardResults) {
            result.writeTo(out);
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        // Group results by index
        Map<String, List<DataFormatStatsShardResult>> byIndex = new HashMap<>();
        for (DataFormatStatsShardResult result : shardResults) {
            byIndex.computeIfAbsent(result.getShardRouting().getIndexName(), k -> new ArrayList<>()).add(result);
        }

        builder.startObject("indices");
        for (Map.Entry<String, List<DataFormatStatsShardResult>> indexEntry : byIndex.entrySet()) {
            builder.startObject(indexEntry.getKey());

            // Aggregate stats for index level
            builder.startObject("composite");
            List<CompositeShardStats> allStats = new ArrayList<>();
            for (DataFormatStatsShardResult r : indexEntry.getValue()) {
                allStats.add(r.getStats());
            }
            CompositeShardStats.aggregate(allStats).toXContent(builder, params);
            builder.endObject();

            if (shardLevel) {
                builder.startObject("shards");
                // Group results by shard ID. TreeMap keeps shard IDs in deterministic order.
                Map<Integer, List<DataFormatStatsShardResult>> byShard = new TreeMap<>();
                for (DataFormatStatsShardResult r : indexEntry.getValue()) {
                    byShard.computeIfAbsent(r.getShardRouting().shardId().id(), k -> new ArrayList<>()).add(r);
                }
                for (Map.Entry<Integer, List<DataFormatStatsShardResult>> shardEntry : byShard.entrySet()) {
                    builder.startArray(String.valueOf(shardEntry.getKey()));
                    for (DataFormatStatsShardResult r : shardEntry.getValue()) {
                        builder.startObject();
                        builder.field("primary", r.getShardRouting().primary());
                        builder.startObject("composite");
                        r.getStats().toXContent(builder, params);
                        builder.endObject();
                        builder.endObject();
                    }
                    builder.endArray();
                }
                builder.endObject();
            }

            builder.endObject();
        }
        builder.endObject();
    }
}
