/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.parquet;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.action.format.StatsResponseUtil;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Response for parquet stats containing per-shard results.
 * Aggregates across all primary shards for index-level stats by summing numeric leaves.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetStatsResponse extends BroadcastResponse {

    private final List<ParquetStatsShardResult> shardResults;
    private final boolean shardLevel;

    public ParquetStatsResponse(
        List<ParquetStatsShardResult> shardResults,
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

    public ParquetStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new ParquetStatsShardResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (ParquetStatsShardResult result : shardResults) {
            result.writeTo(out);
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        Map<String, List<ParquetStatsShardResult>> byIndex = new HashMap<>();
        for (ParquetStatsShardResult result : shardResults) {
            byIndex.computeIfAbsent(result.getShardRouting().getIndexName(), k -> new ArrayList<>()).add(result);
        }

        builder.startObject("indices");
        for (Map.Entry<String, List<ParquetStatsShardResult>> indexEntry : byIndex.entrySet()) {
            builder.startObject(indexEntry.getKey());

            // Aggregate across all primary shards in this index
            List<BytesReference> primaryStatsBytes = indexEntry.getValue()
                .stream()
                .filter(r -> r.getShardRouting().primary())
                .map(ParquetStatsShardResult::getStatsJsonBytes)
                .collect(Collectors.toList());
            BytesReference aggregated = StatsResponseUtil.mergeStatsBytes(primaryStatsBytes);
            builder.startObject("parquet");
            StatsResponseUtil.inlineJsonBytes(builder, aggregated);
            builder.endObject();

            if (shardLevel) {
                builder.startObject("shards");
                Map<Integer, List<ParquetStatsShardResult>> byShard = new TreeMap<>();
                for (ParquetStatsShardResult r : indexEntry.getValue()) {
                    byShard.computeIfAbsent(r.getShardRouting().shardId().id(), k -> new ArrayList<>()).add(r);
                }
                for (Map.Entry<Integer, List<ParquetStatsShardResult>> shardEntry : byShard.entrySet()) {
                    builder.startArray(String.valueOf(shardEntry.getKey()));
                    for (ParquetStatsShardResult r : shardEntry.getValue()) {
                        builder.startObject();
                        builder.field("primary", r.getShardRouting().primary());
                        builder.startObject("parquet");
                        StatsResponseUtil.inlineJsonBytes(builder, r.getStatsJsonBytes());
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
