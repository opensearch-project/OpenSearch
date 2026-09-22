/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Response for per-format index-level stats with format-owned aggregation.
 *
 * @param <T> concrete shard-stats type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatStatsResponse<T extends DataFormatShardStats<T>> extends BroadcastResponse {

    private final List<FormatStatsShardResult<T>> shardResults;
    private final String formatName;
    private final boolean shardLevel;
    private final Writeable.Reader<T> reader;

    public FormatStatsResponse(
        String formatName,
        boolean shardLevel,
        List<FormatStatsShardResult<T>> shardResults,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.formatName = formatName;
        this.shardLevel = shardLevel;
        this.shardResults = shardResults;
        this.reader = null;
    }

    public FormatStatsResponse(StreamInput in, Writeable.Reader<T> reader) throws IOException {
        super(in);
        this.reader = reader;
        this.formatName = in.readString();
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new FormatStatsShardResult<>(in, reader));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(formatName);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (FormatStatsShardResult<T> result : shardResults) {
            result.writeTo(out);
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder b, Params p) throws IOException {
        b.field("format", formatName);
        Map<String, List<FormatStatsShardResult<T>>> byIndex = new HashMap<>();
        for (var r : shardResults) {
            byIndex.computeIfAbsent(r.shardRouting().getIndexName(), k -> new ArrayList<>()).add(r);
        }
        b.startObject("indices");
        for (var entry : byIndex.entrySet()) {
            b.startObject(entry.getKey());
            // format-owned aggregation via T.add()
            T agg = null;
            for (var r : entry.getValue()) {
                if (r.stats() == null) continue;
                agg = (agg == null) ? r.stats() : agg.add(r.stats());
            }
            if (agg != null) agg.toXContent(b, p);
            if (shardLevel) {
                b.startObject("shards");
                Map<Integer, List<FormatStatsShardResult<T>>> byShard = new TreeMap<>();
                for (var r : entry.getValue()) {
                    byShard.computeIfAbsent(r.shardRouting().shardId().id(), k -> new ArrayList<>()).add(r);
                }
                for (var s : byShard.entrySet()) {
                    b.startArray(String.valueOf(s.getKey()));
                    for (var r : s.getValue()) {
                        b.startObject();
                        b.startObject("routing");
                        b.field("state", r.shardRouting().state().toString());
                        b.field("primary", r.shardRouting().primary());
                        b.field("node", r.shardRouting().currentNodeId());
                        b.field("relocating_node", r.shardRouting().relocatingNodeId());
                        b.endObject();
                        if (r.stats() != null) r.stats().toXContent(b, p);
                        b.endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endObject();
        }
        b.endObject();
    }
}
