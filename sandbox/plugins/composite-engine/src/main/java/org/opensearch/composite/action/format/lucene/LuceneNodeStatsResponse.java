/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.lucene;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
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
 * Response for Lucene node-level stats. Groups shard results by node ID and renders
 * aggregated stats per node, with optional per-shard breakdown.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneNodeStatsResponse extends BroadcastResponse {

    private final List<LuceneNodeStatsShardResult> shardResults;
    private final boolean shardLevel;
    private final Map<String, String> nodeNames;

    public LuceneNodeStatsResponse(
        List<LuceneNodeStatsShardResult> shardResults,
        boolean shardLevel,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardResults = shardResults;
        this.shardLevel = shardLevel;
        // Pre-resolve node names; ClusterState reference would not survive wire transport.
        this.nodeNames = new HashMap<>();
        for (DiscoveryNode node : clusterState.nodes()) {
            this.nodeNames.put(node.getId(), node.getName());
        }
    }

    public LuceneNodeStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new LuceneNodeStatsShardResult(in));
        }
        this.nodeNames = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (LuceneNodeStatsShardResult result : shardResults) {
            result.writeTo(out);
        }
        out.writeMap(nodeNames, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        Map<String, List<LuceneNodeStatsShardResult>> byNode = new TreeMap<>();
        for (LuceneNodeStatsShardResult result : shardResults) {
            String nodeId = result.getShardRouting().currentNodeId();
            byNode.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(result);
        }

        builder.startObject("nodes");
        for (Map.Entry<String, List<LuceneNodeStatsShardResult>> entry : byNode.entrySet()) {
            builder.startObject(entry.getKey());

            builder.field("name", nodeNames.getOrDefault(entry.getKey(), "<unknown>"));

            // Aggregate across all primary shards on this node
            List<BytesReference> primaryStatsBytes = entry.getValue()
                .stream()
                .filter(r -> r.getShardRouting().primary())
                .map(LuceneNodeStatsShardResult::getStatsJsonBytes)
                .collect(Collectors.toList());
            BytesReference aggregated = StatsResponseUtil.mergeStatsBytes(primaryStatsBytes);
            builder.startObject("lucene");
            StatsResponseUtil.inlineJsonBytes(builder, aggregated);
            builder.endObject();

            if (shardLevel) {
                builder.startObject("shards");
                Map<String, Map<Integer, List<LuceneNodeStatsShardResult>>> byIndexAndShard = new TreeMap<>();
                for (LuceneNodeStatsShardResult r : entry.getValue()) {
                    byIndexAndShard.computeIfAbsent(r.getShardRouting().getIndexName(), k -> new TreeMap<>())
                        .computeIfAbsent(r.getShardRouting().shardId().id(), k -> new ArrayList<>())
                        .add(r);
                }
                for (Map.Entry<String, Map<Integer, List<LuceneNodeStatsShardResult>>> indexEntry : byIndexAndShard.entrySet()) {
                    builder.startObject(indexEntry.getKey());
                    for (Map.Entry<Integer, List<LuceneNodeStatsShardResult>> shardEntry : indexEntry.getValue().entrySet()) {
                        builder.startArray(String.valueOf(shardEntry.getKey()));
                        for (LuceneNodeStatsShardResult r : shardEntry.getValue()) {
                            builder.startObject();
                            builder.field("primary", r.getShardRouting().primary());
                            builder.startObject("lucene");
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
        builder.endObject();
    }
}
