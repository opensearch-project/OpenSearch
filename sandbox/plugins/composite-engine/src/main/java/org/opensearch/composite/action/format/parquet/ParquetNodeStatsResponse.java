/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.parquet;

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
 * Response for parquet node-level stats, grouped by node ID.
 * Aggregates across all primary shards per node by summing numeric leaves.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetNodeStatsResponse extends BroadcastResponse {

    private final List<ParquetStatsShardResult> shardResults;
    private final boolean shardLevel;
    private final Map<String, String> nodeNames;

    public ParquetNodeStatsResponse(
        List<ParquetStatsShardResult> shardResults,
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

    public ParquetNodeStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new ParquetStatsShardResult(in));
        }
        this.nodeNames = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (ParquetStatsShardResult result : shardResults) {
            result.writeTo(out);
        }
        out.writeMap(nodeNames, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        Map<String, List<ParquetStatsShardResult>> byNode = new TreeMap<>();
        for (ParquetStatsShardResult result : shardResults) {
            String nodeId = result.getShardRouting().currentNodeId();
            byNode.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(result);
        }

        builder.startObject("nodes");
        for (Map.Entry<String, List<ParquetStatsShardResult>> nodeEntry : byNode.entrySet()) {
            String nodeId = nodeEntry.getKey();
            builder.startObject(nodeId);

            builder.field("name", nodeNames.getOrDefault(nodeId, "<unknown>"));

            // Aggregate across all primary shards on this node
            List<BytesReference> primaryStatsBytes = nodeEntry.getValue()
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
                Map<String, Map<Integer, List<ParquetStatsShardResult>>> byIndex = new HashMap<>();
                for (ParquetStatsShardResult r : nodeEntry.getValue()) {
                    byIndex.computeIfAbsent(r.getShardRouting().getIndexName(), k -> new TreeMap<>())
                        .computeIfAbsent(r.getShardRouting().shardId().id(), k -> new ArrayList<>())
                        .add(r);
                }
                for (Map.Entry<String, Map<Integer, List<ParquetStatsShardResult>>> indexEntry : byIndex.entrySet()) {
                    builder.startObject(indexEntry.getKey());
                    for (Map.Entry<Integer, List<ParquetStatsShardResult>> shardEntry : indexEntry.getValue().entrySet()) {
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
        builder.endObject();
    }
}
