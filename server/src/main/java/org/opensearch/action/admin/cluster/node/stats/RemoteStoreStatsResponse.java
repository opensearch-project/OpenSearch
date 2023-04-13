/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.RemoteStoreStats;
import org.opensearch.action.admin.indices.stats.RemoteStoreStats;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.Index;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

public class RemoteStoreStatsResponse extends BroadcastResponse {

    private RemoteStoreStats[] shards;

    private Map<ShardRouting, RemoteStoreStats> shardStatsMap;

    RemoteStoreStatsResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(RemoteStoreStats::new, (size) -> new RemoteStoreStats[size]);
    }

    RemoteStoreStatsResponse(
        RemoteStoreStats[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public Map<ShardRouting, RemoteStoreStats> asMap() {
        if (this.shardStatsMap == null) {
            Map<ShardRouting, RemoteStoreStats> shardStatsMap = new HashMap<>();
            for (RemoteStoreStats ss : shards) {
//                shardStatsMap.put(ss.getShardRouting(), ss);
            }
            this.shardStatsMap = unmodifiableMap(shardStatsMap);
        }
        return this.shardStatsMap;
    }

    public RemoteStoreStats[] getShards() {
        return this.shards;
    }

    public RemoteStoreStats getAt(int position) {
        return shards[position];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shards);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        final String level = params.param("level", "indices");
        final boolean isLevelValid = "cluster".equalsIgnoreCase(level)
                                         || "indices".equalsIgnoreCase(level)
                                         || "shards".equalsIgnoreCase(level);
        if (!isLevelValid) {
            throw new IllegalArgumentException("level parameter must be one of [cluster] or [indices] or [shards] but was [" + level + "]");
        }

        Arrays.stream(shards).forEach(shard -> {
            try {
                shard.toXContent(builder, params);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, false);
    }
}
