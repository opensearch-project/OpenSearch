/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Remote Store stats response
 *
 * @opensearch.internal
 */
public class RemoteStoreStatsResponse extends BroadcastResponse {

    private final RemoteStoreStats[] remoteStoreStats;

    private Map<String, Map<Integer, List<RemoteStoreStats>>> indexWiseStats;

    private final Logger logger;

    public RemoteStoreStatsResponse(StreamInput in) throws IOException {
        super(in);
        remoteStoreStats = in.readArray(RemoteStoreStats::new, RemoteStoreStats[]::new);
        this.logger = Loggers.getLogger(getClass(), "sample-index");
    }

    public RemoteStoreStatsResponse(
        RemoteStoreStats[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.remoteStoreStats = shards;
        this.logger = Loggers.getLogger(getClass(), "sample-index");
    }

    public RemoteStoreStats[] getRemoteStoreStats() {
        return this.remoteStoreStats;
    }

    public RemoteStoreStats getAt(int position) {
        return remoteStoreStats[position];
    }

    public Map<String, Map<Integer, List<RemoteStoreStats>>> groupByIndexAndShards() {
        if (indexWiseStats != null) {
            return indexWiseStats;
        }
        Set<String> indexNames = new HashSet<>();
        for (RemoteStoreStats shardStat : remoteStoreStats) {
            String indexName = shardStat.getShardRouting().getIndexName();
            indexNames.add(indexName);
        }

        Map<String, Map<Integer, List<RemoteStoreStats>>> indexWiseStats = new HashMap<>();
        for (String indexName : indexNames) {
            Set<RemoteStoreStats> perIndexStats = new HashSet<>();
            for (RemoteStoreStats shardStat : remoteStoreStats) {
                if (shardStat.getShardRouting().getIndexName().equals(indexName)) {
                    perIndexStats.add(shardStat);
                }
            }
            indexWiseStats.put(indexName, groupByShards(perIndexStats));
        }
        this.indexWiseStats = indexWiseStats;
        return indexWiseStats;
    }

    public Map<Integer, List<RemoteStoreStats>> groupByShards(Set<RemoteStoreStats> perIndexStats) {
        Map<Integer, List<RemoteStoreStats>> shardStats = new HashMap<>();
        Set<Integer> shardIds = new HashSet<>();
        for (RemoteStoreStats eachShardStats : perIndexStats) {
            int shardId = eachShardStats.getShardRouting().getId();
            shardIds.add(shardId);
        }

        for (Integer shardId : shardIds) {
            List<RemoteStoreStats> stats = new ArrayList<>();
            for (RemoteStoreStats perShardStats : perIndexStats) {
                if (perShardStats.getShardRouting().getId() == shardId) {
                    stats.add(perShardStats);
                }
            }
            shardStats.put(shardId, stats);
        }
        return shardStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(remoteStoreStats);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        groupByIndexAndShards();
        builder.startObject("indices");
        for (String indexName : indexWiseStats.keySet()) {
            builder.startObject(indexName);
            builder.startObject("shards");
            for (int shardId : indexWiseStats.get(indexName).keySet()) {
                builder.startArray(Integer.toString(shardId));
                for (RemoteStoreStats shardStat : indexWiseStats.get(indexName).get(shardId)) {
                    shardStat.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, false);
    }
}
