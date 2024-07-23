/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport action for obtaining cluster stats from node level
 *
 * @opensearch.internal
 */
public class ClusterStatsNodeResponse extends BaseNodeResponse {

    private final NodeInfo nodeInfo;
    private final NodeStats nodeStats;
    private final ShardStats[] shardsStats;
    private ClusterHealthStatus clusterStatus;
    private AggregatedNodeLevelStats aggregatedNodeLevelStats;

    public ClusterStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        clusterStatus = null;
        if (in.readBoolean()) {
            clusterStatus = ClusterHealthStatus.fromValue(in.readByte());
        }
        this.nodeInfo = new NodeInfo(in);
        this.nodeStats = new NodeStats(in);
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.shardsStats = in.readOptionalArray(ShardStats::new, ShardStats[]::new);
            this.aggregatedNodeLevelStats = in.readOptionalWriteable(AggregatedNodeLevelStats::new);
        } else {
            this.shardsStats = in.readArray(ShardStats::new, ShardStats[]::new);
        }
    }

    public ClusterStatsNodeResponse(
        DiscoveryNode node,
        @Nullable ClusterHealthStatus clusterStatus,
        NodeInfo nodeInfo,
        NodeStats nodeStats,
        ShardStats[] shardsStats
    ) {
        super(node);
        this.nodeInfo = nodeInfo;
        this.nodeStats = nodeStats;
        this.shardsStats = shardsStats;
        this.clusterStatus = clusterStatus;
    }

    public ClusterStatsNodeResponse(
        DiscoveryNode node,
        @Nullable ClusterHealthStatus clusterStatus,
        NodeInfo nodeInfo,
        NodeStats nodeStats,
        ShardStats[] shardsStats,
        boolean useAggregatedNodeLevelResponses
    ) {
        super(node);
        this.nodeInfo = nodeInfo;
        this.nodeStats = nodeStats;
        if (useAggregatedNodeLevelResponses) {
            this.aggregatedNodeLevelStats = new AggregatedNodeLevelStats(node, shardsStats);
        }
        this.shardsStats = shardsStats;
        this.clusterStatus = clusterStatus;
    }

    public NodeInfo nodeInfo() {
        return this.nodeInfo;
    }

    public NodeStats nodeStats() {
        return this.nodeStats;
    }

    /**
     * Cluster Health Status, only populated on cluster-manager nodes.
     */
    @Nullable
    public ClusterHealthStatus clusterStatus() {
        return clusterStatus;
    }

    public ShardStats[] shardsStats() {
        return this.shardsStats;
    }

    public AggregatedNodeLevelStats getAggregatedNodeLevelStats() {
        return aggregatedNodeLevelStats;
    }

    public static ClusterStatsNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ClusterStatsNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (clusterStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(clusterStatus.value());
        }
        nodeInfo.writeTo(out);
        nodeStats.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            if (aggregatedNodeLevelStats != null) {
                out.writeOptionalArray(null);
                out.writeOptionalWriteable(aggregatedNodeLevelStats);
            } else {
                out.writeOptionalArray(shardsStats);
                out.writeOptionalWriteable(null);
            }
        } else {
            out.writeArray(shardsStats);
        }
    }

    /**
     * Node level statistics used for ClusterStatsIndices for _cluster/stats call.
     */
    public class AggregatedNodeLevelStats extends BaseNodeResponse {

        CommonStats commonStats;
        Map<String, CommonStats.AggregatedIndexStats> indexStatsMap;

        protected AggregatedNodeLevelStats(StreamInput in) throws IOException {
            super(in);
            commonStats = in.readOptionalWriteable(CommonStats::new);
            indexStatsMap = in.readMap(StreamInput::readString, CommonStats.AggregatedIndexStats::new);
        }

        protected AggregatedNodeLevelStats(DiscoveryNode node, ShardStats[] indexShardsStats) {
            super(node);
            this.commonStats = new CommonStats();
            this.commonStats.docs = new DocsStats();
            this.commonStats.store = new StoreStats();
            this.commonStats.fieldData = new FieldDataStats();
            this.commonStats.queryCache = new QueryCacheStats();
            this.commonStats.completion = new CompletionStats();
            this.commonStats.segments = new SegmentsStats();
            this.indexStatsMap = new HashMap<>();

            // Index Level Stats
            for (org.opensearch.action.admin.indices.stats.ShardStats shardStats : indexShardsStats) {
                CommonStats.AggregatedIndexStats indexShardStats = this.indexStatsMap.get(shardStats.getShardRouting().getIndexName());
                if (indexShardStats == null) {
                    indexShardStats = new CommonStats.AggregatedIndexStats();
                    this.indexStatsMap.put(shardStats.getShardRouting().getIndexName(), indexShardStats);
                }

                indexShardStats.total++;

                CommonStats shardCommonStats = shardStats.getStats();

                if (shardStats.getShardRouting().primary()) {
                    indexShardStats.primaries++;
                    this.commonStats.docs.add(shardCommonStats.docs);
                }
                this.commonStats.store.add(shardCommonStats.store);
                this.commonStats.fieldData.add(shardCommonStats.fieldData);
                this.commonStats.queryCache.add(shardCommonStats.queryCache);
                this.commonStats.completion.add(shardCommonStats.completion);
                this.commonStats.segments.add(shardCommonStats.segments);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(commonStats);
            out.writeMap(indexStatsMap, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
        }
    }
}
