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

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

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

    public ClusterStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        clusterStatus = null;
        if (in.readBoolean()) {
            clusterStatus = ClusterHealthStatus.fromValue(in.readByte());
        }
        this.nodeInfo = new NodeInfo(in);
        this.nodeStats = new NodeStats(in);
        shardsStats = in.readArray(ShardStats::new, ShardStats[]::new);
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
        out.writeArray(shardsStats);
    }
}
