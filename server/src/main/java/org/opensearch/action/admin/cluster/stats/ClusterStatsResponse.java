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

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.IndexMetric;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.Metric;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Transport response for obtaining cluster stats
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStatsResponse extends BaseNodesResponse<ClusterStatsNodeResponse> implements ToXContentFragment {

    final ClusterStatsNodes nodesStats;
    final ClusterStatsIndices indicesStats;
    final ClusterHealthStatus status;
    final long timestamp;
    final String clusterUUID;

    public ClusterStatsResponse(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readVLong();
        // it may be that the cluster-manager switched on us while doing the operation. In this case the status may be null.
        status = in.readOptionalWriteable(ClusterHealthStatus::readFrom);

        String clusterUUID = null;
        MappingStats mappingStats = null;
        AnalysisStats analysisStats = null;
        clusterUUID = in.readOptionalString();
        mappingStats = in.readOptionalWriteable(MappingStats::new);
        analysisStats = in.readOptionalWriteable(AnalysisStats::new);
        this.clusterUUID = clusterUUID;

        // built from nodes rather than from the stream directly
        nodesStats = new ClusterStatsNodes(getNodes());
        indicesStats = new ClusterStatsIndices(getNodes(), mappingStats, analysisStats);
    }

    public ClusterStatsResponse(
        long timestamp,
        String clusterUUID,
        ClusterName clusterName,
        List<ClusterStatsNodeResponse> nodes,
        List<FailedNodeException> failures,
        ClusterState state
    ) {
        this(timestamp, clusterUUID, clusterName, nodes, failures, state, Set.of(Metric.values()), Set.of(IndexMetric.values()));
    }

    public ClusterStatsResponse(
        long timestamp,
        String clusterUUID,
        ClusterName clusterName,
        List<ClusterStatsNodeResponse> nodes,
        List<FailedNodeException> failures,
        ClusterState state,
        Set<Metric> requestedMetrics,
        Set<IndexMetric> indicesMetrics
    ) {
        super(clusterName, nodes, failures);
        this.clusterUUID = clusterUUID;
        this.timestamp = timestamp;
        nodesStats = requestedMetrics.stream().anyMatch(ClusterStatsNodes.NODE_STATS_METRICS::contains)
            ? new ClusterStatsNodes(requestedMetrics, nodes)
            : null;
        MappingStats mappingStats = indicesMetrics.contains(IndexMetric.MAPPINGS) ? MappingStats.of(state) : null;
        AnalysisStats analysisStats = indicesMetrics.contains(IndexMetric.ANALYSIS) ? AnalysisStats.of(state) : null;
        indicesStats = requestedMetrics.contains(Metric.INDICES)
            ? new ClusterStatsIndices(indicesMetrics, nodes, mappingStats, analysisStats)
            : null;
        ClusterHealthStatus status = null;
        for (ClusterStatsNodeResponse response : nodes) {
            // only the cluster-manager node populates the status
            if (response.clusterStatus() != null) {
                status = response.clusterStatus();
                break;
            }
        }
        this.status = status;
    }

    public String getClusterUUID() {
        return this.clusterUUID;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public ClusterHealthStatus getStatus() {
        return this.status;
    }

    public ClusterStatsNodes getNodesStats() {
        return nodesStats;
    }

    public ClusterStatsIndices getIndicesStats() {
        return indicesStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
        out.writeOptionalWriteable(status);
        out.writeOptionalString(clusterUUID);
        if (indicesStats != null) {
            out.writeOptionalWriteable(indicesStats.getMappings());
            out.writeOptionalWriteable(indicesStats.getAnalysis());
        } else {
            out.writeOptionalWriteable(null);
            out.writeOptionalWriteable(null);
        }
    }

    @Override
    protected List<ClusterStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ClusterStatsNodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClusterStatsNodeResponse> nodes) throws IOException {
        // nodeStats and indicesStats are rebuilt from nodes
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("cluster_uuid", getClusterUUID());
        builder.field("timestamp", getTimestamp());
        if (status != null) {
            builder.field("status", status.name().toLowerCase(Locale.ROOT));
        }
        if (indicesStats != null) {
            builder.startObject("indices");
            indicesStats.toXContent(builder, params);
            builder.endObject();
        }
        if (nodesStats != null) {
            builder.startObject("nodes");
            nodesStats.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

}
