/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response containing Flight transport statistics from multiple nodes
 */
class FlightStatsResponse extends BaseNodesResponse<FlightNodeStats> implements ToXContentObject {

    public FlightStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public FlightStatsResponse(ClusterName clusterName, List<FlightNodeStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<FlightNodeStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(FlightNodeStats::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<FlightNodeStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("cluster_name", getClusterName().value());

        builder.startObject("nodes");
        for (FlightNodeStats nodeStats : getNodes()) {
            builder.startObject(nodeStats.getNode().getId());
            builder.field("name", nodeStats.getNode().getName());
            builder.field(
                "streamAddress",
                nodeStats.getNode().getStreamAddress() != null
                    ? nodeStats.getNode().getStreamAddress().toString()
                    : nodeStats.getNode().getAddress().toString()
            );
            nodeStats.getFlightStats().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();

        // Cluster-wide aggregated stats
        builder.startObject("cluster_stats");
        aggregateClusterStats(builder, params);
        builder.endObject();

        builder.endObject();
        return builder;
    }

    private void aggregateClusterStats(XContentBuilder builder, Params params) throws IOException {
        long totalServerRequests = 0;
        long totalServerRequestsCurrent = 0;
        long totalClientBatches = 0;
        long totalClientResponses = 0;
        long totalBytesSent = 0;
        long totalBytesReceived = 0;
        long totalStreamErrors = 0;

        for (FlightNodeStats nodeStats : getNodes()) {
            FlightTransportStats stats = nodeStats.getFlightStats();
            totalServerRequests += stats.performance.serverRequestsReceived;
            totalServerRequestsCurrent += stats.performance.serverRequestsCurrent;
            totalClientBatches += stats.performance.clientBatchesReceived;
            totalClientResponses += stats.performance.clientResponsesReceived;
            totalBytesSent += stats.performance.bytesSentTotal;
            totalBytesReceived += stats.performance.bytesReceivedTotal;
            totalStreamErrors += stats.reliability.streamErrorsTotal;
        }

        builder.startObject("performance");
        builder.field("total_server_requests", totalServerRequests);
        builder.field("total_server_requests_current", totalServerRequestsCurrent);
        builder.field("total_client_batches", totalClientBatches);
        builder.field("total_client_responses", totalClientResponses);
        builder.field("total_bytes_sent", totalBytesSent);
        builder.field("total_bytes_received", totalBytesReceived);
        builder.endObject();

        builder.startObject("reliability");
        builder.field("total_stream_errors", totalStreamErrors);
        if (totalServerRequests > 0) {
            builder.field("cluster_error_rate_percent", (totalStreamErrors * 100.0) / totalServerRequests);
        }
        builder.endObject();

        // Resource utilization stats are per-node only
    }
}
