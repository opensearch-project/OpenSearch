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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
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
        // Performance aggregates
        long totalServerRequests = 0;
        long totalServerRequestsCurrent = 0;
        long totalServerBatches = 0;
        long totalClientBatches = 0;
        long totalClientResponses = 0;
        long totalBytesSent = 0;
        long totalBytesReceived = 0;
        long totalServerRequestTime = 0;
        long totalServerBatchTime = 0;
        long totalClientBatchTime = 0;

        // Reliability aggregates
        long totalClientApplicationErrors = 0;
        long totalClientTransportErrors = 0;
        long totalServerApplicationErrors = 0;
        long totalServerTransportErrors = 0;
        long totalClientStreamsCompleted = 0;
        long totalServerStreamsCompleted = 0;
        long totalUptime = 0;

        // Resource aggregates
        long totalArrowAllocated = 0;
        long totalArrowPeak = 0;
        long totalDirectMemory = 0;
        int totalClientThreadsActive = 0;
        int totalClientThreadsTotal = 0;
        int totalServerThreadsActive = 0;
        int totalServerThreadsTotal = 0;
        int totalConnections = 0;
        int totalChannels = 0;

        for (FlightNodeStats nodeStats : getNodes()) {
            FlightTransportStats stats = nodeStats.getFlightStats();

            // Performance
            totalServerRequests += stats.performance.serverRequestsReceived;
            totalServerRequestsCurrent += stats.performance.serverRequestsCurrent;
            totalServerBatches += stats.performance.serverBatchesSent;
            totalClientBatches += stats.performance.clientBatchesReceived;
            totalClientResponses += stats.performance.clientResponsesReceived;
            totalBytesSent += stats.performance.bytesSent;
            totalBytesReceived += stats.performance.bytesReceived;
            totalServerRequestTime += stats.performance.serverRequestTotalMillis;
            totalServerBatchTime += stats.performance.serverBatchTotalMillis;
            totalClientBatchTime += stats.performance.clientBatchTotalMillis;

            // Reliability
            totalClientApplicationErrors += stats.reliability.clientApplicationErrors;
            totalClientTransportErrors += stats.reliability.clientTransportErrors;
            totalServerApplicationErrors += stats.reliability.serverApplicationErrors;
            totalServerTransportErrors += stats.reliability.serverTransportErrors;
            totalClientStreamsCompleted += stats.reliability.clientStreamsCompleted;
            totalServerStreamsCompleted += stats.reliability.serverStreamsCompleted;
            totalUptime = Math.max(totalUptime, stats.reliability.uptimeMillis);

            // Resources
            totalArrowAllocated += stats.resourceUtilization.arrowAllocatedBytes;
            totalArrowPeak = Math.max(totalArrowPeak, stats.resourceUtilization.arrowPeakBytes);
            totalDirectMemory += stats.resourceUtilization.directMemoryBytes;
            totalClientThreadsActive += stats.resourceUtilization.clientThreadsActive;
            totalClientThreadsTotal += stats.resourceUtilization.clientThreadsTotal;
            totalServerThreadsActive += stats.resourceUtilization.serverThreadsActive;
            totalServerThreadsTotal += stats.resourceUtilization.serverThreadsTotal;
            totalConnections += stats.resourceUtilization.connectionsActive;
            totalChannels += stats.resourceUtilization.channelsActive;
        }

        // Performance stats
        builder.startObject("performance");
        builder.field("server_requests_total", totalServerRequests);
        builder.field("server_requests_current", totalServerRequestsCurrent);
        builder.field("server_batches_sent", totalServerBatches);
        builder.field("client_batches_received", totalClientBatches);
        builder.field("client_responses_received", totalClientResponses);
        builder.field("bytes_sent", totalBytesSent);
        if (params.paramAsBoolean("human", false)) {
            builder.field("bytes_sent_human", new ByteSizeValue(totalBytesSent).toString());
        }
        builder.field("bytes_received", totalBytesReceived);
        if (params.paramAsBoolean("human", false)) {
            builder.field("bytes_received_human", new ByteSizeValue(totalBytesReceived).toString());
        }
        if (totalServerRequests > 0) {
            long avgRequestTime = totalServerRequestTime / totalServerRequests;
            builder.field("server_request_avg_millis", avgRequestTime);
            if (params.paramAsBoolean("human", false)) {
                builder.field("server_request_avg_time", TimeValue.timeValueMillis(avgRequestTime).toString());
            }
        }
        if (totalServerBatches > 0) {
            long avgBatchTime = totalServerBatchTime / totalServerBatches;
            builder.field("server_batch_avg_millis", avgBatchTime);
            if (params.paramAsBoolean("human", false)) {
                builder.field("server_batch_avg_time", TimeValue.timeValueMillis(avgBatchTime).toString());
            }
        }
        if (totalClientBatches > 0) {
            long avgClientBatchTime = totalClientBatchTime / totalClientBatches;
            builder.field("client_batch_avg_millis", avgClientBatchTime);
            if (params.paramAsBoolean("human", false)) {
                builder.field("client_batch_avg_time", TimeValue.timeValueMillis(avgClientBatchTime).toString());
            }
        }
        builder.endObject();

        // Reliability stats
        builder.startObject("reliability");
        builder.field("client_application_errors", totalClientApplicationErrors);
        builder.field("client_transport_errors", totalClientTransportErrors);
        builder.field("server_application_errors", totalServerApplicationErrors);
        builder.field("server_transport_errors", totalServerTransportErrors);
        builder.field("client_streams_completed", totalClientStreamsCompleted);
        builder.field("server_streams_completed", totalServerStreamsCompleted);
        builder.field("cluster_uptime_millis", totalUptime);
        if (params.paramAsBoolean("human", false)) {
            builder.field("cluster_uptime", TimeValue.timeValueMillis(totalUptime).toString());
        }

        long totalErrors = totalClientApplicationErrors + totalClientTransportErrors + totalServerApplicationErrors
            + totalServerTransportErrors;
        long totalStreams = totalClientStreamsCompleted + totalServerStreamsCompleted + totalErrors;
        if (totalStreams > 0) {
            builder.field("cluster_error_rate_percent", (totalErrors * 100.0) / totalStreams);
            builder.field(
                "cluster_success_rate_percent",
                ((totalClientStreamsCompleted + totalServerStreamsCompleted) * 100.0) / totalStreams
            );
        }
        builder.endObject();

        // Resource utilization stats
        builder.startObject("resource_utilization");
        builder.field("arrow_allocated_bytes_total", totalArrowAllocated);
        if (params.paramAsBoolean("human", false)) {
            builder.field("arrow_allocated_total", new ByteSizeValue(totalArrowAllocated).toString());
        }
        builder.field("arrow_peak_bytes_max", totalArrowPeak);
        if (params.paramAsBoolean("human", false)) {
            builder.field("arrow_peak_max", new ByteSizeValue(totalArrowPeak).toString());
        }
        builder.field("direct_memory_bytes_total", totalDirectMemory);
        if (params.paramAsBoolean("human", false)) {
            builder.field("direct_memory_total", new ByteSizeValue(totalDirectMemory).toString());
        }
        builder.field("client_threads_active", totalClientThreadsActive);
        builder.field("client_threads_total", totalClientThreadsTotal);
        builder.field("server_threads_active", totalServerThreadsActive);
        builder.field("server_threads_total", totalServerThreadsTotal);
        builder.field("connections_active", totalConnections);
        builder.field("channels_active", totalChannels);
        if (totalClientThreadsTotal > 0) {
            builder.field("client_thread_utilization_percent", (totalClientThreadsActive * 100.0) / totalClientThreadsTotal);
        }
        if (totalServerThreadsTotal > 0) {
            builder.field("server_thread_utilization_percent", (totalServerThreadsActive * 100.0) / totalServerThreadsTotal);
        }
        builder.endObject();
    }
}
