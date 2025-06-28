/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Performance statistics for Flight transport
 */
class PerformanceStats implements Writeable, ToXContentFragment {

    final long serverRequestsReceived;
    final long serverRequestsCurrent;
    final long serverRequestTimeMillis;
    final long serverRequestAvgTimeMillis;
    final long serverRequestMinTimeMillis;
    final long serverRequestMaxTimeMillis;
    final long serverBatchTimeMillis;
    final long serverBatchAvgTimeMillis;
    final long serverBatchMinTimeMillis;
    final long serverBatchMaxTimeMillis;
    final long clientBatchTimeMillis;
    final long clientBatchAvgTimeMillis;
    final long clientBatchMinTimeMillis;
    final long clientBatchMaxTimeMillis;
    final long clientBatchesReceived;
    final long clientResponsesReceived;
    final long serverBatchesSent;
    final long bytesSentTotal;
    final long bytesReceivedTotal;

    public PerformanceStats(
        long serverRequestsReceived,
        long serverRequestsCurrent,
        long serverRequestTimeMillis,
        long serverRequestAvgTimeMillis,
        long serverRequestMinTimeMillis,
        long serverRequestMaxTimeMillis,
        long serverBatchTimeMillis,
        long serverBatchAvgTimeMillis,
        long serverBatchMinTimeMillis,
        long serverBatchMaxTimeMillis,
        long clientBatchTimeMillis,
        long clientBatchAvgTimeMillis,
        long clientBatchMinTimeMillis,
        long clientBatchMaxTimeMillis,
        long clientBatchesReceived,
        long clientResponsesReceived,
        long serverBatchesSent,
        long bytesSentTotal,
        long bytesReceivedTotal
    ) {
        this.serverRequestsReceived = serverRequestsReceived;
        this.serverRequestsCurrent = serverRequestsCurrent;
        this.serverRequestTimeMillis = serverRequestTimeMillis;
        this.serverRequestAvgTimeMillis = serverRequestAvgTimeMillis;
        this.serverRequestMinTimeMillis = serverRequestMinTimeMillis;
        this.serverRequestMaxTimeMillis = serverRequestMaxTimeMillis;
        this.serverBatchTimeMillis = serverBatchTimeMillis;
        this.serverBatchAvgTimeMillis = serverBatchAvgTimeMillis;
        this.serverBatchMinTimeMillis = serverBatchMinTimeMillis;
        this.serverBatchMaxTimeMillis = serverBatchMaxTimeMillis;
        this.clientBatchTimeMillis = clientBatchTimeMillis;
        this.clientBatchAvgTimeMillis = clientBatchAvgTimeMillis;
        this.clientBatchMinTimeMillis = clientBatchMinTimeMillis;
        this.clientBatchMaxTimeMillis = clientBatchMaxTimeMillis;
        this.clientBatchesReceived = clientBatchesReceived;
        this.clientResponsesReceived = clientResponsesReceived;
        this.serverBatchesSent = serverBatchesSent;
        this.bytesSentTotal = bytesSentTotal;
        this.bytesReceivedTotal = bytesReceivedTotal;
    }

    public PerformanceStats(StreamInput in) throws IOException {
        this.serverRequestsReceived = in.readVLong();
        this.serverRequestsCurrent = in.readVLong();
        this.serverRequestTimeMillis = in.readVLong();
        this.serverRequestAvgTimeMillis = in.readVLong();
        this.serverRequestMinTimeMillis = in.readVLong();
        this.serverRequestMaxTimeMillis = in.readVLong();
        this.serverBatchTimeMillis = in.readVLong();
        this.serverBatchAvgTimeMillis = in.readVLong();
        this.serverBatchMinTimeMillis = in.readVLong();
        this.serverBatchMaxTimeMillis = in.readVLong();
        this.clientBatchTimeMillis = in.readVLong();
        this.clientBatchAvgTimeMillis = in.readVLong();
        this.clientBatchMinTimeMillis = in.readVLong();
        this.clientBatchMaxTimeMillis = in.readVLong();
        this.clientBatchesReceived = in.readVLong();
        this.clientResponsesReceived = in.readVLong();
        this.serverBatchesSent = in.readVLong();
        this.bytesSentTotal = in.readVLong();
        this.bytesReceivedTotal = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverRequestsReceived);
        out.writeVLong(serverRequestsCurrent);
        out.writeVLong(serverRequestTimeMillis);
        out.writeVLong(serverRequestAvgTimeMillis);
        out.writeVLong(serverRequestMinTimeMillis);
        out.writeVLong(serverRequestMaxTimeMillis);
        out.writeVLong(serverBatchTimeMillis);
        out.writeVLong(serverBatchAvgTimeMillis);
        out.writeVLong(serverBatchMinTimeMillis);
        out.writeVLong(serverBatchMaxTimeMillis);
        out.writeVLong(clientBatchTimeMillis);
        out.writeVLong(clientBatchAvgTimeMillis);
        out.writeVLong(clientBatchMinTimeMillis);
        out.writeVLong(clientBatchMaxTimeMillis);
        out.writeVLong(clientBatchesReceived);
        out.writeVLong(clientResponsesReceived);
        out.writeVLong(serverBatchesSent);
        out.writeVLong(bytesSentTotal);
        out.writeVLong(bytesReceivedTotal);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("performance");
        builder.field("server_requests_received", serverRequestsReceived);
        builder.field("server_requests_current", serverRequestsCurrent);
        builder.field("server_request_time_millis", serverRequestTimeMillis);
        builder.field("server_request_avg_time_millis", serverRequestAvgTimeMillis);
        builder.field("server_request_min_time_millis", serverRequestMinTimeMillis);
        builder.field("server_request_max_time_millis", serverRequestMaxTimeMillis);
        builder.field("server_batch_time_millis", serverBatchTimeMillis);
        builder.field("server_batch_avg_time_millis", serverBatchAvgTimeMillis);
        builder.field("server_batch_min_time_millis", serverBatchMinTimeMillis);
        builder.field("server_batch_max_time_millis", serverBatchMaxTimeMillis);
        builder.field("server_batches_sent", serverBatchesSent);
        builder.field("client_batch_time_millis", clientBatchTimeMillis);
        builder.field("client_batch_avg_time_millis", clientBatchAvgTimeMillis);
        builder.field("client_batch_min_time_millis", clientBatchMinTimeMillis);
        builder.field("client_batch_max_time_millis", clientBatchMaxTimeMillis);
        builder.field("client_batches_received", clientBatchesReceived);
        builder.field("client_responses_received", clientResponsesReceived);
        builder.field("bytes_sent_total", bytesSentTotal);
        builder.field("bytes_received_total", bytesReceivedTotal);
        builder.endObject();
        return builder;
    }

}
