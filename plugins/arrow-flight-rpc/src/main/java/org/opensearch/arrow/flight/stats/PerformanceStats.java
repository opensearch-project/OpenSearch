/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Performance statistics for Flight transport
 */
class PerformanceStats implements Writeable, ToXContentFragment {

    final long serverRequestsReceived;
    final long serverRequestsCurrent;
    final long serverRequestTotalMillis;
    final long serverRequestAvgMillis;
    final long serverRequestMinMillis;
    final long serverRequestMaxMillis;
    final long serverBatchTotalMillis;
    final long serverBatchAvgMillis;
    final long serverBatchMinMillis;
    final long serverBatchMaxMillis;
    final long clientBatchTotalMillis;
    final long clientBatchAvgMillis;
    final long clientBatchMinMillis;
    final long clientBatchMaxMillis;
    final long clientBatchesReceived;
    final long clientResponsesReceived;
    final long serverBatchesSent;
    final long bytesSent;
    final long bytesReceived;

    public PerformanceStats(
        long serverRequestsReceived,
        long serverRequestsCurrent,
        long serverRequestTotalMillis,
        long serverRequestAvgMillis,
        long serverRequestMinMillis,
        long serverRequestMaxMillis,
        long serverBatchTotalMillis,
        long serverBatchAvgMillis,
        long serverBatchMinMillis,
        long serverBatchMaxMillis,
        long clientBatchTotalMillis,
        long clientBatchAvgMillis,
        long clientBatchMinMillis,
        long clientBatchMaxMillis,
        long clientBatchesReceived,
        long clientResponsesReceived,
        long serverBatchesSent,
        long bytesSent,
        long bytesReceived
    ) {
        this.serverRequestsReceived = serverRequestsReceived;
        this.serverRequestsCurrent = serverRequestsCurrent;
        this.serverRequestTotalMillis = serverRequestTotalMillis;
        this.serverRequestAvgMillis = serverRequestAvgMillis;
        this.serverRequestMinMillis = serverRequestMinMillis;
        this.serverRequestMaxMillis = serverRequestMaxMillis;
        this.serverBatchTotalMillis = serverBatchTotalMillis;
        this.serverBatchAvgMillis = serverBatchAvgMillis;
        this.serverBatchMinMillis = serverBatchMinMillis;
        this.serverBatchMaxMillis = serverBatchMaxMillis;
        this.clientBatchTotalMillis = clientBatchTotalMillis;
        this.clientBatchAvgMillis = clientBatchAvgMillis;
        this.clientBatchMinMillis = clientBatchMinMillis;
        this.clientBatchMaxMillis = clientBatchMaxMillis;
        this.clientBatchesReceived = clientBatchesReceived;
        this.clientResponsesReceived = clientResponsesReceived;
        this.serverBatchesSent = serverBatchesSent;
        this.bytesSent = bytesSent;
        this.bytesReceived = bytesReceived;
    }

    public PerformanceStats(StreamInput in) throws IOException {
        this.serverRequestsReceived = in.readVLong();
        this.serverRequestsCurrent = in.readVLong();
        this.serverRequestTotalMillis = in.readVLong();
        this.serverRequestAvgMillis = in.readVLong();
        this.serverRequestMinMillis = in.readVLong();
        this.serverRequestMaxMillis = in.readVLong();
        this.serverBatchTotalMillis = in.readVLong();
        this.serverBatchAvgMillis = in.readVLong();
        this.serverBatchMinMillis = in.readVLong();
        this.serverBatchMaxMillis = in.readVLong();
        this.clientBatchTotalMillis = in.readVLong();
        this.clientBatchAvgMillis = in.readVLong();
        this.clientBatchMinMillis = in.readVLong();
        this.clientBatchMaxMillis = in.readVLong();
        this.clientBatchesReceived = in.readVLong();
        this.clientResponsesReceived = in.readVLong();
        this.serverBatchesSent = in.readVLong();
        this.bytesSent = in.readVLong();
        this.bytesReceived = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverRequestsReceived);
        out.writeVLong(serverRequestsCurrent);
        out.writeVLong(serverRequestTotalMillis);
        out.writeVLong(serverRequestAvgMillis);
        out.writeVLong(serverRequestMinMillis);
        out.writeVLong(serverRequestMaxMillis);
        out.writeVLong(serverBatchTotalMillis);
        out.writeVLong(serverBatchAvgMillis);
        out.writeVLong(serverBatchMinMillis);
        out.writeVLong(serverBatchMaxMillis);
        out.writeVLong(clientBatchTotalMillis);
        out.writeVLong(clientBatchAvgMillis);
        out.writeVLong(clientBatchMinMillis);
        out.writeVLong(clientBatchMaxMillis);
        out.writeVLong(clientBatchesReceived);
        out.writeVLong(clientResponsesReceived);
        out.writeVLong(serverBatchesSent);
        out.writeVLong(bytesSent);
        out.writeVLong(bytesReceived);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("performance");
        builder.field("server_requests_received", serverRequestsReceived);
        builder.field("server_requests_current", serverRequestsCurrent);
        builder.field("server_request_total_millis", serverRequestTotalMillis);
        if (params.paramAsBoolean("human", false)) {
            builder.field("server_request_total_time", TimeValue.timeValueMillis(serverRequestTotalMillis).toString());
        }
        builder.field("server_request_avg_millis", serverRequestAvgMillis);
        if (params.paramAsBoolean("human", false)) {
            builder.field("server_request_avg_time", TimeValue.timeValueMillis(serverRequestAvgMillis).toString());
        }
        builder.field("server_request_min_millis", serverRequestMinMillis);
        if (params.paramAsBoolean("human", false)) {
            builder.field("server_request_min_time", TimeValue.timeValueMillis(serverRequestMinMillis).toString());
        }
        builder.field("server_request_max_millis", serverRequestMaxMillis);
        if (params.paramAsBoolean("human", false)) {
            builder.field("server_request_max_time", TimeValue.timeValueMillis(serverRequestMaxMillis).toString());
        }
        builder.field("server_batch_total_millis", serverBatchTotalMillis);
        builder.field("server_batch_avg_millis", serverBatchAvgMillis);
        builder.field("server_batch_min_millis", serverBatchMinMillis);
        builder.field("server_batch_max_millis", serverBatchMaxMillis);
        builder.field("server_batches_sent", serverBatchesSent);
        builder.field("client_batch_total_millis", clientBatchTotalMillis);
        builder.field("client_batch_avg_millis", clientBatchAvgMillis);
        builder.field("client_batch_min_millis", clientBatchMinMillis);
        builder.field("client_batch_max_millis", clientBatchMaxMillis);
        builder.field("client_batches_received", clientBatchesReceived);
        builder.field("client_responses_received", clientResponsesReceived);
        builder.field("bytes_sent", bytesSent);
        if (params.paramAsBoolean("human", false)) {
            builder.field("bytes_sent_human", new ByteSizeValue(bytesSent).toString());
        }
        builder.field("bytes_received", bytesReceived);
        if (params.paramAsBoolean("human", false)) {
            builder.field("bytes_received_human", new ByteSizeValue(bytesReceived).toString());
        }
        builder.endObject();
        return builder;
    }

}
