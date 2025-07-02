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
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Reliability statistics for Flight transport
 */
class ReliabilityStats implements Writeable, ToXContentFragment {

    final long clientApplicationErrors;
    final long clientTransportErrors;
    final long serverApplicationErrors;
    final long serverTransportErrors;
    final long clientStreamsCompleted;
    final long serverStreamsCompleted;
    final long uptimeMillis;

    public ReliabilityStats(
        long clientApplicationErrors,
        long clientTransportErrors,
        long serverApplicationErrors,
        long serverTransportErrors,
        long clientStreamsCompleted,
        long serverStreamsCompleted,
        long uptimeMillis
    ) {
        this.clientApplicationErrors = clientApplicationErrors;
        this.clientTransportErrors = clientTransportErrors;
        this.serverApplicationErrors = serverApplicationErrors;
        this.serverTransportErrors = serverTransportErrors;
        this.clientStreamsCompleted = clientStreamsCompleted;
        this.serverStreamsCompleted = serverStreamsCompleted;
        this.uptimeMillis = uptimeMillis;
    }

    public ReliabilityStats(StreamInput in) throws IOException {
        this.clientApplicationErrors = in.readVLong();
        this.clientTransportErrors = in.readVLong();
        this.serverApplicationErrors = in.readVLong();
        this.serverTransportErrors = in.readVLong();
        this.clientStreamsCompleted = in.readVLong();
        this.serverStreamsCompleted = in.readVLong();
        this.uptimeMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(clientApplicationErrors);
        out.writeVLong(clientTransportErrors);
        out.writeVLong(serverApplicationErrors);
        out.writeVLong(serverTransportErrors);
        out.writeVLong(clientStreamsCompleted);
        out.writeVLong(serverStreamsCompleted);
        out.writeVLong(uptimeMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("reliability");
        builder.field("client_application_errors", clientApplicationErrors);
        builder.field("client_transport_errors", clientTransportErrors);
        builder.field("server_application_errors", serverApplicationErrors);
        builder.field("server_transport_errors", serverTransportErrors);
        builder.field("client_streams_completed", clientStreamsCompleted);
        builder.field("server_streams_completed", serverStreamsCompleted);
        builder.field("uptime_millis", uptimeMillis);
        if (params.paramAsBoolean("human", false)) {
            builder.field("uptime", TimeValue.timeValueMillis(uptimeMillis).toString());
        }
        builder.endObject();
        return builder;
    }

}
