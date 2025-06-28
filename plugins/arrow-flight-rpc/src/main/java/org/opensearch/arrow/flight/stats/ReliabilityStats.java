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
 * Reliability statistics for Flight transport
 */
class ReliabilityStats implements Writeable, ToXContentFragment {

    final long streamErrorsTotal;
    final long connectionErrorsTotal;
    final long timeoutErrorsTotal;
    final long streamsCompletedSuccessfully;
    final long streamsFailedTotal;
    final long uptimeMillis;

    public ReliabilityStats(
        long streamErrorsTotal,
        long connectionErrorsTotal,
        long timeoutErrorsTotal,
        long streamsCompletedSuccessfully,
        long streamsFailedTotal,
        long uptimeMillis
    ) {
        this.streamErrorsTotal = streamErrorsTotal;
        this.connectionErrorsTotal = connectionErrorsTotal;
        this.timeoutErrorsTotal = timeoutErrorsTotal;
        this.streamsCompletedSuccessfully = streamsCompletedSuccessfully;
        this.streamsFailedTotal = streamsFailedTotal;
        this.uptimeMillis = uptimeMillis;
    }

    public ReliabilityStats(StreamInput in) throws IOException {
        this.streamErrorsTotal = in.readVLong();
        this.connectionErrorsTotal = in.readVLong();
        this.timeoutErrorsTotal = in.readVLong();
        this.streamsCompletedSuccessfully = in.readVLong();
        this.streamsFailedTotal = in.readVLong();
        this.uptimeMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(streamErrorsTotal);
        out.writeVLong(connectionErrorsTotal);
        out.writeVLong(timeoutErrorsTotal);
        out.writeVLong(streamsCompletedSuccessfully);
        out.writeVLong(streamsFailedTotal);
        out.writeVLong(uptimeMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("reliability");
        builder.field("stream_errors_total", streamErrorsTotal);
        builder.field("connection_errors_total", connectionErrorsTotal);
        builder.field("timeout_errors_total", timeoutErrorsTotal);
        builder.field("streams_completed_successfully", streamsCompletedSuccessfully);
        builder.field("streams_failed_total", streamsFailedTotal);
        builder.field("uptime_millis", uptimeMillis);

        long totalStreams = streamsCompletedSuccessfully + streamsFailedTotal;
        if (totalStreams > 0) {
            builder.field("success_rate_percent", (streamsCompletedSuccessfully * 100.0) / totalStreams);
        }
        builder.endObject();
        return builder;
    }

}
