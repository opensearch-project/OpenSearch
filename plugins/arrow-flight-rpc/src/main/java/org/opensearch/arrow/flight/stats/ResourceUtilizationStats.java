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
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Resource utilization statistics for Flight transport
 */
class ResourceUtilizationStats implements Writeable, ToXContentFragment {

    final long arrowAllocatedBytes;
    final long arrowPeakBytes;
    final long directMemoryBytes;
    final int clientThreadsActive;
    final int clientThreadsTotal;
    final int serverThreadsActive;
    final int serverThreadsTotal;
    final int connectionsActive;
    final int channelsActive;

    public ResourceUtilizationStats(
        long arrowAllocatedBytes,
        long arrowPeakBytes,
        long directMemoryBytes,
        int clientThreadsActive,
        int clientThreadsTotal,
        int serverThreadsActive,
        int serverThreadsTotal,
        int connectionsActive,
        int channelsActive
    ) {
        this.arrowAllocatedBytes = arrowAllocatedBytes;
        this.arrowPeakBytes = arrowPeakBytes;
        this.directMemoryBytes = directMemoryBytes;
        this.clientThreadsActive = clientThreadsActive;
        this.clientThreadsTotal = clientThreadsTotal;
        this.serverThreadsActive = serverThreadsActive;
        this.serverThreadsTotal = serverThreadsTotal;
        this.connectionsActive = connectionsActive;
        this.channelsActive = channelsActive;
    }

    public ResourceUtilizationStats(StreamInput in) throws IOException {
        this.arrowAllocatedBytes = in.readVLong();
        this.arrowPeakBytes = in.readVLong();
        this.directMemoryBytes = in.readVLong();
        this.clientThreadsActive = in.readVInt();
        this.clientThreadsTotal = in.readVInt();
        this.serverThreadsActive = in.readVInt();
        this.serverThreadsTotal = in.readVInt();
        this.connectionsActive = in.readVInt();
        this.channelsActive = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(arrowAllocatedBytes);
        out.writeVLong(arrowPeakBytes);
        out.writeVLong(directMemoryBytes);
        out.writeVInt(clientThreadsActive);
        out.writeVInt(clientThreadsTotal);
        out.writeVInt(serverThreadsActive);
        out.writeVInt(serverThreadsTotal);
        out.writeVInt(connectionsActive);
        out.writeVInt(channelsActive);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("resource_utilization");
        builder.field("arrow_allocated_bytes", arrowAllocatedBytes);
        if (params.paramAsBoolean("human", false)) {
            builder.field("arrow_allocated", new ByteSizeValue(arrowAllocatedBytes).toString());
        }
        builder.field("arrow_peak_bytes", arrowPeakBytes);
        if (params.paramAsBoolean("human", false)) {
            builder.field("arrow_peak", new ByteSizeValue(arrowPeakBytes).toString());
        }
        builder.field("direct_memory_bytes", directMemoryBytes);
        if (params.paramAsBoolean("human", false)) {
            builder.field("direct_memory", new ByteSizeValue(directMemoryBytes).toString());
        }
        builder.field("client_threads_active", clientThreadsActive);
        builder.field("client_threads_total", clientThreadsTotal);
        builder.field("server_threads_active", serverThreadsActive);
        builder.field("server_threads_total", serverThreadsTotal);
        builder.field("connections_active", connectionsActive);
        builder.field("channels_active", channelsActive);
        if (clientThreadsTotal > 0) {
            builder.field("client_thread_utilization_percent", (clientThreadsActive * 100.0) / clientThreadsTotal);
        }
        if (serverThreadsTotal > 0) {
            builder.field("server_thread_utilization_percent", (serverThreadsActive * 100.0) / serverThreadsTotal);
        }
        builder.endObject();
        return builder;
    }

}
