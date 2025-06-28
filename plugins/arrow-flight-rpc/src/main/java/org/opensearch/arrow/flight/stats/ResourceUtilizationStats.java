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
 * Resource utilization statistics for Flight transport
 */
class ResourceUtilizationStats implements Writeable, ToXContentFragment {

    final long arrowAllocatorAllocatedBytes;
    final long arrowAllocatorPeakBytes;
    final long directMemoryUsedBytes;
    final int flightServerThreadsActive;
    final int flightServerThreadsTotal;
    final int connectionPoolSize;
    final int channelsActive;

    public ResourceUtilizationStats(
        long arrowAllocatorAllocatedBytes,
        long arrowAllocatorPeakBytes,
        long directMemoryUsedBytes,
        int flightServerThreadsActive,
        int flightServerThreadsTotal,
        int connectionPoolSize,
        int channelsActive
    ) {
        this.arrowAllocatorAllocatedBytes = arrowAllocatorAllocatedBytes;
        this.arrowAllocatorPeakBytes = arrowAllocatorPeakBytes;
        this.directMemoryUsedBytes = directMemoryUsedBytes;
        this.flightServerThreadsActive = flightServerThreadsActive;
        this.flightServerThreadsTotal = flightServerThreadsTotal;
        this.connectionPoolSize = connectionPoolSize;
        this.channelsActive = channelsActive;
    }

    public ResourceUtilizationStats(StreamInput in) throws IOException {
        this.arrowAllocatorAllocatedBytes = in.readVLong();
        this.arrowAllocatorPeakBytes = in.readVLong();
        this.directMemoryUsedBytes = in.readVLong();
        this.flightServerThreadsActive = in.readVInt();
        this.flightServerThreadsTotal = in.readVInt();

        this.connectionPoolSize = in.readVInt();
        this.channelsActive = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(arrowAllocatorAllocatedBytes);
        out.writeVLong(arrowAllocatorPeakBytes);
        out.writeVLong(directMemoryUsedBytes);
        out.writeVInt(flightServerThreadsActive);
        out.writeVInt(flightServerThreadsTotal);

        out.writeVInt(connectionPoolSize);
        out.writeVInt(channelsActive);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("resource_utilization");
        builder.field("arrow_allocator_allocated_bytes", arrowAllocatorAllocatedBytes);
        builder.field("arrow_allocator_peak_bytes", arrowAllocatorPeakBytes);
        builder.field("direct_memory_used_bytes", directMemoryUsedBytes);
        builder.field("flight_server_threads_active", flightServerThreadsActive);
        builder.field("flight_server_threads_total", flightServerThreadsTotal);

        builder.field("connection_pool_size", connectionPoolSize);
        builder.field("channels_active", channelsActive);
        if (flightServerThreadsTotal > 0) {
            builder.field("thread_pool_utilization_percent", (flightServerThreadsActive * 100.0) / flightServerThreadsTotal);
        }
        builder.endObject();
        return builder;
    }

}
