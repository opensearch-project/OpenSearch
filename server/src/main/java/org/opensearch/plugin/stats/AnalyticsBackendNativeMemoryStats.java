/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Immutable stats POJO holding jemalloc memory metrics from the native (Rust) layer.
 * <p>
 * Reports two counters:
 * <ul>
 *   <li>{@code allocated_bytes} – live malloc'd bytes tracked by jemalloc</li>
 *   <li>{@code resident_bytes} – physical RSS attributed to jemalloc arenas</li>
 * </ul>
 * A value of {@code -1} indicates an error reading the metric from the native layer.
 */
public class AnalyticsBackendNativeMemoryStats implements Writeable, ToXContentFragment {

    private final long allocatedBytes;
    private final long residentBytes;

    /**
     * Constructs a new AnalyticsBackendNativeMemoryStats with the given metric values.
     *
     * @param allocatedBytes live malloc'd bytes tracked by jemalloc, or -1 on error
     * @param residentBytes  physical RSS attributed to jemalloc arenas, or -1 on error
     */
    public AnalyticsBackendNativeMemoryStats(long allocatedBytes, long residentBytes) {
        this.allocatedBytes = allocatedBytes;
        this.residentBytes = residentBytes;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public AnalyticsBackendNativeMemoryStats(StreamInput in) throws IOException {
        this.allocatedBytes = in.readLong();
        this.residentBytes = in.readLong();
    }

    /**
     * Returns the number of live malloc'd bytes tracked by jemalloc, or -1 on error.
     */
    public long getAllocatedBytes() {
        return allocatedBytes;
    }

    /**
     * Returns the physical RSS attributed to jemalloc arenas, or -1 on error.
     */
    public long getResidentBytes() {
        return residentBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(allocatedBytes);
        out.writeLong(residentBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("native_memory");
        builder.field("allocated_bytes", allocatedBytes);
        builder.field("resident_bytes", residentBytes);
        builder.endObject();
        return builder;
    }
}
