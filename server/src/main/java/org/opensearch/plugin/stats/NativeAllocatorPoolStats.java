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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Point-in-time snapshot of native allocator pool statistics for a node.
 *
 * <p>Arrow-agnostic POJO. The plugin that owns the allocator (e.g. {@code arrow-base})
 * constructs instances of this class and exposes them through a
 * {@link NativeAllocatorStatsRegistry} component returned from its
 * {@code createComponents()}. Server is the type's home so that the cross-module
 * dependency from {@code :server} to {@code :libs:opensearch-arrow-spi} is unnecessary,
 * mirroring the placement of {@link AnalyticsBackendNativeMemoryStats}.
 *
 * <p>Renders as the inner body of the {@code native_allocator} object inside
 * {@code _nodes/stats[/native_allocator]} — the caller ({@code NodeStats.toXContent})
 * is responsible for opening the {@code native_allocator} wrapper. Each pool exposes
 * {@code allocated_bytes}, {@code peak_bytes}, and {@code limit_bytes}; root exposes
 * the same.
 *
 * @opensearch.api
 */
public class NativeAllocatorPoolStats implements Writeable, ToXContentFragment {

    private final long rootAllocatedBytes;
    private final long rootPeakBytes;
    private final long rootLimitBytes;
    private final List<PoolStats> pools;

    /**
     * Creates a new stats snapshot from the given values.
     *
     * @param rootAllocatedBytes current bytes allocated by the root
     * @param rootPeakBytes peak bytes ever allocated by the root since process start
     * @param rootLimitBytes configured root limit
     * @param pools per-pool stats
     */
    public NativeAllocatorPoolStats(long rootAllocatedBytes, long rootPeakBytes, long rootLimitBytes, List<PoolStats> pools) {
        this.rootAllocatedBytes = rootAllocatedBytes;
        this.rootPeakBytes = rootPeakBytes;
        this.rootLimitBytes = rootLimitBytes;
        this.pools = Collections.unmodifiableList(pools);
    }

    /**
     * Deserializes from stream.
     *
     * @param in the stream input
     */
    public NativeAllocatorPoolStats(StreamInput in) throws IOException {
        this.rootAllocatedBytes = in.readVLong();
        this.rootPeakBytes = in.readVLong();
        this.rootLimitBytes = in.readVLong();
        int count = in.readVInt();
        List<PoolStats> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(new PoolStats(in));
        }
        this.pools = Collections.unmodifiableList(list);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rootAllocatedBytes);
        out.writeVLong(rootPeakBytes);
        out.writeVLong(rootLimitBytes);
        out.writeVInt(pools.size());
        for (PoolStats pool : pools) {
            pool.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("root");
        builder.field("allocated_bytes", rootAllocatedBytes);
        builder.field("peak_bytes", rootPeakBytes);
        builder.field("limit_bytes", rootLimitBytes);
        builder.endObject();

        builder.startObject("pools");
        for (PoolStats pool : pools) {
            pool.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /** Returns the root allocator's currently allocated bytes. */
    public long getRootAllocatedBytes() {
        return rootAllocatedBytes;
    }

    /** Returns the root allocator's peak allocated bytes since process start. */
    public long getRootPeakBytes() {
        return rootPeakBytes;
    }

    /** Returns the root allocator's configured limit in bytes. */
    public long getRootLimitBytes() {
        return rootLimitBytes;
    }

    /** Returns the per-pool statistics. */
    public List<PoolStats> getPools() {
        return pools;
    }

    /**
     * Per-pool statistics snapshot.
     */
    public static class PoolStats implements Writeable, ToXContentFragment {

        private final String name;
        private final long allocatedBytes;
        private final long peakBytes;
        private final long limitBytes;

        /**
         * Creates a new pool stats snapshot.
         *
         * @param name pool name
         * @param allocatedBytes current allocated bytes
         * @param peakBytes peak bytes ever allocated since process start
         * @param limitBytes configured limit
         */
        public PoolStats(String name, long allocatedBytes, long peakBytes, long limitBytes) {
            this.name = name;
            this.allocatedBytes = allocatedBytes;
            this.peakBytes = peakBytes;
            this.limitBytes = limitBytes;
        }

        /**
         * Deserializes from stream.
         *
         * @param in the stream input
         */
        public PoolStats(StreamInput in) throws IOException {
            this.name = in.readString();
            this.allocatedBytes = in.readVLong();
            this.peakBytes = in.readVLong();
            this.limitBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(allocatedBytes);
            out.writeVLong(peakBytes);
            out.writeVLong(limitBytes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            builder.field("allocated_bytes", allocatedBytes);
            builder.field("peak_bytes", peakBytes);
            builder.field("limit_bytes", limitBytes);
            builder.endObject();
            return builder;
        }

        /** Returns the pool name. */
        public String getName() {
            return name;
        }

        /** Returns the currently allocated bytes. */
        public long getAllocatedBytes() {
            return allocatedBytes;
        }

        /** Returns the peak allocated bytes since process start. */
        public long getPeakBytes() {
            return peakBytes;
        }

        /** Returns the configured limit in bytes. */
        public long getLimitBytes() {
            return limitBytes;
        }
    }
}
