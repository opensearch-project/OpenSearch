/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Point-in-time snapshot of native memory statistics for a node.
 *
 * <p>Includes process-wide native memory stats (allocated/resident from jemalloc)
 * and per-pool stats for all registered pools (Arrow and virtual).
 *
 * <p>Renders as the body of the {@code native_memory} object inside
 * {@code _nodes/stats/native_memory}.
 *
 * @opensearch.api
 */
public class NativeAllocatorPoolStats implements Writeable, ToXContentFragment {

    private final long nativeAllocatedBytes;
    private final long nativeResidentBytes;
    private final List<PoolStats> pools;

    /**
     * Creates a new stats snapshot.
     *
     * @param nativeAllocatedBytes process-wide native allocated bytes (jemalloc), -1 if unavailable
     * @param nativeResidentBytes process-wide native resident bytes (jemalloc RSS), -1 if unavailable
     * @param pools per-pool stats (Arrow + virtual)
     */
    public NativeAllocatorPoolStats(long nativeAllocatedBytes, long nativeResidentBytes, List<PoolStats> pools) {
        this.nativeAllocatedBytes = nativeAllocatedBytes;
        this.nativeResidentBytes = nativeResidentBytes;
        this.pools = Collections.unmodifiableList(pools);
    }

    /**
     * Deserializes from stream.
     */
    public NativeAllocatorPoolStats(StreamInput in) throws IOException {
        this.nativeAllocatedBytes = in.readLong();
        this.nativeResidentBytes = in.readLong();
        int count = in.readVInt();
        List<PoolStats> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(new PoolStats(in));
        }
        this.pools = Collections.unmodifiableList(list);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(nativeAllocatedBytes);
        out.writeLong(nativeResidentBytes);
        out.writeVInt(pools.size());
        for (PoolStats pool : pools) {
            pool.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("allocated_bytes", nativeAllocatedBytes);
        builder.field("resident_bytes", nativeResidentBytes);

        builder.startObject("pools");
        for (PoolStats pool : pools) {
            pool.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /** Returns process-wide native allocated bytes, or -1 if unavailable. */
    public long getNativeAllocatedBytes() {
        return nativeAllocatedBytes;
    }

    /** Returns process-wide native resident bytes (RSS), or -1 if unavailable. */
    public long getNativeResidentBytes() {
        return nativeResidentBytes;
    }

    /** Returns the per-pool statistics. */
    public List<PoolStats> getPools() {
        return pools;
    }

    /** Returns stats grouped by pool group. Pools without a group use their name as the key. */
    public Map<String, PoolStats> getGroupedStats() {
        // [allocated, peak, limit] — peak uses max (highest watermark) rather than sum
        // because individual pool peaks are not additive (they occur at different times).
        Map<String, long[]> grouped = new LinkedHashMap<>();
        for (PoolStats pool : pools) {
            String g = pool.getGroup() != null ? pool.getGroup() : pool.getName();
            grouped.merge(
                g,
                new long[] { pool.getAllocatedBytes(), pool.getPeakBytes(), pool.getLimitBytes() },
                (a, b) -> new long[] { a[0] + b[0], Math.max(a[1], b[1]), a[2] + b[2] }
            );
        }
        Map<String, PoolStats> result = new LinkedHashMap<>();
        for (var e : grouped.entrySet()) {
            result.put(e.getKey(), new PoolStats(e.getKey(), e.getValue()[0], e.getValue()[1], e.getValue()[2]));
        }
        return result;
    }

    /**
     * BWC: Reads and discards the V_3_7_0 format (3 VLongs + pools with 4 fields each).
     * Returns a dummy instance since the data is discarded.
     */
    public static NativeAllocatorPoolStats readAndDiscardV3_7(StreamInput in) throws IOException {
        in.readVLong(); // rootAllocatedBytes
        in.readVLong(); // rootPeakBytes
        in.readVLong(); // rootLimitBytes
        int count = in.readVInt();
        for (int i = 0; i < count; i++) {
            in.readString();  // name
            in.readVLong();   // allocatedBytes
            in.readVLong();   // peakBytes
            in.readVLong();   // limitBytes
        }
        return new NativeAllocatorPoolStats(-1L, -1L, List.of());
    }

    /**
     * BWC: Writes old V_3_7_0 format for a given stats instance (or null).
     * Format: optional boolean + 3 VLongs + pool list with 4 fields each.
     */
    public static void writeV3_7(StreamOutput out, @Nullable NativeAllocatorPoolStats stats) throws IOException {
        if (stats == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVLong(stats.nativeAllocatedBytes);  // map to rootAllocatedBytes
            out.writeVLong(stats.nativeResidentBytes);   // map to rootPeakBytes
            out.writeVLong(0L);                          // rootLimitBytes (no equivalent)
            out.writeVInt(stats.pools.size());
            for (PoolStats pool : stats.pools) {
                out.writeString(pool.getName());
                out.writeVLong(pool.getAllocatedBytes());
                out.writeVLong(pool.getPeakBytes());
                out.writeVLong(pool.getLimitBytes());
            }
        }
    }

    /**
     * Per-pool statistics snapshot.
     */
    public static class PoolStats implements Writeable, ToXContentFragment {

        private final String name;
        private final long allocatedBytes;
        private final long peakBytes;
        private final long limitBytes;
        private final String group;
        private final long minBytes;

        public PoolStats(String name, long allocatedBytes, long peakBytes, long limitBytes) {
            this(name, allocatedBytes, peakBytes, limitBytes, null, 0L);
        }

        public PoolStats(String name, long allocatedBytes, long peakBytes, long limitBytes, String group, long minBytes) {
            this.name = name;
            this.allocatedBytes = allocatedBytes;
            this.peakBytes = peakBytes;
            this.limitBytes = limitBytes;
            this.group = group;
            this.minBytes = minBytes;
        }

        public PoolStats(StreamInput in) throws IOException {
            this.name = in.readString();
            this.allocatedBytes = in.readVLong();
            this.peakBytes = in.readVLong();
            this.limitBytes = in.readVLong();
            this.group = in.readOptionalString();
            this.minBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(allocatedBytes);
            out.writeVLong(peakBytes);
            out.writeVLong(limitBytes);
            out.writeOptionalString(group);
            out.writeVLong(minBytes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            builder.field("allocated_bytes", allocatedBytes);
            builder.field("limit_bytes", limitBytes);
            builder.field("min_bytes", minBytes);
            if (group != null) {
                builder.field("group", group);
            }
            builder.endObject();
            return builder;
        }

        public String getName() {
            return name;
        }

        public long getAllocatedBytes() {
            return allocatedBytes;
        }

        public long getPeakBytes() {
            return peakBytes;
        }

        public long getLimitBytes() {
            return limitBytes;
        }

        public String getGroup() {
            return group;
        }

        public long getMinBytes() {
            return minBytes;
        }
    }
}
