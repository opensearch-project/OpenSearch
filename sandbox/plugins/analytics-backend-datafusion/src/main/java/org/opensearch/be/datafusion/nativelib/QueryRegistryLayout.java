/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

/**
 * Mirrors the Rust {@code query_tracker::WireQueryMetric} {@code #[repr(C)]}
 * struct (5 × i64 = 40 bytes). A {@code df_query_registry_snapshot} call fills
 * an array of these structs back-to-back into a caller-provided buffer.
 *
 * <p>Provides a decoder that reads one entry at a time by striding over the
 * buffer at multiples of {@link #ENTRY_BYTES}. Field offsets are derived from
 * the {@link StructLayout} so they stay in sync with any future reshuffle of
 * {@code WireQueryMetric}.
 */
public final class QueryRegistryLayout {

    /** Layout of a single wire entry. */
    public static final StructLayout ENTRY_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_LONG.withName("context_id"),
        ValueLayout.JAVA_LONG.withName("current_bytes"),
        ValueLayout.JAVA_LONG.withName("peak_bytes"),
        ValueLayout.JAVA_LONG.withName("wall_nanos"),
        ValueLayout.JAVA_LONG.withName("completed")
    );

    /** Byte size of one wire entry. Matches {@code size_of::<WireQueryMetric>()} on the Rust side. */
    public static final long ENTRY_BYTES = ENTRY_LAYOUT.byteSize();

    private static final long OFF_CONTEXT_ID = ENTRY_LAYOUT.byteOffset(PathElement.groupElement("context_id"));
    private static final long OFF_CURRENT_BYTES = ENTRY_LAYOUT.byteOffset(PathElement.groupElement("current_bytes"));
    private static final long OFF_PEAK_BYTES = ENTRY_LAYOUT.byteOffset(PathElement.groupElement("peak_bytes"));
    private static final long OFF_WALL_NANOS = ENTRY_LAYOUT.byteOffset(PathElement.groupElement("wall_nanos"));
    private static final long OFF_COMPLETED = ENTRY_LAYOUT.byteOffset(PathElement.groupElement("completed"));

    static {
        long expected = 5L * Long.BYTES;
        if (ENTRY_BYTES != expected) {
            throw new AssertionError("QueryRegistryLayout entry size mismatch: expected " + expected + " but got " + ENTRY_BYTES);
        }
    }

    private QueryRegistryLayout() {}

    /**
     * Decode a single entry at zero-based index {@code i} from a buffer populated
     * by {@code df_query_registry_snapshot}.
     */
    public static QueryMemoryUsage readEntry(MemorySegment seg, int i) {
        long base = i * ENTRY_BYTES;
        return new QueryMemoryUsage(
            seg.get(ValueLayout.JAVA_LONG, base + OFF_CONTEXT_ID),
            seg.get(ValueLayout.JAVA_LONG, base + OFF_CURRENT_BYTES),
            seg.get(ValueLayout.JAVA_LONG, base + OFF_PEAK_BYTES),
            seg.get(ValueLayout.JAVA_LONG, base + OFF_WALL_NANOS),
            seg.get(ValueLayout.JAVA_LONG, base + OFF_COMPLETED) != 0L
        );
    }
}
