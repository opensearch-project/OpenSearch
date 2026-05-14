/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.spi.QueryExecutionMetrics;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

/**
 * Mirrors the Rust {@code query_tracker::WireQueryMetric} {@code #[repr(C)]}
 * struct (5 × i64 = 40 bytes) and decodes a strided buffer of those structs
 * directly into the SPI types consumed by
 * {@link org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin#getActiveQueryMetrics}.
 *
 * <p>Decoder reads each row by slicing the segment to that row's 40-byte
 * window. We deliberately avoid {@code SequenceLayout}-derived
 * {@code VarHandle}s because their bounds check enforces the segment span
 * the entire sequence layout, not just up to the requested row. With a
 * generic {@code Long.MAX_VALUE / ENTRY_BYTES} sequence layout the read
 * always fails an {@link IndexOutOfBoundsException}.
 *
 * <p>Buffer shape (populated by {@code df_query_registry_top_n_by_current}):
 * <pre>
 *   [ entry 0 ][ entry 1 ] ... [ entry N-1 ]
 *   each entry = { context_id, current_bytes, peak_bytes, wall_nanos, completed } (5 × i64)
 * </pre>
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

    // Byte offsets within a single entry. Field order must match
    // ENTRY_LAYOUT and the Rust #[repr(C)] WireQueryMetric struct.
    private static final long OFF_CONTEXT_ID    = 0L;
    private static final long OFF_CURRENT_BYTES = 8L;
    private static final long OFF_PEAK_BYTES    = 16L;
    private static final long OFF_WALL_NANOS    = 24L;
    private static final long OFF_COMPLETED     = 32L;

    static {
        long expected = 5L * Long.BYTES;
        if (ENTRY_BYTES != expected) {
            throw new AssertionError("QueryRegistryLayout entry size mismatch: expected " + expected + " but got " + ENTRY_BYTES);
        }
    }

    private QueryRegistryLayout() {}

    /**
     * Read the {@code context_id} of the entry at zero-based index {@code i}.
     *
     * @param seg buffer populated by {@code df_query_registry_top_n_by_current}
     * @param i   row index, must be {@code < written} returned by the FFI call
     */
    public static long readContextId(MemorySegment seg, int i) {
        long rowOffset = (long) i * ENTRY_BYTES;
        return seg.get(ValueLayout.JAVA_LONG, rowOffset + OFF_CONTEXT_ID);
    }

    /**
     * Read the metrics fields of the entry at zero-based index {@code i} into
     * a fresh {@link QueryExecutionMetrics} record.
     *
     * @param seg buffer populated by {@code df_query_registry_top_n_by_current}
     * @param i   row index, must be {@code < written} returned by the FFI call
     */
    public static QueryExecutionMetrics readMetrics(MemorySegment seg, int i) {
        long rowOffset = (long) i * ENTRY_BYTES;
        return new QueryExecutionMetrics(
            seg.get(ValueLayout.JAVA_LONG, rowOffset + OFF_CURRENT_BYTES),
            seg.get(ValueLayout.JAVA_LONG, rowOffset + OFF_PEAK_BYTES),
            seg.get(ValueLayout.JAVA_LONG, rowOffset + OFF_WALL_NANOS),
            seg.get(ValueLayout.JAVA_LONG, rowOffset + OFF_COMPLETED) != 0L
        );
    }
}
