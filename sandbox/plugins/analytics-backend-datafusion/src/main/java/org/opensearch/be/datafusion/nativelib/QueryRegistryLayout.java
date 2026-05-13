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
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Mirrors the Rust {@code query_tracker::WireQueryMetric} {@code #[repr(C)]}
 * struct (5 × i64 = 40 bytes) and decodes a strided buffer of those structs
 * directly into the SPI types consumed by
 * {@link org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin#getActiveQueryMetrics}.
 *
 * <p>This is the same idiom {@code StatsLayout} uses for the {@code df_stats}
 * call: a {@link StructLayout} describes the wire shape, cached
 * {@link VarHandle}s read individual fields, and the decoder produces the
 * caller's final type with no transport-only intermediate.
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

    /**
     * Sequence layout used to derive per-row {@link VarHandle}s. The first path
     * element is a {@code sequenceElement()} (the array index), the second is
     * the field name within {@code ENTRY_LAYOUT}.
     */
    private static final SequenceLayout ROWS = MemoryLayout.sequenceLayout(Long.MAX_VALUE / ENTRY_BYTES, ENTRY_LAYOUT);

    private static final VarHandle CONTEXT_ID = rowHandle("context_id");
    private static final VarHandle CURRENT_BYTES = rowHandle("current_bytes");
    private static final VarHandle PEAK_BYTES = rowHandle("peak_bytes");
    private static final VarHandle WALL_NANOS = rowHandle("wall_nanos");
    private static final VarHandle COMPLETED = rowHandle("completed");

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
        return (long) CONTEXT_ID.get(seg, 0L, (long) i);
    }

    /**
     * Read the metrics fields of the entry at zero-based index {@code i} into
     * a fresh {@link QueryExecutionMetrics} record.
     *
     * @param seg buffer populated by {@code df_query_registry_top_n_by_current}
     * @param i   row index, must be {@code < written} returned by the FFI call
     */
    public static QueryExecutionMetrics readMetrics(MemorySegment seg, int i) {
        long row = (long) i;
        return new QueryExecutionMetrics(
            (long) CURRENT_BYTES.get(seg, 0L, row),
            (long) PEAK_BYTES.get(seg, 0L, row),
            (long) WALL_NANOS.get(seg, 0L, row),
            (long) COMPLETED.get(seg, 0L, row) != 0L
        );
    }

    private static VarHandle rowHandle(String field) {
        // sequenceElement() leaves the row index as a free coordinate, so the
        // returned VarHandle takes (segment, base, rowIndex) — we always pass
        // base=0 because the FFM call writes at the start of the segment.
        return ROWS.varHandle(PathElement.sequenceElement(), PathElement.groupElement(field));
    }
}
