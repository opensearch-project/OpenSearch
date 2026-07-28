/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.Map;

/**
 * Layer 5 — hot-buffer pooling. Owns a confined {@link Arena} and hands out reusable,
 * grow-on-demand native scratch segments for the FFM out-buffers that the column reader
 * passes to the native page-decode / value-read functions.
 *
 * <h2>Named slots</h2>
 * Buffers are keyed by a caller-chosen <em>slot</em> name (for example {@code "value"},
 * {@code "presence"}, {@code "offsets"}). Each slot retains a single backing segment that
 * grows monotonically to the largest size ever requested for that slot — so steady-state
 * page decoding reuses the same native memory with no per-call allocation. A request for a
 * size that still fits the slot's current backing segment returns that same segment (sliced
 * to the requested length); a larger request replaces the backing segment.
 *
 * <p>Because doc IDs arrive ascending and a producer is single-threaded (one segment per
 * query thread), a pool shared across a producer's column readers needs no synchronization.
 * {@link #close()} frees every slot at once (the "release" step).
 *
 * <p>Callers must re-fetch (not cache across calls) a slot's segment, since a growth event
 * replaces it. Returned segments are owned by the pool and must not be freed by callers.
 */
public final class BufferPool implements AutoCloseable {

    /** A pooled slot: its backing segment and the byte capacity that segment was sized to. */
    private static final class Slot {
        MemorySegment segment;
        long capacity;
    }

    private final Arena arena = Arena.ofConfined();
    private final Map<String, Slot> slots = new HashMap<>();
    private boolean closed;

    /**
     * Returns the named slot's segment, grown if necessary to hold at least {@code byteSize}
     * bytes (minimum 1). The returned segment may be larger than requested; callers slice it
     * to the exact length they need.
     */
    public MemorySegment bytes(String slot, long byteSize) {
        ensureOpen();
        long needed = Math.max(byteSize, 1);
        Slot s = slots.computeIfAbsent(slot, k -> new Slot());
        if (s.segment == null || s.capacity < needed) {
            // Grow geometrically to avoid repeated resizes as pages get larger.
            long newCap = s.capacity == 0 ? needed : Math.max(needed, s.capacity * 2);
            s.segment = arena.allocate(newCap);
            s.capacity = newCap;
        }
        return s.segment;
    }

    /** Returns the named slot's segment sized for at least {@code count} {@code long} slots. */
    public MemorySegment longs(String slot, long count) {
        return bytes(slot, Math.max(count, 1) * ValueLayout.JAVA_LONG.byteSize());
    }

    /** Returns the named slot's segment sized for at least {@code count} {@code int} slots. */
    public MemorySegment ints(String slot, long count) {
        return bytes(slot, Math.max(count, 1) * ValueLayout.JAVA_INT.byteSize());
    }

    /**
     * Returns the named slot's segment sized for exactly one {@code long} out-pointer. Each
     * distinct name gets its own stable single-long scratch cell, reused across calls.
     */
    public MemorySegment longOut(String slot) {
        return bytes(slot, ValueLayout.JAVA_LONG.byteSize());
    }

    /** Number of distinct slots currently backed by a segment (for tests / diagnostics). */
    public int slotCount() {
        return slots.size();
    }

    /** Alias for {@link #close()}, matching the design's "release" vocabulary. */
    public void release() {
        close();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("BufferPool already closed");
        }
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            slots.clear();
            arena.close();
        }
    }
}
