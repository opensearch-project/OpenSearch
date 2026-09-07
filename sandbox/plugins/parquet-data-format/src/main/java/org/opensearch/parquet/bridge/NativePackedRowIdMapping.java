/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.Cleaner;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Zero-copy {@link RowIdMapping} over a bit-packed buffer allocated by the native
 * (Rust) writer during sort-on-close.
 *
 * <h2>Buffer layout (cross-language contract with {@code packed_mapping.rs})</h2>
 * The native buffer holds two equally-sized sections, each storing {@code count}
 * values at {@code bpv} bits per value, packed back-to-back in little-endian order
 * (value {@code i} occupies bits {@code [i*bpv, (i+1)*bpv)} of its section):
 * <pre>
 *   [ forward: fwd[oldId] = newId | reverse: rev[newId] = oldId ]
 * </pre>
 * Each section is followed by 7 zero pad bytes so the unaligned 8-byte window read
 * used for decoding never runs past the section. Total native footprint is
 * {@code 2 * (ceil(count*bpv/8) + 7)} bytes — roughly {@code 2 * 3N} for 10M rows
 * (24 bpv) instead of the {@code 2 * 8N} of raw longs, with zero Java heap usage.
 *
 * <h2>Ownership and cleanup</h2>
 * This object owns the native buffer. Cleanup follows the standard layered scheme:
 * <ul>
 *   <li><b>Deterministic:</b> {@link #close()} frees the buffer via
 *       {@code parquet_free_row_id_mapping}. Idempotent and thread-safe. Any access
 *       after close throws {@link IllegalStateException} (enforced by the shared
 *       {@link Arena}) rather than reading freed memory.</li>
 *   <li><b>Backstop:</b> a {@link Cleaner} frees the buffer when this object becomes
 *       unreachable without being closed, logging a leak warning. GC timing is not a
 *       memory-pressure mechanism for native bytes — the backstop exists to bound
 *       leaks, not replace close().</li>
 * </ul>
 * Outstanding native bytes across all live mappings are tracked in
 * {@link #outstandingNativeBytes()} for observability.
 */
public final class NativePackedRowIdMapping implements RowIdMapping, AutoCloseable {

    private static final Logger logger = LogManager.getLogger(NativePackedRowIdMapping.class);
    private static final Cleaner CLEANER = Cleaner.create();
    private static final AtomicLong OUTSTANDING_NATIVE_BYTES = new AtomicLong();

    private static final ValueLayout.OfLong LE_LONG_UNALIGNED = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final MemorySegment segment;
    private final int count;
    private final int bpv;
    private final long valueMask;
    private final long reverseSectionOffset;
    private final long nativeByteLen;
    private final Arena arena;
    private final NativeBufferState state;
    private final Cleaner.Cleanable cleanable;

    /**
     * Wraps a native bit-packed mapping buffer produced by {@code parquet_finalize_writer}.
     * Takes ownership: the buffer is freed on {@link #close()} (or by the Cleaner backstop).
     *
     * @param addr    native address of the packed buffer
     * @param byteLen total buffer length in bytes (both sections including padding)
     * @param count   number of row IDs per direction
     * @param bpv     bits per value, in [1, 57]
     */
    NativePackedRowIdMapping(long addr, long byteLen, int count, int bpv) {
        if (addr == 0 || byteLen <= 0 || count <= 0) {
            throw new IllegalArgumentException("Invalid native mapping: addr=" + addr + " byteLen=" + byteLen + " count=" + count);
        }
        if (bpv < 1 || bpv > 57) {
            throw new IllegalArgumentException("bpv must be in [1, 57], got " + bpv);
        }
        this.count = count;
        this.bpv = bpv;
        this.valueMask = (1L << bpv) - 1;
        this.reverseSectionOffset = byteLen / 2;
        this.nativeByteLen = byteLen;
        this.state = new NativeBufferState(addr, byteLen);
        // Shared arena: mappings are produced on the flush thread and consumed on merge
        // threads. Closing the arena invalidates all access atomically across threads.
        this.arena = Arena.ofShared();
        this.segment = MemorySegment.ofAddress(addr).reinterpret(byteLen, arena, null);
        this.state.arena = arena;
        OUTSTANDING_NATIVE_BYTES.addAndGet(byteLen);
        // Backstop: frees native memory if the owner never calls close(). Must not
        // capture 'this' (state is a static class) or the mapping would never be collected.
        this.cleanable = CLEANER.register(this, state);
    }

    @Override
    public long getNewRowId(long oldId) {
        if (oldId < 0 || oldId >= count) {
            return -1L;
        }
        return unpack(0, oldId);
    }

    @Override
    public long getOldRowId(long newId) {
        if (newId < 0 || newId >= count) {
            return -1L;
        }
        return unpack(reverseSectionOffset, newId);
    }

    @Override
    public boolean isNewToOldSupported() {
        return true;
    }

    @Override
    public int size() {
        return count;
    }

    /**
     * Decodes value {@code index} from the section starting at {@code sectionBase}.
     * Single unaligned little-endian 8-byte read + shift/mask; bpv &lt;= 57 guarantees
     * the value fits inside the window, and the 7-byte section padding guarantees
     * the read stays in bounds.
     */
    private long unpack(long sectionBase, long index) {
        long bitPos = index * bpv;
        long bytePos = sectionBase + (bitPos >>> 3);
        int shift = (int) (bitPos & 7);
        long window = segment.get(LE_LONG_UNALIGNED, bytePos);
        return (window >>> shift) & valueMask;
    }

    /** Native bytes held by this mapping (both directions including padding). */
    public long nativeBytesUsed() {
        return nativeByteLen;
    }

    /** Total native bytes currently held by all live {@code NativePackedRowIdMapping} instances. */
    public static long outstandingNativeBytes() {
        return OUTSTANDING_NATIVE_BYTES.get();
    }

    /**
     * Frees the native buffer. Idempotent. After close, any lookup throws
     * {@link IllegalStateException} instead of touching freed memory.
     */
    @Override
    public void close() {
        state.explicitClose = true;
        cleanable.clean(); // runs NativeBufferState.run() at most once
    }

    @Override
    public String toString() {
        return "NativePackedRowIdMapping{size=" + count + ", bpv=" + bpv + ", nativeBytes=" + nativeByteLen + '}';
    }

    /**
     * Cleanup action shared between {@link #close()} and the Cleaner backstop.
     * Static class holding no reference to the mapping object itself, so the Cleaner
     * can fire when the mapping becomes unreachable.
     */
    private static final class NativeBufferState implements Runnable {
        private final long addr;
        private final long byteLen;
        private final AtomicBoolean freed = new AtomicBoolean();
        private volatile Arena arena;
        private volatile boolean explicitClose;

        NativeBufferState(long addr, long byteLen) {
            this.addr = addr;
            this.byteLen = byteLen;
        }

        @Override
        public void run() {
            if (freed.compareAndSet(false, true)) {
                if (explicitClose == false) {
                    logger.warn("NativePackedRowIdMapping leaked (never closed); freeing {} native bytes via Cleaner backstop", byteLen);
                }
                try {
                    // Invalidate all Java-side access before freeing the memory.
                    arena.close();
                } finally {
                    RustBridge.freeRowIdMapping(addr, byteLen);
                    OUTSTANDING_NATIVE_BYTES.addAndGet(-byteLen);
                }
            }
        }
    }
}
