/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.docvaluescodec.bridge;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

/**
 * Numeric column reader backed by a forward-only native Arrow column cursor.
 *
 * <p>The native cursor only advances forward. This wrapper still serves a request that falls behind
 * the current batch by reopening the cursor (cheap, since file metadata is cached) and scanning
 * forward again, so an ascending walk that occasionally rewinds keeps working.
 *
 * <p>Each batch call returns borrowed Arrow buffer addresses; the resident {@link DecodedBatch}
 * points off-heap views at them and reads values in place, with no copy. Those views are valid only
 * until the next batch call on this reader, which always replaces the batch first.
 *
 * <p>TODO (numeric-only v1): the following are intentionally not implemented yet and are tracked
 * here so callers know the gaps:
 * <ul>
 *   <li>boolean values (Arrow packs them bit-wise, so borrowing needs a value bit offset like the
 *       validity bitmap);</li>
 *   <li>repeated / multi-valued numerics (more than one value per document);</li>
 *   <li>binary / keyword columns;</li>
 *   <li>a copy fallback and its overflow-retry, which are only needed if a non-borrowable path is
 *       ever served here.</li>
 *   <li>the Parquet page index (per-page min/max/null-count) and a {@code pageIndex()} accessor: the
 *       page-index FFM path was left out of this numeric bridge. A future DocValues skipper needs it
 *       to skip whole pages without decoding. TODO: add the page-index read (Rust cursor + bridge)
 *       when the skipper lands.</li>
 * </ul>
 */
public final class ParquetColumnReader implements Closeable, NumericValueReader {

    private static final long CLOSED_HANDLE = -1L;

    private static final int DEFAULT_INITIAL_BATCH_SIZE = 32;

    /** Number of scalar out-parameters {@code nextBatch} writes back. */
    private static final int OUT_PARAM_COUNT = 6;

    /** Java-side cap on a returned batch; mirrors {@code MAX_BATCH_SIZE} in doc_values_cursor.rs. */
    private static final int MAX_BATCH_ROWS = 8192;

    private final Path file;
    private final String column;

    private long handle;
    private DecodedBatch decodedBatch;

    private ParquetColumnReader(long handle, Path file, String column) {
        this.handle = handle;
        this.file = file;
        this.column = column;
    }

    /** Opens a numeric cursor with the default starting window. */
    public static ParquetColumnReader open(Path file, String column) throws IOException {
        return open(file, column, DEFAULT_INITIAL_BATCH_SIZE);
    }

    /** Opens a numeric cursor with an explicit starting window. */
    public static ParquetColumnReader open(Path file, String column, int initialBatchSize) throws IOException {
        long handle = ParquetDocValuesBridge.openColumnCursor(file.toString(), column, initialBatchSize);
        return new ParquetColumnReader(handle, file, column);
    }

    @Override
    public DecodedBatch decodedBatch() {
        return decodedBatch;
    }

    /**
     * Loads the batch beginning at {@code row}. Forward requests ride the native cursor; a request
     * behind the current batch reopens it first.
     */
    @Override
    public void loadBatchContaining(long row) throws IOException {
        ensureOpen();
        DecodedBatch current = decodedBatch;
        if (current != null && row < current.firstRow()) {
            reopen();
        }
        loadNumericBatch(row);
    }

    /** Replaces the forward-only cursor with a fresh one at row zero. Only reached on a backward request. */
    private void reopen() throws IOException {
        decodedBatch = null;
        ParquetDocValuesBridge.resetColumnCursor(handle);
    }

    private void loadNumericBatch(long row) throws IOException {
        long firstRow;
        long lastRow;
        long valuesAddr;
        long validityAddr;
        int kind;
        int bitOffset;

        // The six scalar out-parameters are tiny and read out immediately, so a per-call arena is
        // enough; the borrowed value/validity buffers live in native (Rust-owned) memory and are
        // reinterpreted separately below, outside this arena.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG, OUT_PARAM_COUNT);
            MemorySegment firstRowOut = out.asSlice(0L, Long.BYTES);
            MemorySegment lastRowOut = out.asSlice(Long.BYTES, Long.BYTES);
            MemorySegment valuesAddrOut = out.asSlice(2L * Long.BYTES, Long.BYTES);
            MemorySegment validityAddrOut = out.asSlice(3L * Long.BYTES, Long.BYTES);
            MemorySegment validityBitOffsetOut = out.asSlice(4L * Long.BYTES, Long.BYTES);
            MemorySegment valueKindOut = out.asSlice(5L * Long.BYTES, Long.BYTES);

            long rc = ParquetDocValuesBridge.nextBatch(
                handle,
                row,
                firstRowOut,
                lastRowOut,
                valuesAddrOut,
                validityAddrOut,
                validityBitOffsetOut,
                valueKindOut
            );
            checkStatus(rc, row);

            firstRow = firstRowOut.get(ValueLayout.JAVA_LONG, 0);
            lastRow = lastRowOut.get(ValueLayout.JAVA_LONG, 0);
            valuesAddr = valuesAddrOut.get(ValueLayout.JAVA_LONG, 0);
            validityAddr = validityAddrOut.get(ValueLayout.JAVA_LONG, 0);
            bitOffset = (int) validityBitOffsetOut.get(ValueLayout.JAVA_LONG, 0);
            kind = (int) valueKindOut.get(ValueLayout.JAVA_LONG, 0);
        }

        // Validate the native cursor's framing before pointing memory views at the borrowed
        // buffers. reinterpret() is unbounded (it trusts the native address and length), so these
        // checks fail fast on a malformed contract instead of reading out of bounds. They do not,
        // and cannot, make a correct-looking but wrong address safe - that is inherent to a
        // zero-copy FFM borrow.
        int width = widthForKind(kind, row);
        if (firstRow < 0 || lastRow < firstRow || row < firstRow || row > lastRow) {
            throw contractViolation(row, "row range [" + firstRow + ", " + lastRow + "]");
        }
        long batchRowsLong = lastRow - firstRow + 1;
        if (batchRowsLong > MAX_BATCH_ROWS) {
            throw contractViolation(row, batchRowsLong + " rows exceeds cap " + MAX_BATCH_ROWS);
        }
        if (valuesAddr == 0 || bitOffset < 0) {
            throw contractViolation(row, "values address " + valuesAddr + ", bit offset " + bitOffset);
        }
        int batchRows = (int) batchRowsLong;

        // Borrowed Arrow buffers, read in place: O(rows accessed), no copy. Valid until the next
        // batch call on this cursor; the resident batch is always replaced before that call.
        MemorySegment values = MemorySegment.ofAddress(valuesAddr).reinterpret((long) batchRows * width);
        MemorySegment presenceBits;
        int presenceBitOffset;
        if (validityAddr == 0) {
            presenceBits = null;
            presenceBitOffset = 0;
        } else {
            long presenceWords = ((long) bitOffset + batchRows + 63) >>> 6;
            presenceBits = MemorySegment.ofAddress(validityAddr).reinterpret(presenceWords * Long.BYTES);
            presenceBitOffset = bitOffset;
        }
        decodedBatch = new DecodedBatch(firstRow, lastRow, values, kind, presenceBits, presenceBitOffset);
    }

    /** Byte width of a value KIND, rejecting any kind this reader does not understand. */
    private int widthForKind(int kind, long row) throws IOException {
        return switch (kind) {
            case DecodedBatch.KIND_LONG -> Long.BYTES;
            case DecodedBatch.KIND_INT, DecodedBatch.KIND_UINT_BITS -> Integer.BYTES;
            case DecodedBatch.KIND_SHORT, DecodedBatch.KIND_USHORT -> Short.BYTES;
            case DecodedBatch.KIND_BYTE, DecodedBatch.KIND_UBYTE -> Byte.BYTES;
            default -> throw contractViolation(row, "unknown value kind " + kind);
        };
    }

    private IOException contractViolation(long row, String detail) {
        return new IOException(
            "native numeric cursor returned an invalid batch at row " + row + " (" + detail + ") for " + file + "/" + column
        );
    }

    private void checkStatus(long rc, long row) throws IOException {
        if (rc == ParquetDocValuesBridge.RC_EOF) {
            throw new IOException("native numeric cursor exhausted before row " + row + " (" + file + "/" + column + ")");
        }
        if (rc != ParquetDocValuesBridge.RC_OK) {
            throw new IOException("Unexpected native numeric cursor status " + rc + " at row " + row + " (" + file + "/" + column + ")");
        }
    }

    private void ensureOpen() {
        if (handle == CLOSED_HANDLE) {
            throw new IllegalStateException("ParquetColumnReader is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (handle == CLOSED_HANDLE) {
            return;
        }
        long current = handle;
        handle = CLOSED_HANDLE;
        decodedBatch = null;
        ParquetDocValuesBridge.closeColumnCursor(current);
    }
}
