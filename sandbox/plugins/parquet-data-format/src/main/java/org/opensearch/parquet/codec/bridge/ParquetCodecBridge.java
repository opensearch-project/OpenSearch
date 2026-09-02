/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.bridge;

import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge for the Parquet read codec: binds the forward-only column-cursor downcalls exported by
 * the native library. Kept separate from the write-path {@code RustBridge} so the read and write
 * native surfaces stay independent. The cursor is column-oriented rather than doc-values specific,
 * so later codec parts (binary/keyword columns, a doc-values skipper) bind their downcalls here too.
 */
public final class ParquetCodecBridge {

    private static final MethodHandle OPEN_CURSOR;
    private static final MethodHandle CLOSE_CURSOR;
    private static final MethodHandle RESET_CURSOR;
    private static final MethodHandle NEXT_BATCH;

    /** Status returned by {@link #nextBatch} when a batch was produced. */
    public static final long RC_OK = 0L;
    /** Status returned by {@link #nextBatch} when the cursor is exhausted. A {@code < 0} return is an error pointer. */
    public static final long RC_EOF = 2L;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        OPEN_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_open_iter").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,    // file_ptr
                ValueLayout.JAVA_LONG,  // file_len
                ValueLayout.ADDRESS,    // column_ptr
                ValueLayout.JAVA_LONG,  // column_len
                ValueLayout.JAVA_LONG,  // initial_batch_size
                ValueLayout.JAVA_LONG   // max_batch_size
            )
        );
        CLOSE_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_close_iter").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        RESET_CURSOR = linker.downcallHandle(
            lib.find("parquet_df_reset_iter").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        NEXT_BATCH = linker.downcallHandle(
            lib.find("parquet_df_next_batch").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,  // handle
                ValueLayout.JAVA_LONG,  // target_row
                ValueLayout.ADDRESS,    // out_first_row
                ValueLayout.ADDRESS,    // out_last_row
                ValueLayout.ADDRESS,    // out_values_addr
                ValueLayout.ADDRESS,    // out_validity_addr
                ValueLayout.ADDRESS,    // out_validity_bit_offset
                ValueLayout.ADDRESS     // out_value_kind
            )
        );
    }

    /**
     * Opens a forward-only cursor over one Parquet column and returns its native handle.
     *
     * @param initialBatchSize rows in the first decode window; must be in {@code 1..=maxBatchSize}
     * @param maxBatchSize     ceiling the adaptive window grows to, for this cursor's lifetime
     */
    public static long openColumnCursor(String file, String column, long initialBatchSize, long maxBatchSize) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var c = call.str(column);
            return call.invokeIO(OPEN_CURSOR, f.segment(), f.len(), c.segment(), c.len(), initialBatchSize, maxBatchSize);
        }
    }

    /** Releases a cursor handle. */
    public static void closeColumnCursor(long handle) throws IOException {
        try (var call = new NativeCall()) {
            call.invokeIO(CLOSE_CURSOR, handle);
        }
    }

    /** Rewinds a cursor to row zero, retaining cached file metadata. */
    public static void resetColumnCursor(long handle) throws IOException {
        try (var call = new NativeCall()) {
            call.invokeIO(RESET_CURSOR, handle);
        }
    }

    /**
     * Advances the cursor to the batch containing {@code targetRow}, writing the batch row range,
     * the borrowed Arrow value and validity buffer addresses, the validity bit offset, and the
     * value KIND into the caller-owned out-parameters. Returns {@link #RC_OK} or {@link #RC_EOF};
     * a {@code < 0} return is decoded into an {@link IOException}.
     */
    public static long nextBatch(
        long handle,
        long targetRow,
        MemorySegment outFirstRow,
        MemorySegment outLastRow,
        MemorySegment outValuesAddr,
        MemorySegment outValidityAddr,
        MemorySegment outValidityBitOffset,
        MemorySegment outValueKind
    ) throws IOException {
        try (var call = new NativeCall()) {
            return call.invokeIO(
                NEXT_BATCH,
                handle,
                targetRow,
                outFirstRow,
                outLastRow,
                outValuesAddr,
                outValidityAddr,
                outValidityBitOffset,
                outValueKind
            );
        }
    }

    private ParquetCodecBridge() {}
}
