/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;

/**
 * Thin wrapper around a confined {@link Arena} for FFM calls.
 *
 * <h2>Arena strategy</h2>
 * <ul>
 *   <li><b>Confined Arena (this class)</b> — for short-lived, per-call allocations: strings
 *       passed to native functions, out-pointers for return values, temp buffers. These are created at
 *       call start, closed immediately after — these are deterministic and cheap.
 *       Confined means single-thread only, which is fine since FFM calls are synchronous.</li>
 *   <li><b>Global Arena ({@link NativeLibraryLoader})</b> — for the {@link java.lang.foreign.SymbolLookup}
 *       that keeps the native library loaded for the JVM lifetime. Never freed.</li>
 *   <li><b>No Arena needed</b> — for opaque native pointers ({@code long} values returned by
 *       functions like {@code createGlobalRuntime}, {@code createReader}). These are Rust heap
 *       addresses cast to {@code i64}. They are not {@link MemorySegment}s, don't need an Arena,
 *       and live until explicitly freed by the corresponding close/free function.</li>
 * </ul>
 *
 * <pre>{@code
 * try (var call = new NativeCall()) {
 *     var name = call.str("hello");
 *     long ptr = call.invoke(CREATE, name.segment(), name.len());
 *     // ptr is a native pointer (long) — lives beyond this Arena
 * }
 * // Arena closed, temp string freed. ptr still valid.
 * }</pre>
 */
public final class NativeCall implements AutoCloseable {

    private final Arena arena;
    private boolean closed;

    public NativeCall() {
        this.arena = Arena.ofConfined();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("NativeCall already closed");
        }
    }

    // ---- String marshaling (single UTF-8 encode) ----

    /**
     * A UTF-8 string segment paired with its byte length.
     * Avoids double-encoding when both segment and length are needed.
     *
     * @param segment the native memory segment containing UTF-8 bytes
     * @param len the byte length of the UTF-8 encoding
     */
    public record Str(MemorySegment segment, long len) {
    }

    /**
     * Allocate a UTF-8 string and return both the segment and its byte length.
     * Encodes the string exactly once.
     *
     * @param s the string to marshal (must not be null)
     * @return a {@link Str} containing the segment and its byte length
     * @throws NullPointerException if s is null
     * @throws IllegalStateException if this NativeCall is closed
     */
    public Str str(String s) {
        ensureOpen();
        if (s == null) {
            throw new NullPointerException("Cannot marshal null string to native");
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new Str(arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes), bytes.length);
    }

    // ---- String array marshaling ----

    /**
     * Parallel pointer and length arrays for passing a string array to native code.
     * Matches the Rust convention of {@code (*const *const u8, *const i64, count)}.
     *
     * @param ptrs memory segment containing pointers to each string's bytes
     * @param lens memory segment containing the byte length of each string
     * @param count the number of strings
     */
    public record StrArray(MemorySegment ptrs, MemorySegment lens, long count) {
    }

    /**
     * Marshal a Java string array into parallel native arrays of pointers and lengths.
     *
     * @param strings the strings to marshal (must not be null, elements must not be null)
     * @return a {@link StrArray} with pointer array, length array, and count
     * @throws NullPointerException if strings or any element is null
     * @throws IllegalStateException if this NativeCall is closed
     */
    public StrArray strArray(String[] strings) {
        ensureOpen();
        if (strings == null) {
            throw new NullPointerException("Cannot marshal null string array to native");
        }
        MemorySegment ptrs = arena.allocate(ValueLayout.ADDRESS, strings.length);
        MemorySegment lens = arena.allocate(ValueLayout.JAVA_LONG, strings.length);
        for (int i = 0; i < strings.length; i++) {
            if (strings[i] == null) {
                throw new NullPointerException("Cannot marshal null string at index " + i);
            }
            byte[] bytes = strings[i].getBytes(StandardCharsets.UTF_8);
            ptrs.setAtIndex(ValueLayout.ADDRESS, i, arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes));
            lens.setAtIndex(ValueLayout.JAVA_LONG, i, bytes.length);
        }
        return new StrArray(ptrs, lens, strings.length);
    }

    // ---- Out-buffer with overflow detection ----

    /**
     * A bounded output buffer for native functions that write results into caller-provided memory.
     * Provides overflow detection when reading the actual length written by native code.
     *
     * @param data the buffer segment native code writes into
     * @param lenOut out-pointer where native code writes the actual byte count
     * @param capacity the allocated capacity of the data buffer
     */
    public record OutBuffer(MemorySegment data, MemorySegment lenOut, int capacity) {

        /**
         * Returns the actual length written by native code.
         *
         * @return the number of bytes written
         * @throws IllegalStateException if the native code reported more bytes than the buffer capacity
         */
        public int actualLength() {
            int len = (int) lenOut.get(ValueLayout.JAVA_LONG, 0);
            if (len < 0) {
                throw new IllegalStateException("Native output reported negative length: " + len);
            }
            if (len > capacity) {
                throw new IllegalStateException("Native output (" + len + " bytes) exceeds buffer capacity (" + capacity + " bytes)");
            }
            return len;
        }

        /** Extract the written bytes as a Java byte array. */
        public byte[] toByteArray() {
            return data.asSlice(0, actualLength()).toArray(ValueLayout.JAVA_BYTE);
        }
    }

    /**
     * Allocate a bounded output buffer with overflow detection.
     *
     * @param capacity the buffer size in bytes
     * @return an {@link OutBuffer} containing the data segment, length out-pointer, and capacity
     * @throws IllegalStateException if this NativeCall is closed
     */
    public OutBuffer outBuffer(int capacity) {
        ensureOpen();
        return new OutBuffer(arena.allocate(capacity), arena.allocate(ValueLayout.JAVA_LONG), capacity);
    }

    // ---- Simple allocations ----

    /**
     * Allocate an out-pointer for {@code int}.
     * Read with {@code seg.get(ValueLayout.JAVA_INT, 0)}.
     */
    public MemorySegment intOut() {
        ensureOpen();
        return arena.allocate(ValueLayout.JAVA_INT);
    }

    /** Allocate an out-pointer for {@code long}. */
    public MemorySegment longOut() {
        ensureOpen();
        return arena.allocate(ValueLayout.JAVA_LONG);
    }

    /** Allocate a byte buffer of the given size. */
    public MemorySegment buf(int size) {
        ensureOpen();
        return arena.allocate(size);
    }

    /** Allocate a segment from a byte array. */
    public MemorySegment bytes(byte[] data) {
        ensureOpen();
        if (data == null) {
            throw new NullPointerException("Cannot marshal null byte array to native");
        }
        return arena.allocateFrom(ValueLayout.JAVA_BYTE, data);
    }

    /**
     * Allocate a segment from a long array. Returns an empty (zero-byte) segment if the array
     * is empty so callers can pass it as a non-null pointer with count zero.
     */
    public MemorySegment longs(long[] data) {
        ensureOpen();
        if (data == null) {
            throw new NullPointerException("Cannot marshal null long array to native");
        }
        if (data.length == 0) {
            return arena.allocate(0);
        }
        return arena.allocateFrom(ValueLayout.JAVA_LONG, data);
    }

    // ---- Invocation ----

    /**
     * Invoke a MethodHandle that returns {@code long} and check the result.
     * Throws {@link RuntimeException} on native error.
     */
    public long invoke(MethodHandle handle, Object... args) {
        ensureOpen();
        try {
            long result = (long) handle.invokeWithArguments(args);
            return NativeLibraryLoader.checkResult(result);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Invoke a MethodHandle that returns {@code long} and check the result.
     * Throws {@link IOException} on native error.
     */
    public long invokeIO(MethodHandle handle, Object... args) throws IOException {
        ensureOpen();
        try {
            long result = (long) handle.invokeWithArguments(args);
            return NativeLibraryLoader.checkResultIO(result);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    /**
     * Invoke a void MethodHandle. No return value, no error check.
     * Use for fire-and-forget calls like close, shutdown, init.
     * This is static because it does not require an Arena.
     */
    public static void invokeVoid(MethodHandle handle, Object... args) {
        try {
            handle.invokeWithArguments(args);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void close() {
        closed = true;
        arena.close();
    }
}
