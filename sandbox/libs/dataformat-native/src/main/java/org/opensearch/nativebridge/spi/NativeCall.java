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
 *     long ptr = call.check((long) CREATE.invokeExact(call.str("hello"), NativeCall.len("hello")));
 *     // ptr is a native pointer (long) — lives beyond this Arena
 * }
 * // Arena closed, temp string freed. ptr still valid.
 * }</pre>
 */
public final class NativeCall implements AutoCloseable {

    private final Arena arena;

    public NativeCall() {
        this.arena = Arena.ofConfined();
    }

    /** Allocate a UTF-8 string segment. Use with {@link #len(String)} for the length param. */
    public MemorySegment str(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        MemorySegment seg = arena.allocate(bytes.length);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return seg;
    }

    /** UTF-8 byte length of a string (for the len param paired with {@link #str}). */
    public static long len(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    /** Allocate an out-pointer for {@code int}. Read with {@code seg.get(ValueLayout.JAVA_INT, 0)}. */
    public MemorySegment intOut() {
        return arena.allocate(ValueLayout.JAVA_INT);
    }

    /** Allocate an out-pointer for {@code long}. */
    public MemorySegment longOut() {
        return arena.allocate(ValueLayout.JAVA_LONG);
    }

    /** Allocate a byte buffer of the given size. */
    public MemorySegment buf(int size) {
        return arena.allocate(size);
    }

    /** Allocate a segment from a byte array. */
    public MemorySegment bytes(byte[] data) {
        MemorySegment seg = arena.allocate(data.length);
        MemorySegment.copy(data, 0, seg, ValueLayout.JAVA_BYTE, 0, data.length);
        return seg;
    }

    /** Invoke a MethodHandle and check the result. Throws RuntimeException on native error. */
    public long invoke(MethodHandle handle, Object... args) {
        try {
            long result = (long) handle.invokeWithArguments(args);
            return NativeLibraryLoader.checkResult(result);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /** Invoke a MethodHandle and check the result. Throws IOException on native error. */
    public long invokeIO(MethodHandle handle, Object... args) throws IOException {
        try {
            long result = (long) handle.invokeWithArguments(args);
            return NativeLibraryLoader.checkResultIO(result);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    @Override
    public void close() {
        arena.close();
    }
}
