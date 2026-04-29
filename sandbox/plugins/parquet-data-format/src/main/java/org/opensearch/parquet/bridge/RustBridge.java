/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;

public class RustBridge {

    private static final MethodHandle CREATE_WRITER;
    private static final MethodHandle WRITE;
    private static final MethodHandle FINALIZE_WRITER;
    private static final MethodHandle SYNC_TO_DISK;
    private static final MethodHandle GET_FILE_METADATA;
    private static final MethodHandle GET_FILTERED_BYTES;
    private static final MethodHandle INIT_HEAP;
    private static final MethodHandle ALLOCATE_TEST_BUFFER;
    private static final MethodHandle FREE_TEST_BUFFER;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        CREATE_WRITER = linker.downcallHandle(
            lib.find("parquet_create_writer").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        WRITE = linker.downcallHandle(
            lib.find("parquet_write").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );
        FINALIZE_WRITER = linker.downcallHandle(
            lib.find("parquet_finalize_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS
            )
        );
        SYNC_TO_DISK = linker.downcallHandle(
            lib.find("parquet_sync_to_disk").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        GET_FILE_METADATA = linker.downcallHandle(
            lib.find("parquet_get_file_metadata").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        GET_FILTERED_BYTES = linker.downcallHandle(
            lib.find("parquet_get_filtered_native_bytes_used").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        INIT_HEAP = linker.downcallHandle(lib.find("parquet_init_heap").orElseThrow(), FunctionDescriptor.ofVoid());
        ALLOCATE_TEST_BUFFER = linker.downcallHandle(
            lib.find("parquet_allocate_test_buffer").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        FREE_TEST_BUFFER = linker.downcallHandle(
            lib.find("parquet_free_test_buffer").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    public static void initLogger() {}

    static void createWriter(String file, long schemaAddress) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            call.invokeIO(CREATE_WRITER, f.segment(), f.len(), schemaAddress);
        }
    }

    static void write(String file, long arrayAddress, long schemaAddress) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            call.invokeIO(WRITE, f.segment(), f.len(), arrayAddress, schemaAddress);
        }
    }

    static ParquetFileMetadata finalizeWriter(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var crc32Out = call.longOut();
            var out = call.outBuffer(1024);
            long rc = call.invokeIO(
                FINALIZE_WRITER,
                f.segment(),
                f.len(),
                versionOut,
                numRowsOut,
                out.data(),
                (long) out.capacity(),
                out.lenOut(),
                crc32Out
            );
            if (rc == 1) return null;
            int createdByLen = out.actualLength();
            return new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(out.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                crc32Out.get(ValueLayout.JAVA_LONG, 0)
            );
        }
    }

    static void syncToDisk(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            call.invokeIO(SYNC_TO_DISK, f.segment(), f.len());
        }
    }

    public static ParquetFileMetadata getFileMetadata(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var out = call.outBuffer(1024);
            call.invokeIO(GET_FILE_METADATA, f.segment(), f.len(), versionOut, numRowsOut, out.data(), (long) out.capacity(), out.lenOut());
            int createdByLen = out.actualLength();
            return new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(out.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                0L
            );
        }
    }

    public static long getFilteredNativeBytesUsed(String pathPrefix) {
        try (var call = new NativeCall()) {
            var p = call.str(pathPrefix);
            return call.invoke(GET_FILTERED_BYTES, p.segment(), p.len());
        }
    }

    /** Initializes the parquet plugin's mimalloc heap. Call once at startup. */
    public static void initHeap() {
        try {
            INIT_HEAP.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException("initHeap failed", t);
        }
    }

    /** Allocates a test buffer on parquet's heap. Returns native pointer. */
    public static long allocateTestBuffer(long size) {
        try {
            return (long) ALLOCATE_TEST_BUFFER.invokeExact(size);
        } catch (Throwable t) {
            throw new RuntimeException("allocateTestBuffer failed", t);
        }
    }

    /** Frees a test buffer. Safe to call from any thread. */
    public static void freeTestBuffer(long ptr, long size) {
        try {
            FREE_TEST_BUFFER.invokeExact(ptr, size);
        } catch (Throwable t) {
            throw new RuntimeException("freeTestBuffer failed", t);
        }
    }

    private RustBridge() {}
}
