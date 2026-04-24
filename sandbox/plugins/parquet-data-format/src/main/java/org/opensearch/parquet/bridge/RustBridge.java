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

/**
 * FFM bridge to the native Rust Parquet writer and ObjectStore handle management.
 *
 * <p>Writer functions are handle-based: {@link #createWriter} returns a writer handle,
 * subsequent calls pass that handle. No global state on the Rust side.
 *
 * <p>Store functions manage ObjectStore handles for shard-scoped PrefixStore creation.
 */
public class RustBridge {

    // ═══════════════════════════════════════════════════════════════════════════
    // Writer method handles
    // ═══════════════════════════════════════════════════════════════════════════

    private static final MethodHandle CREATE_WRITER;
    private static final MethodHandle WRITE;
    private static final MethodHandle FINALIZE_WRITER;
    private static final MethodHandle DESTROY_WRITER;
    private static final MethodHandle GET_FILE_METADATA;

    // ═══════════════════════════════════════════════════════════════════════════
    // Store method handles
    // ═══════════════════════════════════════════════════════════════════════════

    private static final MethodHandle CREATE_SCOPED_STORE;
    private static final MethodHandle CREATE_LOCAL_STORE;
    private static final MethodHandle DESTROY_STORE;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        // parquet_create_writer(store_handle: i64, path_ptr: *const u8, path_len: i64, schema_addr: i64) -> i64
        CREATE_WRITER = linker.downcallHandle(
            lib.find("parquet_create_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        // parquet_write(writer_handle: i64, array_addr: i64, schema_addr: i64) -> i64
        WRITE = linker.downcallHandle(
            lib.find("parquet_write").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // parquet_finalize_writer(writer_handle: i64, num_rows_out: *mut i64, crc32_out: *mut i64) -> i64
        FINALIZE_WRITER = linker.downcallHandle(
            lib.find("parquet_finalize_writer").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );

        // parquet_destroy_writer(writer_handle: i64)
        DESTROY_WRITER = linker.downcallHandle(
            lib.find("parquet_destroy_writer").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // parquet_get_file_metadata(file_ptr, file_len, version_out, num_rows_out, created_by_buf, buf_len, len_out) -> i64
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

        // parquet_create_local_store(path_ptr: *const u8, path_len: i64) -> i64
        CREATE_LOCAL_STORE = linker.downcallHandle(
            lib.find("parquet_create_local_store").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // parquet_create_scoped_store(parent_handle: i64, prefix_ptr: *const u8, prefix_len: i64) -> i64
        CREATE_SCOPED_STORE = linker.downcallHandle(
            lib.find("parquet_create_scoped_store").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );

        // parquet_destroy_store(handle: i64)
        DESTROY_STORE = linker.downcallHandle(
            lib.find("parquet_destroy_store").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );
    }

    public static void initLogger() {}

    // ═══════════════════════════════════════════════════════════════════════════
    // Writer methods
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates a new Parquet writer backed by an ObjectStore.
     *
     * @param storeHandle native ObjectStore pointer (shard-scoped)
     * @param objectPath relative path within the store (e.g., "gen_1/data_0.parquet")
     * @param schemaAddress native Arrow schema pointer
     * @return writer handle for subsequent write/finalize calls
     */
    static long createWriter(long storeHandle, String objectPath, long schemaAddress) throws IOException {
        try (var call = new NativeCall()) {
            var path = call.str(objectPath);
            return call.invokeIO(CREATE_WRITER, storeHandle, path.segment(), path.len(), schemaAddress);
        }
    }

    /**
     * Writes an Arrow record batch to the writer.
     *
     * @param writerHandle writer handle from {@link #createWriter}
     * @param arrayAddress native Arrow array pointer
     * @param schemaAddress native Arrow schema pointer
     */
    static void write(long writerHandle, long arrayAddress, long schemaAddress) throws IOException {
        try (var call = new NativeCall()) {
            call.invokeIO(WRITE, writerHandle, arrayAddress, schemaAddress);
        }
    }

    /**
     * Finalizes the writer: flushes footer, uploads to ObjectStore, returns metadata.
     * The writer handle is consumed and invalid after this call.
     *
     * @param writerHandle writer handle from {@link #createWriter}
     * @return file metadata (num_rows, crc32), or null if writer was empty
     */
    static ParquetFileMetadata finalizeWriter(long writerHandle) throws IOException {
        try (var call = new NativeCall()) {
            var numRowsOut = call.longOut();
            var crc32Out = call.longOut();
            call.invokeIO(FINALIZE_WRITER, writerHandle, numRowsOut, crc32Out);
            return new ParquetFileMetadata(
                0, // version not returned by new API
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                null, // createdBy not returned by new API
                crc32Out.get(ValueLayout.JAVA_LONG, 0)
            );
        }
    }

    /**
     * Destroys a writer without finalizing (error cleanup path).
     */
    static void destroyWriter(long writerHandle) {
        NativeCall.invokeVoid(DESTROY_WRITER, writerHandle);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Store methods
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Creates a local filesystem ObjectStore rooted at the given path.
     *
     * @param rootPath absolute path to the root directory
     * @return store handle
     */
    public static long createLocalStore(String rootPath) throws IOException {
        try (var call = new NativeCall()) {
            var p = call.str(rootPath);
            return call.invokeIO(CREATE_LOCAL_STORE, p.segment(), p.len());
        }
    }

    /**
     * Creates a PrefixStore scoping a parent ObjectStore to the given prefix.
     *
     * @param parentHandle parent ObjectStore pointer (repo-level)
     * @param prefix path prefix (e.g., "indices/{uuid}/{shardId}/parquet/")
     * @return scoped store handle
     */
    public static long createScopedStore(long parentHandle, String prefix) throws IOException {
        try (var call = new NativeCall()) {
            var p = call.str(prefix);
            return call.invokeIO(CREATE_SCOPED_STORE, parentHandle, p.segment(), p.len());
        }
    }

    /**
     * Drops an ObjectStore handle, decrementing the Arc refcount.
     */
    public static void destroyStore(long handle) {
        NativeCall.invokeVoid(DESTROY_STORE, handle);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // File metadata (reads from local file path)
    // ═══════════════════════════════════════════════════════════════════════════

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

    private RustBridge() {}
}
