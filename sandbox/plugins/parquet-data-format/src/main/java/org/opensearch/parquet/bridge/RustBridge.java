/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.nativebridge.spi.PlatformHelper;

import java.io.IOException;

/**
 * JNI bridge to the native Rust Parquet writer library ({@code parquet_dataformat_jni}).
 *
 * <p>Provides two categories of native handles:
 * <ul>
 *   <li>Object store handles — created via {@link #createObjectStore} and shared across
 *       multiple writers targeting the same backend.</li>
 *   <li>Writer handles — created via {@link #createWriter} using an existing store handle,
 *       representing a single Parquet file being written.</li>
 * </ul>
 *
 * <p>The native library is loaded from the classpath resource at
 * {@code /native/{os}-{arch}/libparquet_dataformat_jni.{so|dylib|dll}}, falling back to
 * {@link System#loadLibrary(String)} if the resource is not found.
 *
 * <p>All handle-based methods are package-private and should only be called through
 * {@link NativeObjectStore} and {@link NativeParquetWriter}.
 */
public class RustBridge {

    private static final String LIB_NAME = "parquet_dataformat_jni";

    static {
        PlatformHelper.loadNativeLibrary(LIB_NAME, RustBridge.class);
    }

    /** Initializes the native Rust logger. */
    public static native void initLogger();

    // -----------------------------------------------------------------------
    // Object Store lifecycle
    // -----------------------------------------------------------------------

    /**
     * Creates a native object store and returns an opaque handle.
     *
     * @param storeType  backend type: "local", "s3", "gcs", or "azure"
     * @param configJson JSON string with backend-specific configuration
     * @return opaque native handle to the object store
     * @throws IOException if the store creation fails
     */
    static native long createObjectStore(String storeType, String configJson) throws IOException;

    /**
     * Destroys a native object store, releasing its resources.
     *
     * @param storeHandle the native object store handle
     */
    static native void destroyObjectStore(long storeHandle);

    // -----------------------------------------------------------------------
    // Writer lifecycle
    // -----------------------------------------------------------------------

    /**
     * Creates a native writer backed by the given object store.
     *
     * @param storeHandle   the native object store handle
     * @param path          the object path to write to
     * @param schemaAddress the native memory address of the Arrow schema
     * @return opaque native handle to the writer
     * @throws IOException if the native writer creation fails
     */
    static native long createWriter(long storeHandle, String path, long schemaAddress) throws IOException;

    static native void write(long handle, long arrayAddress, long schemaAddress) throws IOException;

    static native ParquetFileMetadata finalizeWriter(long handle) throws IOException;

    static native long getWriterMemoryUsage(long handle);

    /**
     * Destroys the native writer, freeing all Rust-side resources.
     *
     * @param handle the native writer handle
     */
    static native void destroyWriter(long handle);

    // -----------------------------------------------------------------------
    // Utility (no handle needed)
    // -----------------------------------------------------------------------

    /**
     * Returns metadata for the specified Parquet file on local disk.
     *
     * @param file the path to the Parquet file
     * @return the file metadata
     * @throws IOException if the metadata cannot be read
     */
    public static native ParquetFileMetadata getFileMetadata(String file) throws IOException;

    private RustBridge() {}
}
