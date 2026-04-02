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
import java.nio.file.Path;
import java.util.List;

/**
 * JNI bridge to the native Rust Parquet writer library ({@code parquet_dataformat_jni}).
 *
 * <p>Provides static native methods that operate on Arrow C Data Interface memory addresses.
 * The native library is loaded from the classpath resource at
 * {@code /native/{os}-{arch}/libparquet_dataformat_jni.{so|dylib|dll}}, falling back to
 * {@link System#loadLibrary(String)} if the resource is not found.
 *
 * <p>Writer lifecycle methods are package-private and should only be called through
 * {@link NativeParquetWriter}.
 */
public class RustBridge {

    private static final String LIB_NAME = "parquet_dataformat_jni";

    static {
        PlatformHelper.loadNativeLibrary(LIB_NAME, RustBridge.class);
    }

    /** Initializes the native Rust logger. */
    public static native void initLogger();

    // Writer lifecycle methods — package-private, controlled by NativeParquetWriter
    static native void createWriter(
        String file,
        String indexName,
        long schemaAddress,
        List<String> sortColumns,
        List<Boolean> reverseSorts
    ) throws IOException;

    static native void write(String file, long arrayAddress, long schemaAddress) throws IOException;

    static native ParquetFileMetadata finalizeWriter(String file) throws IOException;

    static native void syncToDisk(String file) throws IOException;

    // Settings management
    public static native void onSettingsUpdate(NativeSettings nativeSettings) throws IOException;

    public static native void removeSettings(String indexName);

    // Merge support
    public static native void mergeParquetFilesInRust(
        List<Path> inputFiles,
        String outputFile,
        String indexName
    );

    // Public utility methods
    /**
     * Returns metadata for the specified Parquet file.
     *
     * @param file the path to the Parquet file
     * @return the file metadata
     * @throws IOException if the metadata cannot be read
     */
    public static native ParquetFileMetadata getFileMetadata(String file) throws IOException;

    /**
     * Returns the native memory bytes used by files matching the given path prefix.
     *
     * @param pathPrefix the path prefix to filter by
     * @return the number of native bytes used
     */
    public static native long getFilteredNativeBytesUsed(String pathPrefix);

    private RustBridge() {}
}
