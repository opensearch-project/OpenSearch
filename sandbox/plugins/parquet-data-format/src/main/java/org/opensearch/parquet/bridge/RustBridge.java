/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.nativebridge.spi.PlatformHelper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * JNI bridge to the native Rust Parquet writer library ({@code parquet_dataformat_jni}).
 *
 * <p>Provides static native methods that operate on Arrow C Data Interface memory addresses.
 * The native library is loaded from the classpath resource at
 * {@code /native/{os}-{arch}/libparquet_dataformat_jni.{so|dylib|dll}}, falling back to
 * {@link System#loadLibrary(String)} if the resource is not found.
 *
 * <p>Writer lifecycle methods ({@link #createWriter}, {@link #write}, {@link #closeWriter},
 * {@link #flushToDisk}) are package-private and should only be called through
 * {@link NativeParquetWriter}. Utility methods ({@link #initLogger}, {@link #getFileMetadata},
 * {@link #getFilteredNativeBytesUsed}) are public.
 */
public class RustBridge {

    private static final String LIB_NAME = "parquet_dataformat_jni";

    static {
        loadNativeLibrary();
    }

    @SuppressForbidden(reason = "Needs temp directory to extract native library from classpath at static init time")
    private static void loadNativeLibrary() {
        String platformDir = PlatformHelper.getPlatformDirectory();
        String libFileName = PlatformHelper.getPlatformLibraryName(LIB_NAME);
        String resourcePath = "/native/" + platformDir + "/" + libFileName;

        try (InputStream is = RustBridge.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                System.loadLibrary(LIB_NAME);
                return;
            }
            Path tempDir = Files.createTempDirectory("opensearch-native-");
            Path tempLib = tempDir.resolve(libFileName);
            Files.copy(is, tempLib, StandardCopyOption.REPLACE_EXISTING);
            System.load(tempLib.toAbsolutePath().toString());
            tempLib.toFile().deleteOnExit();
            tempDir.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load native library: " + libFileName, e);
        }
    }

    /** Initializes the native Rust logger. */
    public static native void initLogger();

    // Writer lifecycle methods — package-private, controlled by NativeParquetWriter
    static native int createWriter(String file, long schemaAddress);

    static native int write(String file, long arrayAddress, long schemaAddress);

    static native ParquetFileMetadata closeWriter(String file);

    static native int flushToDisk(String file);

    // Public utility methods
    /**
     * Returns metadata for the specified Parquet file.
     *
     * @param file the path to the Parquet file
     * @return the file metadata
     */
    public static native ParquetFileMetadata getFileMetadata(String file);

    /**
     * Returns the native memory bytes used by files matching the given path prefix.
     *
     * @param pathPrefix the path prefix to filter by
     * @return the number of native bytes used
     */
    public static native long getFilteredNativeBytesUsed(String pathPrefix);

    private RustBridge() {}
}
