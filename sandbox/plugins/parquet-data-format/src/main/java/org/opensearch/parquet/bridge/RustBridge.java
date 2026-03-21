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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * JNI bridge to the native Rust Parquet writer implementation.
 *
 * <p>All native methods operate on Arrow C Data Interface pointers.
 * Writer lifecycle methods are package-private, accessible only via {@link NativeParquetWriter}.
 */
public class RustBridge {

    private static final String LIB_NAME = "parquet_dataformat_jni";

    static {
        loadNativeLibrary();
    }

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

    public static native void initLogger();

    // Writer lifecycle methods — package-private, controlled by NativeParquetWriter
    static native int createWriter(String file, long schemaAddress);

    static native int write(String file, long arrayAddress, long schemaAddress);

    static native ParquetFileMetadata closeWriter(String file);

    static native int flushToDisk(String file);

    // Public utility methods
    public static native ParquetFileMetadata getFileMetadata(String file);

    public static native long getFilteredNativeBytesUsed(String pathPrefix);

    private RustBridge() {}
}
