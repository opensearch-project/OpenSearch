/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * JNI wrapper for DataFusion operations
 */
public class DataFusionJNI {

    /**
     * Private constructor to prevent instantiation.
     */
    private DataFusionJNI() {
        // Utility class
    }

    private static boolean libraryLoaded = false;

    static {
        loadNativeLibrary();
    }

    /**
     * Load the native library from resources
     */
    private static synchronized void loadNativeLibrary() {
        if (libraryLoaded) {
            return;
        }

        try {
            String osName = System.getProperty("os.name").toLowerCase();
            String libExtension;
            String libName;

            if (osName.contains("windows")) {
                libExtension = ".dll";
                libName = "libopensearch_datafusion_jni.dll";
            } else if (osName.contains("mac")) {
                libExtension = ".dylib";
                libName = "libopensearch_datafusion_jni.dylib";
            } else {
                libExtension = ".so";
                libName = "libopensearch_datafusion_jni.so";
            }

            // Try to load from resources first
            InputStream libStream = DataFusionJNI.class.getResourceAsStream("/native/" + libName);
            if (libStream != null) {
                // Extract to temporary file and load
                Path tempLib = Files.createTempFile("libopensearch_datafusion_jni", libExtension);
                Files.copy(libStream, tempLib, StandardCopyOption.REPLACE_EXISTING);
                tempLib.toFile().deleteOnExit();
                System.load(tempLib.toAbsolutePath().toString());
                libStream.close();
            } else {
                // Fallback to system library path
                System.loadLibrary("opensearch_datafusion_jni");
            }

            libraryLoaded = true;
        } catch (IOException | UnsatisfiedLinkError e) {
            throw new RuntimeException("Failed to load DataFusion JNI library", e);
        }
    }

    /**
     * Get version information
     * @return JSON string with version information
     */
    public static native String getVersion();
}
