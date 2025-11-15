/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Handles loading of the native DataFusion JNI library.
 */
final class NativeLibraryLoader {

    private static volatile boolean loaded = false;
    private static final String LIB_NAME = "opensearch_datafusion_jni";

    private NativeLibraryLoader() {}

    static synchronized void load() {
        if (loaded) return;

        try {
            System.loadLibrary(LIB_NAME);
            loaded = true;
        } catch (UnsatisfiedLinkError e) {
            loadFromResources();
        }
    }

    private static void loadFromResources() {
        String libName = System.mapLibraryName(LIB_NAME);
        String resourcePath = "/native/" + libName;

        try (InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new NativeException("Native library not found: " + resourcePath);
            }

            Path tempFile = Files.createTempFile(LIB_NAME, getNativeExtension());
            tempFile.toFile().deleteOnExit();
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
            System.load(tempFile.toAbsolutePath().toString());
            loaded = true;
        } catch (IOException e) {
            throw new NativeException("Failed to load native library from resources", e);
        }
    }

    private static String getNativeExtension() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("windows")) return ".dll";
        if (osName.contains("mac")) return ".dylib";
        return ".so";
    }
}
