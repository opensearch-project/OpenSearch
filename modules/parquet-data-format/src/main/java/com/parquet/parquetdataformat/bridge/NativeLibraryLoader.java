/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.vectorized.execution.jni.NativeLoaderException;
import org.opensearch.vectorized.execution.jni.PlatformHelper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * Handles loading of the native JNI library.
 * TODO move to common lib once we switch to passing absolute lib paths
 */
public final class NativeLibraryLoader {

    private static volatile boolean loaded = false;

    private static final String DEFAULT_PATH = "native";

    private static final Logger logger = LogManager.getLogger(NativeLibraryLoader.class);

    NativeLibraryLoader() {}

    /**
     * Load the native library by name.
     * Supports loading from resources and platform-specific directories.
     *
     * @throws UnsatisfiedLinkError if the library cannot be loaded
     */
    public static synchronized void load(String libraryName) {
        if (loaded) return;
        try {
            System.loadLibrary(libraryName);
            loaded = true;
            return;
        } catch (UnsatisfiedLinkError ignored) {
            logger.warn("Failed to load library '" + libraryName + "' from system path");
        }

        //Look-up with default path
        try {
            loadFromResources(DEFAULT_PATH, libraryName);
            return;
        }  catch (UnsatisfiedLinkError | IOException ignored) {
            logger.warn("Failed to load library '" + libraryName + "' from default path");
        }

        // Try platform-specific directory
        try {
            String platformDir = PlatformHelper.getPlatformDirectory();
            String currentDir = Optional.of(System.getProperty("user.dir")).orElse("/");
            String path = Paths.get(currentDir, "native", platformDir,
                PlatformHelper.getPlatformLibraryName(libraryName)).toString();
            loadFromResources(path, libraryName);
        } catch (UnsatisfiedLinkError | IOException e) {
            throw new NativeLoaderException(
                "Failed to load library '" + libraryName + "' from all attempted locations", e);
        }
    }

    private static void loadFromResources(String providedPath, String libraryName) throws IOException {
        String platformDir = PlatformHelper.getPlatformDirectory();
        String libName = PlatformHelper.getPlatformLibraryName(libraryName);
        String resourcePath = Paths.get("/", providedPath, platformDir, libName).toString();
        try (InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Native library not found: " + resourcePath);
            }
            Path tempFile = Files.createTempFile(libraryName, PlatformHelper.getNativeExtension());
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {}
            }));
            System.load(tempFile.toAbsolutePath().toString());
            loaded = true;
        }
    }
}
