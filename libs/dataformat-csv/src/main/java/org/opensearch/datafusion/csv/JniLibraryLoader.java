/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Utility class for loading the data source JNI library.
 */
public class JniLibraryLoader {

    private static final Logger logger = LogManager.getLogger(JniLibraryLoader.class);
    private static volatile boolean libraryLoaded = false;

    private static final String LIBRARY_NAME = "opensearch_datafusion_csv_jni";

    /**
     * Loads the DataFusion JNI library. This method is thread-safe and will only
     * load the library once.
     */
    public static synchronized void loadLibrary() {
        if (libraryLoaded) {
            return;
        }

        try {
            // First try to load from system library path
            System.loadLibrary(LIBRARY_NAME);
            logger.info("Loaded DataFusion JNI library from system path");
            libraryLoaded = true;
            return;
        } catch (UnsatisfiedLinkError e) {
            logger.debug("Could not load library from system path, trying to extract from JAR", e);
        }

        // Try to extract and load from JAR resources
        String libraryPath = extractLibraryFromJar();
        if (libraryPath != null) {
            try {
                System.load(libraryPath);
                logger.info("Loaded DataFusion JNI library from extracted path: {}", libraryPath);
                libraryLoaded = true;
                return;
            } catch (UnsatisfiedLinkError e) {
                logger.error("Failed to load extracted library from: " + libraryPath, e);
            }
        }

        throw new RuntimeException("Failed to load DataFusion JNI library");
    }

    /**
     * Extracts the platform-specific JNI library from JAR resources to a temporary file.
     *
     * @return Path to the extracted library file, or null if extraction failed
     */
    private static String extractLibraryFromJar() {
        String osName = System.getProperty("os.name").toLowerCase();
        String osArch = System.getProperty("os.arch").toLowerCase();

        logger.debug("Detecting platform: OS={}, Arch={}", osName, osArch);

        String libraryFileName = getLibraryFileName(osName);
        if (libraryFileName == null) {
            logger.error("Unsupported platform: {}", osName);
            return null;
        }

        String resourcePath = "/" + libraryFileName;
        logger.debug("Looking for library resource: {}", resourcePath);

        try (InputStream inputStream = JniLibraryLoader.class.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                logger.error("Library resource not found: {}", resourcePath);
                return null;
            }

            // Create temporary file
            Path tempDir = Files.createTempDirectory("datafusion-jni");
            Path tempLibrary = tempDir.resolve(libraryFileName);

            // Extract library to temporary file
            Files.copy(inputStream, tempLibrary, StandardCopyOption.REPLACE_EXISTING);

            // Make executable on Unix-like systems
            if (!osName.contains("windows")) {
                tempLibrary.toFile().setExecutable(true);
            }

            // Schedule cleanup on JVM shutdown
            tempLibrary.toFile().deleteOnExit();
            tempDir.toFile().deleteOnExit();

            String libraryPath = tempLibrary.toAbsolutePath().toString();
            logger.debug("Extracted library to: {}", libraryPath);
            return libraryPath;

        } catch (IOException e) {
            logger.error("Failed to extract library from JAR", e);
            return null;
        }
    }

    /**
     * Gets the platform-specific library file name.
     *
     * @param osName Operating system name
     * @return Library file name, or null if platform is unsupported
     */
    private static String getLibraryFileName(String osName) {
        String prefix;
        String extension;

        if (osName.contains("windows")) {
            prefix = "";
            extension = ".dll";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            prefix = "lib";
            extension = ".dylib";
        } else if (osName.contains("linux") || osName.contains("unix")) {
            prefix = "lib";
            extension = ".so";
        } else {
            return null;
        }

        return prefix + LIBRARY_NAME + extension;
    }

    /**
     * Checks if the JNI library has been loaded.
     *
     * @return true if the library is loaded, false otherwise
     */
    public static boolean isLibraryLoaded() {
        return libraryLoaded;
    }
}
