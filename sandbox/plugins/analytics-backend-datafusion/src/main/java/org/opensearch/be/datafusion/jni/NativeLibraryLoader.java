/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.be.datafusion.jni;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Loads native libraries from the classpath (resources/native/).
 */
public class NativeLibraryLoader {

    private static volatile boolean loaded = false;

    public static synchronized void load(String libraryName) {
        if (loaded) return;

        String osName = System.getProperty("os.name").toLowerCase();
        String fileName;
        if (osName.contains("linux")) {
            fileName = "lib" + libraryName + ".so";
        } else if (osName.contains("mac")) {
            fileName = "lib" + libraryName + ".dylib";
        } else {
            throw new UnsupportedOperationException("Unsupported OS: " + osName);
        }

        String resourcePath = "/native/" + fileName;
        try (InputStream in = NativeLibraryLoader.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                // Fallback: try System.loadLibrary for development
                System.loadLibrary(libraryName);
                loaded = true;
                return;
            }
            Path tempFile = Files.createTempFile(libraryName, fileName.substring(fileName.lastIndexOf('.')));
            tempFile.toFile().deleteOnExit();
            Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
            System.load(tempFile.toAbsolutePath().toString());
            loaded = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load native library: " + fileName, e);
        }
    }
}
