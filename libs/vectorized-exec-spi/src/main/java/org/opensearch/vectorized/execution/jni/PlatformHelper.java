/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.jni;

import java.util.Locale;

/**
 * Utility class for platform-specific operations and native library handling.
 * Provides methods to detect operating system, architecture, and generate
 * platform-specific library names and paths.
 */
public class PlatformHelper {

    private static final String OS_NAME = System.getProperty("os.name").toLowerCase(Locale.ROOT);
    private static final String OS_ARCH = System.getProperty("os.arch").toLowerCase(Locale.ROOT);


    /**
     * Gets the platform-specific library name with proper prefix and extension.
     * @param baseName the base library name without prefix/extension
     * @return platform-specific library name (e.g., "libfoo.so" on Linux)
     */
    public static String getPlatformLibraryName(String baseName) {
        if (isWindows()) {
            return baseName + ".dll";
        } else if (isMac()) {
            return "lib" + baseName + ".dylib";
        } else {
            return "lib" + baseName + ".so";
        }
    }

    /**
     * Gets the platform directory name in format "os-arch".
     * @return platform directory name (e.g., "linux-x64")
     */
    public static String getPlatformDirectory() {
        String os = getOSName();
        String arch = getArchName();
        return os + "-" + arch;
    }

    /**
     * Gets the normalized operating system name.
     * @return OS name ("windows", "macos", "linux", or "unknown")
     */
    public static String getOSName() {
        if (isWindows()) return "windows";
        if (isMac()) return "macos";
        if (isLinux()) return "linux";
        return "unknown";
    }

    /**
     * Checks if the current platform is Windows.
     * @return true if Windows, false otherwise
     */
    public static boolean isWindows() {
        return OS_NAME.contains("win");
    }

    /**
     * Checks if the current platform is macOS.
     * @return true if macOS, false otherwise
     */
    public static boolean isMac() {
        return OS_NAME.contains("mac") || OS_NAME.contains("darwin");
    }

    /**
     * Checks if the current platform is Linux.
     * @return true if Linux, false otherwise
     */
    public static boolean isLinux() {
        return OS_NAME.contains("linux");
    }

    /**
     * Gets the normalized architecture name.
     * @return architecture name ("x64", "x86", "arm64", or raw arch string)
     */
    public static String getArchName() {
        if (OS_ARCH.contains("amd64") || OS_ARCH.contains("x86_64")) {
            return "x64";
        } else if (OS_ARCH.contains("x86")) {
            return "x86";
        } else if (OS_ARCH.contains("aarch64") || OS_ARCH.contains("arm64")) {
            return "arm64";
        }
        return OS_ARCH;
    }

    /**
     * Gets the native library file extension for the current platform.
     * @return file extension (".dll", ".dylib", or ".so")
     */
    public static String getNativeExtension() {
        if (isWindows()) {
            return ".dll";
        } else if (isMac()) {
            return ".dylib";
        } else {
            return ".so";
        }
    }
}
