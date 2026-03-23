/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import java.util.Locale;

/**
 * Utility class for platform-specific operations and native library handling.
 */
public class PlatformHelper {

    private static final String OS_NAME = System.getProperty("os.name").toLowerCase(Locale.ROOT);
    private static final String OS_ARCH = System.getProperty("os.arch").toLowerCase(Locale.ROOT);

    /**
     * Returns the platform-specific library file name for the given base name.
     * @param baseName the base library name without extension
     * @return the full library file name (e.g. libfoo.so, libfoo.dylib, foo.dll)
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
     * Returns the platform directory name in the format {@code os-arch}.
     * @return platform directory string (e.g. linux-x86_64, macos-aarch64)
     */
    public static String getPlatformDirectory() {
        return getOSName() + "-" + getArchName();
    }

    /**
     * Returns the normalized operating system name.
     * @return one of "windows", "macos", "linux", or "unknown"
     */
    public static String getOSName() {
        if (isWindows()) return "windows";
        if (isMac()) return "macos";
        if (isLinux()) return "linux";
        return "unknown";
    }

    /**
     * Returns {@code true} if the current OS is Windows.
     * @return true on Windows
     */
    public static boolean isWindows() {
        return OS_NAME.contains("win");
    }

    /**
     * Returns {@code true} if the current OS is macOS.
     * @return true on macOS
     */
    public static boolean isMac() {
        return OS_NAME.contains("mac") || OS_NAME.contains("darwin");
    }

    /**
     * Returns {@code true} if the current OS is Linux.
     * @return true on Linux
     */
    public static boolean isLinux() {
        return OS_NAME.contains("linux");
    }

    /**
     * Returns the normalized CPU architecture name.
     * @return one of "x86_64", "x86", "aarch64", or the raw arch string
     */
    public static String getArchName() {
        if (OS_ARCH.contains("amd64") || OS_ARCH.contains("x86_64")) {
            return "x86_64";
        } else if (OS_ARCH.contains("x86")) {
            return "x86";
        } else if (OS_ARCH.contains("aarch64") || OS_ARCH.contains("arm64")) {
            return "aarch64";
        }
        return OS_ARCH;
    }

    /**
     * Returns the native library file extension for the current platform.
     * @return ".dll", ".dylib", or ".so"
     */
    public static String getNativeExtension() {
        if (isWindows()) return ".dll";
        if (isMac()) return ".dylib";
        return ".so";
    }

    private PlatformHelper() {}
}
