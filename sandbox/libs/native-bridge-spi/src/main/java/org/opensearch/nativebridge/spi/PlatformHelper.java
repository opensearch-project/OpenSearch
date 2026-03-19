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

    public static String getPlatformLibraryName(String baseName) {
        if (isWindows()) {
            return baseName + ".dll";
        } else if (isMac()) {
            return "lib" + baseName + ".dylib";
        } else {
            return "lib" + baseName + ".so";
        }
    }

    public static String getPlatformDirectory() {
        return getOSName() + "-" + getArchName();
    }

    public static String getOSName() {
        if (isWindows()) return "windows";
        if (isMac()) return "macos";
        if (isLinux()) return "linux";
        return "unknown";
    }

    public static boolean isWindows() {
        return OS_NAME.contains("win");
    }

    public static boolean isMac() {
        return OS_NAME.contains("mac") || OS_NAME.contains("darwin");
    }

    public static boolean isLinux() {
        return OS_NAME.contains("linux");
    }

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

    public static String getNativeExtension() {
        if (isWindows()) return ".dll";
        if (isMac()) return ".dylib";
        return ".so";
    }

    private PlatformHelper() {}
}
