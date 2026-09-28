/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.test.OpenSearchTestCase;

public class PlatformHelperTests extends OpenSearchTestCase {

    public void testGetPlatformLibraryNameMatchesCurrentOS() {
        String libName = PlatformHelper.getPlatformLibraryName("testlib");
        if (PlatformHelper.isMac()) {
            assertEquals("libtestlib.dylib", libName);
        } else if (PlatformHelper.isWindows()) {
            assertEquals("testlib.dll", libName);
        } else if (PlatformHelper.isLinux()) {
            assertEquals("libtestlib.so", libName);
        }
    }

    public void testGetNativeExtensionMatchesCurrentOS() {
        String ext = PlatformHelper.getNativeExtension();
        if (PlatformHelper.isMac()) {
            assertEquals(".dylib", ext);
        } else if (PlatformHelper.isWindows()) {
            assertEquals(".dll", ext);
        } else {
            assertEquals(".so", ext);
        }
    }

    public void testGetPlatformDirectory() {
        String dir = PlatformHelper.getPlatformDirectory();
        String expected = PlatformHelper.getOSName() + "-" + PlatformHelper.getArchName();
        assertEquals(expected, dir);
    }

    public void testGetOSNameReturnsKnownValue() {
        String osName = PlatformHelper.getOSName();
        assertTrue(
            "OS name should be one of: macos, linux, windows, unknown but was: " + osName,
            osName.equals("macos") || osName.equals("linux") || osName.equals("windows") || osName.equals("unknown")
        );
    }

    public void testGetArchNameReturnsKnownValue() {
        String arch = PlatformHelper.getArchName();
        assertTrue(
            "Arch should be one of: x86_64, x86, aarch64 or raw arch but was: " + arch,
            arch.equals("x86_64") || arch.equals("x86") || arch.equals("aarch64") || arch.length() > 0
        );
    }

    public void testExactlyOneOSDetected() {
        int count = 0;
        if (PlatformHelper.isMac()) count++;
        if (PlatformHelper.isWindows()) count++;
        if (PlatformHelper.isLinux()) count++;
        assertTrue("Exactly one OS should be detected, or none for unknown OS", count <= 1);
    }

    public void testLibraryNameContainsBaseName() {
        String libName = PlatformHelper.getPlatformLibraryName("mylib");
        assertTrue("Library name should contain base name", libName.contains("mylib"));
    }
}
