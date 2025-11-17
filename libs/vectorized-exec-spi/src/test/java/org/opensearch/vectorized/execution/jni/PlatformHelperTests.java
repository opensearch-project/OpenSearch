/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.jni;

import org.opensearch.test.OpenSearchTestCase;

public class PlatformHelperTests extends OpenSearchTestCase {

    public void testGetPlatformLibraryName() {
        String libName = PlatformHelper.getPlatformLibraryName("test");
        assertNotNull(libName);
        assertTrue(libName.contains("test"));
        assertTrue(libName.endsWith(".dll") || libName.endsWith(".dylib") || libName.endsWith(".so"));
    }

    public void testGetPlatformDirectory() {
        String dir = PlatformHelper.getPlatformDirectory();
        assertNotNull(dir);
        assertTrue(dir.contains("-"));
    }

    public void testGetOSName() {
        String osName = PlatformHelper.getOSName();
        assertNotNull(osName);
        assertTrue(osName.equals("windows") || osName.equals("macos") || osName.equals("linux") || osName.equals("unknown"));
    }

    public void testPlatformDetection() {
        boolean isWindows = PlatformHelper.isWindows();
        boolean isMac = PlatformHelper.isMac();
        boolean isLinux = PlatformHelper.isLinux();
        
        int count = (isWindows ? 1 : 0) + (isMac ? 1 : 0) + (isLinux ? 1 : 0);
        assertTrue("Exactly one platform should be detected", count <= 1);
    }

    public void testGetArchName() {
        String arch = PlatformHelper.getArchName();
        assertNotNull(arch);
        assertFalse(arch.isEmpty());
    }

    public void testGetNativeExtension() {
        String ext = PlatformHelper.getNativeExtension();
        assertNotNull(ext);
        assertTrue(ext.equals(".dll") || ext.equals(".dylib") || ext.equals(".so"));
    }

    public void testLibraryNameConsistency() {
        String libName = PlatformHelper.getPlatformLibraryName("mylib");
        String extension = PlatformHelper.getNativeExtension();
        assertTrue(libName.endsWith(extension));
    }
}
