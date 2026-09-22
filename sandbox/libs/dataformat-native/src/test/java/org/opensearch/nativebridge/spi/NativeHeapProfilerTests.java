/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Tests for NativeHeapProfiler MBean operations.
 * These tests run with the native library loaded (same as NativeAllocatorConfigTests).
 */
public class NativeHeapProfilerTests extends OpenSearchTestCase {

    public void testActivateAndDeactivate() {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        assertFalse(profiler.isActive());

        profiler.activate();
        assertTrue(profiler.isActive());

        profiler.deactivate();
        assertFalse(profiler.isActive());
    }

    public void testDumpCreatesNonEmptyFile() throws Exception {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        Path dumpDir = createTempDir();
        NativeHeapProfiler.setAllowedDumpDirs(List.of(dumpDir.toString()));

        profiler.activate();
        Path dumpFile = dumpDir.resolve("test.heap");
        String result = profiler.dump(dumpFile.toString());
        profiler.deactivate();

        assertEquals(dumpFile.toString(), result);
        assertTrue(Files.exists(dumpFile));
        assertTrue(Files.size(dumpFile) > 0);
    }

    public void testDumpRejectsPathTraversal() {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        NativeHeapProfiler.setAllowedDumpDirs(List.of("/tmp"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> profiler.dump("/tmp/../etc/evil.heap"));
        assertTrue(e.getMessage().contains("Path traversal"));
    }

    public void testDumpRejectsRelativePath() {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        NativeHeapProfiler.setAllowedDumpDirs(List.of("/tmp"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> profiler.dump("relative/path.heap"));
        assertTrue(e.getMessage().contains("must be absolute"));
    }

    public void testDumpRejectsDisallowedPath() {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        NativeHeapProfiler.setAllowedDumpDirs(List.of("/data"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> profiler.dump("/etc/not_allowed.heap"));
        assertTrue(e.getMessage().contains("not under allowed directories"));
    }

    public void testDumpRejectsEmptyPath() {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        expectThrows(IllegalArgumentException.class, () -> profiler.dump(""));
    }

    public void testDumpRejectsNullPath() {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        expectThrows(IllegalArgumentException.class, () -> profiler.dump(null));
    }

    public void testResetRejectsInvalidLgSample() {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        expectThrows(IllegalArgumentException.class, () -> profiler.reset(-1));
        expectThrows(IllegalArgumentException.class, () -> profiler.reset(31));
    }

    public void testResetWithValidLgSample() {
        NativeHeapProfiler profiler = new NativeHeapProfiler();
        profiler.activate();
        // Should not throw
        profiler.reset(17);
        profiler.deactivate();
    }
}
