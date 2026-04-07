/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

/**
 * Tests for DataFusionService lifecycle and NativeRuntimeHandle.
 */
public class DataFusionServiceTests extends OpenSearchTestCase {

    private static boolean runtimeInitialized = false;

    private void ensureTokioInit() {
        if (runtimeInitialized == false) {
            NativeBridge.initTokioRuntimeManager(2);
            runtimeInitialized = true;
        }
    }

    public void testServiceStartStop() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64 * 1024 * 1024)
            .spillMemoryLimit(32 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .build();
        service.start();

        NativeRuntimeHandle handle = service.getNativeRuntime();
        assertNotNull(handle);
        assertTrue(handle.isOpen());
        assertTrue(handle.get() != 0);

        service.stop();
        assertFalse(handle.isOpen());
    }

    public void testGetNativeRuntimeBeforeStartThrows() {
        DataFusionService service = DataFusionService.builder().build();
        expectThrows(IllegalStateException.class, service::getNativeRuntime);
    }

    public void testNativeRuntimeHandleCloseIsIdempotent() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");
        long ptr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle handle = new NativeRuntimeHandle(ptr);

        assertTrue(handle.isOpen());
        handle.close();
        assertFalse(handle.isOpen());
        // Second close should not throw
        handle.close();
        assertFalse(handle.isOpen());
    }

    public void testNativeRuntimeHandleGetAfterCloseThrows() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");
        long ptr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle handle = new NativeRuntimeHandle(ptr);
        handle.close();
        expectThrows(IllegalStateException.class, handle::get);
    }

    public void testNativeRuntimeHandleRejectsZeroPointer() {
        expectThrows(IllegalArgumentException.class, () -> new NativeRuntimeHandle(0L));
    }

    public void testNativePanicIsCaughtAsException() {
        RuntimeException ex = expectThrows(RuntimeException.class, () -> NativeBridge.testPanic("test panic message"));
        assertTrue("Should contain panic message, got: " + ex.getMessage(), ex.getMessage().contains("test panic message"));
    }

    public void testCacheFileOperationsDoNotThrow() {
        ensureTokioInit();
        Path spillDir = createTempDir("spill");
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64 * 1024 * 1024)
            .spillMemoryLimit(32 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .build();
        service.start();

        // These are no-ops in the current Rust impl but should not throw
        service.onFilesAdded(java.util.List.of("/tmp/test1.parquet", "/tmp/test2.parquet"));
        service.onFilesDeleted(java.util.List.of("/tmp/test1.parquet"));
        service.onFilesAdded(null);
        service.onFilesDeleted(java.util.List.of());

        service.stop();
    }
}
