/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

/**
 * Tests for the DynamicLimitPool — verifies that the memory pool limit
 * can be read and changed at runtime via the FFM bridge.
 */
public class DynamicMemoryPoolTests extends OpenSearchTestCase {

    private DataFusionService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeBridge.initTokioRuntimeManager(2, 4);
        Path spillDir = createTempDir("datafusion-spill");
        service = DataFusionService.builder()
            .memoryPoolLimit(64 * 1024 * 1024) // 64MB
            .spillMemoryLimit(32 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .build();
        service.start();
    }

    @Override
    public void tearDown() throws Exception {
        if (service != null) {
            service.stop();
        }
        NativeBridge.shutdownTokioRuntimeManager();
        super.tearDown();
    }

    public void testGetInitialPoolLimit() {
        long limit = service.getMemoryPoolLimit();
        assertEquals("Initial pool limit should be 64 MB", 64 * 1024 * 1024, limit);
    }

    public void testGetInitialPoolUsage() {
        long usage = service.getMemoryPoolUsage();
        assertEquals("Initial pool usage should be 0", 0, usage);
    }

    public void testSetPoolLimitIncrease() {
        long newLimit = 128L * 1024 * 1024; // 128MB
        service.setMemoryPoolLimit(newLimit);
        assertEquals("Pool limit should be updated to 128 MB", newLimit, service.getMemoryPoolLimit());
    }

    public void testSetPoolLimitDecrease() {
        long newLimit = 32L * 1024 * 1024; // 32MB
        service.setMemoryPoolLimit(newLimit);
        assertEquals("Pool limit should be updated to 32 MB", newLimit, service.getMemoryPoolLimit());
    }

    public void testSetPoolLimitMultipleTimes() {
        service.setMemoryPoolLimit(100L * 1024 * 1024);
        assertEquals(100L * 1024 * 1024, service.getMemoryPoolLimit());

        service.setMemoryPoolLimit(50L * 1024 * 1024);
        assertEquals(50L * 1024 * 1024, service.getMemoryPoolLimit());

        service.setMemoryPoolLimit(200L * 1024 * 1024);
        assertEquals(200L * 1024 * 1024, service.getMemoryPoolLimit());
    }

    public void testDirectNativeBridgeCalls() {
        long runtimePtr = service.getNativeRuntime().get();

        long limit = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals(64 * 1024 * 1024, limit);

        NativeBridge.setMemoryPoolLimit(runtimePtr, 256L * 1024 * 1024);
        assertEquals(256L * 1024 * 1024, NativeBridge.getMemoryPoolLimit(runtimePtr));

        long usage = NativeBridge.getMemoryPoolUsage(runtimePtr);
        assertTrue("Usage should be >= 0", usage >= 0);
    }

    /**
     * H1 — after the service has been stopped, {@link DataFusionService#setMemoryPoolLimit}
     * must surface an {@link IllegalStateException} rather than dereferencing a closed runtime
     * handle. The plugin-level listener catches this to keep cluster-state updates quiet during
     * node shutdown.
     */
    public void testSetMemoryPoolLimitAfterStopThrowsIllegalState() {
        service.stop();
        expectThrows(IllegalStateException.class, () -> service.setMemoryPoolLimit(128L * 1024 * 1024));
        // Null out so tearDown does not try to stop again.
        service = null;
    }
}
