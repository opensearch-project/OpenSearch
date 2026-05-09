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
 * Verifies that the three new prepared-plan FFI entry points resolve against
 * the native library symbols. Full execution is not tested here — only that
 * the MethodHandles link successfully and the methods can be invoked without
 * a symbol-not-found error.
 */
public class NativeBridgePreparedPlanTests extends OpenSearchTestCase {

    public void testPreparePartialPlanRejectsNullPointer() {
        // Validates the Java-side pointer check fires before the native call.
        expectThrows(IllegalArgumentException.class, () -> NativeBridge.preparePartialPlan(0L, new byte[] { 0x01 }));
    }

    public void testPrepareFinalPlanRejectsNullPointer() {
        expectThrows(IllegalArgumentException.class, () -> NativeBridge.prepareFinalPlan(0L, new byte[] { 0x01 }));
    }

    public void testExecuteLocalPreparedPlanRejectsNullPointer() {
        expectThrows(IllegalArgumentException.class, () -> NativeBridge.executeLocalPreparedPlan(0L));
    }

    /**
     * Smoke test: create a local session, attempt to prepare a final plan with
     * garbage bytes — should fail with a decode error (not a link error).
     * This proves the MethodHandle resolved and the native function was called.
     */
    public void testPrepareFinalPlanWithInvalidBytesThrowsDecodeError() {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);
        DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
        try {
            RuntimeException ex = expectThrows(
                RuntimeException.class,
                () -> NativeBridge.prepareFinalPlan(session.getPointer(), new byte[] { 0x00, 0x01, 0x02 })
            );
            // The error should mention Substrait decode failure, not a symbol error
            assertTrue(
                "Expected decode error, got: " + ex.getMessage(),
                ex.getMessage().contains("decode") || ex.getMessage().contains("Substrait")
            );
        } finally {
            session.close();
            runtimeHandle.close();
        }
    }
}
