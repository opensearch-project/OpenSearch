/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend.jni;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link NativeHandle} lifecycle, stale pointer detection, and registry tracking.
 */
public class NativeHandleTests extends OpenSearchTestCase {

    /** Concrete test handle that records whether doClose was called. */
    private static class TestHandle extends NativeHandle {
        boolean closed;

        TestHandle(long ptr) {
            super(ptr);
        }

        @Override
        protected void doClose() {
            closed = true;
        }
    }

    public void testConstructorRejectsZeroPointer() {
        expectThrows(IllegalArgumentException.class, () -> new TestHandle(0L));
    }

    public void testGetPointerReturnsValue() {
        TestHandle handle = new TestHandle(42L);
        assertEquals(42L, handle.getPointer());
        handle.close();
    }

    public void testGetPointerAfterCloseThrows() {
        TestHandle handle = new TestHandle(42L);
        handle.close();
        expectThrows(IllegalStateException.class, handle::getPointer);
    }

    public void testCloseCallsDoClose() {
        TestHandle handle = new TestHandle(42L);
        assertFalse(handle.closed);
        handle.close();
        assertTrue(handle.closed);
    }

    public void testDoubleCloseIsIdempotent() {
        TestHandle handle = new TestHandle(42L);
        handle.close();
        assertTrue(handle.closed);
        // Second close should not throw
        handle.close();
        assertTrue(handle.closed);
    }

    // ---- Stale pointer registry tests ----

    public void testIsLivePointerTrueWhileOpen() {
        TestHandle handle = new TestHandle(100L);
        assertTrue(NativeHandle.isLivePointer(100L));
        handle.close();
    }

    public void testIsLivePointerFalseAfterClose() {
        TestHandle handle = new TestHandle(101L);
        handle.close();
        assertFalse(NativeHandle.isLivePointer(101L));
    }

    public void testIsLivePointerFalseForUnknownPointer() {
        assertFalse(NativeHandle.isLivePointer(999999L));
    }

    public void testValidatePointerSucceedsWhileOpen() {
        TestHandle handle = new TestHandle(200L);
        // Should not throw
        NativeHandle.validatePointer(200L, "test");
        handle.close();
    }

    public void testValidatePointerThrowsAfterClose() {
        TestHandle handle = new TestHandle(201L);
        handle.close();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> NativeHandle.validatePointer(201L, "test"));
        assertTrue(ex.getMessage().contains("test"));
        assertTrue(ex.getMessage().contains("not a live handle"));
    }

    public void testValidatePointerThrowsForNullPointer() {
        expectThrows(IllegalArgumentException.class, () -> NativeHandle.validatePointer(0L, "test"));
    }

    public void testValidatePointerThrowsForUnknownPointer() {
        expectThrows(IllegalStateException.class, () -> NativeHandle.validatePointer(888888L, "unknown"));
    }

    public void testLiveHandleCountTracksOpenHandles() {
        int baseline = NativeHandle.liveHandleCount();
        TestHandle h1 = new TestHandle(301L);
        TestHandle h2 = new TestHandle(302L);
        assertEquals(baseline + 2, NativeHandle.liveHandleCount());

        h1.close();
        assertEquals(baseline + 1, NativeHandle.liveHandleCount());

        h2.close();
        assertEquals(baseline, NativeHandle.liveHandleCount());
    }

    public void testEnsureOpenMessageIncludesClassName() {
        TestHandle handle = new TestHandle(400L);
        handle.close();
        IllegalStateException ex = expectThrows(IllegalStateException.class, handle::ensureOpen);
        assertTrue(ex.getMessage().contains("TestHandle"));
        assertTrue(ex.getMessage().contains("0x190")); // 400 in hex
    }
}
