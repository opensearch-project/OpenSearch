/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NativeStoreHandleTests extends OpenSearchTestCase {

    public void testCreateRegistersInLivePointers() {
        NativeStoreHandle handle = new NativeStoreHandle(100L, ptr -> {});
        assertTrue(NativeStoreHandle.isLivePointer(100L));
        handle.close();
    }

    public void testCloseRemovesFromLivePointers() {
        NativeStoreHandle handle = new NativeStoreHandle(200L, ptr -> {});
        assertTrue(NativeStoreHandle.isLivePointer(200L));
        handle.close();
        assertFalse(NativeStoreHandle.isLivePointer(200L));
    }

    public void testIsLiveReturnsTrueWhenOpen() {
        NativeStoreHandle handle = new NativeStoreHandle(300L, ptr -> {});
        assertTrue(handle.isLive());
        handle.close();
    }

    public void testIsLiveReturnsFalseAfterClose() {
        NativeStoreHandle handle = new NativeStoreHandle(400L, ptr -> {});
        handle.close();
        assertFalse(handle.isLive());
    }

    public void testIsLiveReturnsFalseForEmpty() {
        assertFalse(NativeStoreHandle.EMPTY.isLive());
    }

    public void testGetPointerThrowsAfterClose() {
        NativeStoreHandle handle = new NativeStoreHandle(500L, ptr -> {});
        handle.close();
        expectThrows(IllegalStateException.class, handle::getPointer);
    }

    public void testGetPointerReturnsValueWhenOpen() {
        NativeStoreHandle handle = new NativeStoreHandle(600L, ptr -> {});
        assertEquals(600L, handle.getPointer());
        handle.close();
    }

    public void testEmptyGetPointerReturnsNegativeOne() {
        assertEquals(-1L, NativeStoreHandle.EMPTY.getPointer());
    }

    public void testCloseIsIdempotent() {
        AtomicInteger destroyCount = new AtomicInteger(0);
        NativeStoreHandle handle = new NativeStoreHandle(700L, ptr -> destroyCount.incrementAndGet());
        handle.close();
        handle.close();
        handle.close();
        assertEquals("Destroyer should only be called once", 1, destroyCount.get());
    }

    public void testDestroyerCalledWithCorrectPointer() {
        AtomicBoolean called = new AtomicBoolean(false);
        long[] capturedPtr = new long[1];
        NativeStoreHandle handle = new NativeStoreHandle(800L, ptr -> {
            capturedPtr[0] = ptr;
            called.set(true);
        });
        handle.close();
        assertTrue(called.get());
        assertEquals(800L, capturedPtr[0]);
    }

    public void testValidatePointerSucceedsForLiveHandle() {
        NativeStoreHandle handle = new NativeStoreHandle(900L, ptr -> {});
        NativeStoreHandle.validatePointer(900L, "test");
        handle.close();
    }

    public void testValidatePointerThrowsForClosedHandle() {
        NativeStoreHandle handle = new NativeStoreHandle(1000L, ptr -> {});
        handle.close();
        expectThrows(IllegalStateException.class, () -> NativeStoreHandle.validatePointer(1000L, "test"));
    }

    public void testValidatePointerThrowsForUnknownPointer() {
        expectThrows(IllegalStateException.class, () -> NativeStoreHandle.validatePointer(99999L, "test"));
    }

    public void testValidatePointerThrowsForZero() {
        expectThrows(IllegalArgumentException.class, () -> NativeStoreHandle.validatePointer(0L, "test"));
    }

    public void testValidatePointerThrowsForNegative() {
        expectThrows(IllegalArgumentException.class, () -> NativeStoreHandle.validatePointer(-1L, "test"));
    }

    public void testLiveHandleCount() {
        int before = NativeStoreHandle.liveHandleCount();
        NativeStoreHandle h1 = new NativeStoreHandle(1100L, ptr -> {});
        NativeStoreHandle h2 = new NativeStoreHandle(1200L, ptr -> {});
        assertEquals(before + 2, NativeStoreHandle.liveHandleCount());
        h1.close();
        assertEquals(before + 1, NativeStoreHandle.liveHandleCount());
        h2.close();
        assertEquals(before, NativeStoreHandle.liveHandleCount());
    }

    public void testConstructorRejectsZeroPointer() {
        expectThrows(IllegalArgumentException.class, () -> new NativeStoreHandle(0L, ptr -> {}));
    }

    public void testConstructorRejectsNegativePointer() {
        expectThrows(IllegalArgumentException.class, () -> new NativeStoreHandle(-5L, ptr -> {}));
    }

    public void testConstructorRejectsNullDestroyer() {
        expectThrows(IllegalArgumentException.class, () -> new NativeStoreHandle(1300L, null));
    }

    public void testEmptyCloseIsNoOp() {
        // Should not throw
        NativeStoreHandle.EMPTY.close();
        NativeStoreHandle.EMPTY.close();
    }
}
