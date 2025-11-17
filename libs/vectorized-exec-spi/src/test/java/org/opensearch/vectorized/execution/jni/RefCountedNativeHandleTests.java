/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.jni;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

public class RefCountedNativeHandleTests extends OpenSearchTestCase {

    private static class TestRefCountedHandle extends RefCountedNativeHandle {
        private final AtomicBoolean closed;

        TestRefCountedHandle(long ptr, AtomicBoolean closed) {
            super(ptr);
            this.closed = closed;
        }

        @Override
        protected void doClose() {
            closed.set(true);
        }
    }

    public void testInitialRefCountIsOne() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestRefCountedHandle handle = new TestRefCountedHandle(12345L, closed);
        assertEquals(1, handle.getRefCount());
    }

    public void testRetainIncrementsRefCount() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestRefCountedHandle handle = new TestRefCountedHandle(12345L, closed);
        handle.retain();
        assertEquals(2, handle.getRefCount());
        handle.retain();
        assertEquals(3, handle.getRefCount());
    }

    public void testCloseDecrementsRefCount() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestRefCountedHandle handle = new TestRefCountedHandle(12345L, closed);
        handle.retain();
        handle.retain();
        assertEquals(3, handle.getRefCount());
        handle.close();
        assertEquals(2, handle.getRefCount());
        assertFalse(closed.get());
    }

    public void testCloseReleasesWhenRefCountReachesZero() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestRefCountedHandle handle = new TestRefCountedHandle(12345L, closed);
        handle.retain();
        assertEquals(2, handle.getRefCount());
        handle.close();
        assertFalse(closed.get());
        handle.close();
        assertTrue(closed.get());
    }

    public void testRetainAfterCloseThrows() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestRefCountedHandle handle = new TestRefCountedHandle(12345L, closed);
        handle.close();
        IllegalStateException e = expectThrows(IllegalStateException.class, handle::retain);
        assertEquals("Handle already closed", e.getMessage());
    }

    public void testMultipleRetainAndClose() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestRefCountedHandle handle = new TestRefCountedHandle(12345L, closed);
        handle.retain();
        handle.retain();
        handle.retain();
        assertEquals(4, handle.getRefCount());
        handle.close();
        handle.close();
        handle.close();
        assertFalse(closed.get());
        assertEquals(1, handle.getRefCount());
        handle.close();
        assertTrue(closed.get());
    }
}
