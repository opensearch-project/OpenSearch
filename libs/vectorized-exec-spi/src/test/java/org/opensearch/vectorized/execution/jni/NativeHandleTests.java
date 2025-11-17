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

public class NativeHandleTests extends OpenSearchTestCase {

    private static class TestHandle extends NativeHandle {
        private final AtomicBoolean closed;

        TestHandle(long ptr, AtomicBoolean closed) {
            super(ptr);
            this.closed = closed;
        }

        @Override
        protected void doClose() {
            closed.set(true);
        }
    }

    public void testConstructorRejectsNullPointer() {
        AtomicBoolean closed = new AtomicBoolean(false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TestHandle(0L, closed));
        assertEquals("Null native pointer", e.getMessage());
    }

    public void testGetPointerReturnsValue() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestHandle handle = new TestHandle(12345L, closed);
        assertEquals(12345L, handle.getPointer());
    }

    public void testCloseCallsDoClose() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestHandle handle = new TestHandle(12345L, closed);
        assertFalse(closed.get());
        handle.close();
        assertTrue(closed.get());
    }

    public void testMultipleCloseCallsOnlyCloseOnce() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestHandle handle = new TestHandle(12345L, closed) {
            private int closeCount = 0;

            @Override
            protected void doClose() {
                closeCount++;
                assertEquals(1, closeCount);
                super.doClose();
            }
        };
        handle.close();
        handle.close();
        handle.close();
        assertTrue(closed.get());
    }

    public void testGetPointerAfterCloseThrows() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestHandle handle = new TestHandle(12345L, closed);
        handle.close();
        IllegalStateException e = expectThrows(IllegalStateException.class, handle::getPointer);
        assertEquals("Handle already closed", e.getMessage());
    }

    public void testEnsureOpenAfterCloseThrows() {
        AtomicBoolean closed = new AtomicBoolean(false);
        TestHandle handle = new TestHandle(12345L, closed);
        handle.close();
        IllegalStateException e = expectThrows(IllegalStateException.class, handle::ensureOpen);
        assertEquals("Handle already closed", e.getMessage());
    }
}
