/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.jni;

import java.lang.ref.Cleaner;

/**
 * Base class for type-safe native pointer wrappers.
 * Provides automatic resource management and prevents use-after-close errors.
 * Subclasses must implement {@link #doClose()} to release native resources.
 * Cleaner is used to ensure resources are cleaned up even if the object is not explicitly closed.
 */
public abstract class NativeHandle implements AutoCloseable {

    protected final long ptr;
    private volatile boolean closed = false;
    protected static final long NULL_POINTER = 0L;
    private final Cleaner.Cleanable cleanable;

    private static final Cleaner CLEANER = Cleaner.create();

    /**
     * Creates a new native handle.
     * @param ptr the native pointer (must not be 0)
     * @throws IllegalArgumentException if ptr is 0
     */
    protected NativeHandle(long ptr) {
        if (ptr == NULL_POINTER) {
            throw new IllegalArgumentException("Null native pointer");
        }
        this.ptr = ptr;
        this.cleanable = CLEANER.register(this, new CleanupAction(ptr, this::doClose));
    }

    /**
     * Ensures the handle is still open.
     * @throws IllegalStateException if the handle has been closed
     */
    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Handle already closed");
        }
    }

    /**
     * Gets the native pointer value.
     * @return the native pointer
     * @throws IllegalStateException if the handle has been closed
     */
    public long getPointer() {
        ensureOpen();
        return ptr;
    }

    @Override
    public final void close() {
        if (!closed) {
            synchronized (this) {
                if (!closed) {
                    closed = true;
                    cleanable.clean();  // Triggers cleanup immediately
                }
            }
        }
    }

    /**
     * Releases the native resource.
     * Called once when the handle is closed.
     * Subclasses must implement this to free native memory.
     */
    protected abstract void doClose();

    /**
     * Cleans up the native resource.
     * Called by the cleaner when the handle is garbage collected.
     */
    private static final class CleanupAction implements Runnable {
        private final long ptr;
        private final Runnable doClose;

        CleanupAction(long ptr, Runnable doClose) {
            this.ptr = ptr;
            this.doClose = doClose;
        }

        @Override
        public void run() {
            doClose.run();
        }
    }
}
