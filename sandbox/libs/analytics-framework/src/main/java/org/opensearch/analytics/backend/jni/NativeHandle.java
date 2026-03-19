/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend.jni;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for type-safe native pointer wrappers.
 * Provides automatic resource management and prevents use-after-close errors.
 * Subclasses must implement {@link #doClose()} to release native resources.
 */
public abstract class NativeHandle implements AutoCloseable {

    protected final long ptr;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    protected static final long NULL_POINTER = 0L;
    private final Cleaner.Cleanable cleanable;

    private static final Cleaner CLEANER = Cleaner.create();

    protected NativeHandle(long ptr) {
        if (ptr == NULL_POINTER) {
            throw new IllegalArgumentException("Null native pointer");
        }
        this.ptr = ptr;
        this.cleanable = CLEANER.register(this, new CleanupAction(ptr, this::doClose));
    }

    public void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Handle already closed");
        }
    }

    public long getPointer() {
        ensureOpen();
        return ptr;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cleanable.clean();
        }
    }

    protected abstract void doClose();

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
