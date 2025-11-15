/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.handle;

/**
 * Base class for type-safe native pointer wrappers.
 */
public abstract class NativeHandle implements AutoCloseable {

    protected final long ptr;
    private volatile boolean closed = false;

    protected NativeHandle(long ptr) {
        if (ptr == 0) {
            throw new IllegalArgumentException("Null native pointer");
        }
        this.ptr = ptr;
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Handle already closed");
        }
    }

    public long getPointer() {
        ensureOpen();
        return ptr;
    }

    @Override
    public final void close() {
        if (!closed) {
            closed = true;
            doClose();
        }
    }

    protected abstract void doClose();
}
