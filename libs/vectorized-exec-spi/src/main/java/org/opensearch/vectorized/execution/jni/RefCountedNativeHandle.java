/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.jni;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reference-counted native handle for shared native resources.
 * Allows multiple owners to safely share a native pointer.
 * The native resource is released only when the reference count reaches zero.
 */
public abstract class RefCountedNativeHandle extends NativeHandle {

    private final AtomicInteger refCount = new AtomicInteger(1);

    /**
     * Creates a new reference-counted handle with initial reference count of 1.
     * @param ptr the native pointer (must not be 0)
     * @throws IllegalArgumentException if ptr is 0
     */
    protected RefCountedNativeHandle(long ptr) {
        super(ptr);
    }

    /**
     * Increments the reference count.
     * @throws IllegalStateException if the handle has been closed
     */
    public void retain() {
        ensureOpen();
        refCount.incrementAndGet();
    }

    /**
     * Decrements the reference count and closes the handle if it reaches zero.
     */
    @Override
    public final void close() {
        ensureOpen();
        if (refCount.decrementAndGet() == 0) {
            super.close();
        }
    }

    /**
     * Gets the current reference count.
     * @return the current reference count
     */
    public int getRefCount() {
        return refCount.get();
    }
}
