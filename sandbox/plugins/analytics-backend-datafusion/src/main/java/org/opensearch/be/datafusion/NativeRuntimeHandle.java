/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * Thread-safe wrapper around a native runtime pointer.
 * <p>
 * Encapsulates the raw {@code long} so it cannot be copied or used after
 * the runtime is destroyed. All consumers obtain the pointer via {@link #get()}
 * which performs a liveness check on every call.
 * <p>
 * Implements {@link Closeable} so it integrates with try-with-resources,
 * {@code IOUtils.close()}, and leak detection infrastructure.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NativeRuntimeHandle implements Closeable {

    private volatile long pointer;

    /**
     * Creates a handle wrapping the given native pointer.
     *
     * @param pointer the native runtime pointer (must be non-zero)
     * @throws IllegalArgumentException if pointer is zero
     */
    public NativeRuntimeHandle(long pointer) {
        if (pointer == 0L) {
            throw new IllegalArgumentException("Cannot create NativeRuntimeHandle with null pointer");
        }
        this.pointer = pointer;
    }

    /**
     * Returns the native runtime pointer, checking that it is still live.
     *
     * @throws IllegalStateException if the handle has been closed
     */
    public long get() {
        long ptr = pointer;
        if (ptr == 0L) {
            throw new IllegalStateException("Native runtime handle has been closed");
        }
        return ptr;
    }

    /**
     * Returns true if the handle has not been closed.
     */
    public boolean isOpen() {
        return pointer != 0L;
    }

    /**
     * Releases the native runtime. Idempotent and thread-safe.
     * After this call, {@link #get()} will throw.
     */
    @Override
    public synchronized void close() {
        long ptr = pointer;
        if (ptr != 0L) {
            // TODO: NativeBridge.closeGlobalRuntime(ptr);
            pointer = 0L;
        }
    }
}
