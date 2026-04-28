/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Type-safe handle for the native DataFusion global runtime.
 * <p>
 * Extends {@link NativeHandle} to get automatic resource management, Cleaner-based
 * GC safety net, stale pointer tracking, and double-close prevention.
 * <p>
 * The runtime pointer lives until {@link #close()} is called, which invokes
 * {@link NativeBridge#closeGlobalRuntime(long)}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NativeRuntimeHandle extends NativeHandle {

    /**
     * Creates a handle wrapping the given native runtime pointer.
     *
     * @param pointer the native runtime pointer (must be non-zero)
     * @throws IllegalArgumentException if pointer is zero
     */
    public NativeRuntimeHandle(long pointer) {
        super(pointer);
    }

    /**
     * Returns the native runtime pointer, checking that it is still live.
     * <p>
     * This method preserves backward compatibility with callers that use
     * {@code handle.get()} instead of {@code handle.getPointer()}.
     *
     * @return the native runtime pointer
     * @throws IllegalStateException if the handle has been closed
     */
    public long get() {
        return getPointer();
    }

    /**
     * Returns true if the handle has not been closed.
     */
    public boolean isOpen() {
        try {
            ensureOpen();
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }

    @Override
    protected void doClose() {
        NativeBridge.closeGlobalRuntime(ptr);
    }
}
