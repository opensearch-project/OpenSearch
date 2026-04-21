/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugins.NativeStoreHandle;

/**
 * Facade for native (Rust) object store interactions on a repository.
 *
 * <p>Wraps a {@link NativeStoreHandle} and provides the single entry point
 * for all native store operations. Today this exposes the pointer for FFM
 * callers; future operations (native read, list, register) will be added here
 * rather than leaking raw pointers.
 *
 * <p>Instances are created by repository implementations during construction
 * and closed in {@code doClose()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record NativeStoreRepository(NativeStoreHandle handle) implements AutoCloseable {

    /**
     * Sentinel representing "no native store". Safe to close (no-op).
     */
    public static final NativeStoreRepository EMPTY = new NativeStoreRepository(NativeStoreHandle.EMPTY);

    public NativeStoreRepository(NativeStoreHandle handle) {
        this.handle = handle != null ? handle : NativeStoreHandle.EMPTY;
    }

    /**
     * Returns the underlying native store pointer for FFM callers.
     *
     * @return the pointer ({@code > 0}), or {@code -1} if empty
     * @throws IllegalStateException if the handle has been closed
     */
    public long getPointer() {
        return handle.getPointer();
    }

    /**
     * Returns true if this holds a live native store (not empty, not closed).
     */
    public boolean isLive() {
        return handle.isLive();
    }

    /**
     * Destroys the native resource. Safe to call multiple times.
     */
    @Override
    public void close() {
        handle.close();
    }
}
