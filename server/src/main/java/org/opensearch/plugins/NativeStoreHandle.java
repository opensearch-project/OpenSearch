/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe, closeable wrapper around a native (Rust) object store pointer.
 *
 * <p>Prevents use-after-close by guarding {@link #getPointer()} with a
 * liveness check. The destructor function is captured at creation time,
 * so the pointer and its cleanup are always paired.
 *
 * <p>Instances are created by {@link NativeRemoteObjectStoreProvider} and
 * owned by the repository that implements
 * {@link org.opensearch.repositories.NativeStoreAwareRepository}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class NativeStoreHandle implements AutoCloseable {

    /** Sentinel representing "no native store". Safe to close (no-op). */
    public static final NativeStoreHandle EMPTY = new NativeStoreHandle();

    private final long ptr;
    private final Destroyer destroyer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Callback to destroy the native resource.
     */
    @FunctionalInterface
    public interface Destroyer {
        void destroy(long ptr);
    }

    /**
     * Creates a live handle wrapping the given pointer.
     *
     * @param ptr       the native pointer (must be {@code > 0})
     * @param destroyer the function to call on close to free the native resource
     * @throws IllegalArgumentException if ptr is {@code <= 0} or destroyer is null
     */
    public NativeStoreHandle(long ptr, Destroyer destroyer) {
        if (ptr <= 0) {
            throw new IllegalArgumentException("Native store pointer must be > 0, got: " + ptr);
        }
        if (destroyer == null) {
            throw new IllegalArgumentException("Destroyer must not be null");
        }
        this.ptr = ptr;
        this.destroyer = destroyer;
    }

    /** Private constructor for the EMPTY sentinel. */
    private NativeStoreHandle() {
        this.ptr = -1;
        this.destroyer = null;
    }

    /**
     * Returns the native pointer, or {@code -1} for {@link #EMPTY}.
     *
     * @throws IllegalStateException if the handle has been closed
     */
    public long getPointer() {
        if (this == EMPTY) {
            return -1;
        }
        if (closed.get()) {
            throw new IllegalStateException("NativeStoreHandle already closed (ptr=0x" + Long.toHexString(ptr) + ")");
        }
        return ptr;
    }

    /**
     * Returns true if this handle holds a live pointer (not EMPTY, not closed).
     */
    public boolean isLive() {
        return this != EMPTY && closed.get() == false;
    }

    /**
     * Destroys the native resource. Safe to call multiple times — only the
     * first call invokes the destroyer. No-op on {@link #EMPTY}.
     */
    @Override
    public void close() {
        if (this == EMPTY) {
            return;
        }
        if (closed.compareAndSet(false, true)) {
            destroyer.destroy(ptr);
        }
    }
}
