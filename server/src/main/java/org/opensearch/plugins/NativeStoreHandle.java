/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe, closeable wrapper around a native (Rust) object store pointer.
 *
 * <p>Prevents use-after-close by guarding {@link #getPointer()} with a
 * liveness check. The destructor function is captured at creation time,
 * so the pointer and its cleanup are always paired.
 *
 * <p>All live pointers are tracked in a global registry ({@link #LIVE_POINTERS}).
 * Use {@link #isLivePointer(long)} to validate a raw pointer before passing it
 * to native code — catches use-after-free bugs as exceptions instead of SIGSEGV.
 *
 * <p>Instances are created by {@link NativeRemoteObjectStoreProvider} and
 * owned by the repository that holds the native store pointer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class NativeStoreHandle implements AutoCloseable {

    /** Sentinel representing "no native store". Safe to close (no-op). */
    public static final NativeStoreHandle EMPTY = new NativeStoreHandle();

    /**
     * Global registry of all live native pointers managed by NativeStoreHandle.
     * Used to detect use-after-free: if a pointer is not in this set, it has
     * been closed or was never created by a NativeStoreHandle.
     */
    private static final Set<Long> LIVE_POINTERS = ConcurrentHashMap.newKeySet();

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
        LIVE_POINTERS.add(ptr);
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
     * Checks the global registry to detect if the pointer was closed from any reference.
     */
    public boolean isLive() {
        return this != EMPTY && LIVE_POINTERS.contains(ptr);
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
            LIVE_POINTERS.remove(ptr);
            destroyer.destroy(ptr);
        }
    }

    /**
     * Checks if a raw pointer value corresponds to a live, open NativeStoreHandle.
     * Use this before passing raw pointer values to native code to detect
     * use-after-free bugs.
     *
     * @param ptr the raw pointer value to check
     * @return true if the pointer is tracked and has not been closed
     */
    public static boolean isLivePointer(long ptr) {
        return LIVE_POINTERS.contains(ptr);
    }

    /**
     * Validates that a raw pointer value is live, throwing if it is stale or unknown.
     * Use this as a guard before FFM downcalls that accept raw pointer arguments.
     *
     * @param ptr  the raw pointer value to validate
     * @param name a descriptive name for error messages (e.g., "storeHandle", "nativeStoreForReader")
     * @throws IllegalArgumentException if ptr is 0 or negative
     * @throws IllegalStateException    if the pointer is not in the live registry
     */
    public static void validatePointer(long ptr, String name) {
        if (ptr <= 0) {
            throw new IllegalArgumentException(name + " pointer is invalid: " + ptr);
        }
        if (LIVE_POINTERS.contains(ptr) == false) {
            throw new IllegalStateException(
                name + " pointer 0x" + Long.toHexString(ptr) + " is not a live handle — already closed or never created"
            );
        }
    }

    /**
     * Returns the number of currently live handles. Useful for leak detection in tests.
     *
     * @return the count of open native store handles
     */
    public static int liveHandleCount() {
        return LIVE_POINTERS.size();
    }
}
