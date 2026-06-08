/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend.jni;

import java.lang.ref.Cleaner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for type-safe native pointer wrappers.
 *
 * <p>Provides automatic resource management, prevents use-after-close errors,
 * and tracks all live handles in a global registry to detect stale pointer usage.
 *
 * <p>Subclasses must implement {@link #doClose()} to release native resources.
 * Cleaner is used to ensure resources are cleaned up even if the object is not explicitly closed.
 *
 * <h2>Stale pointer detection</h2>
 * <p>All live native pointers are tracked in a global {@link #LIVE_HANDLES} set.
 * When a handle is created, its pointer is registered. When closed, it is unregistered.
 * Use {@link #isLivePointer(long)} to check if a raw pointer value is still valid
 * before passing it to native code.
 */
public abstract class NativeHandle implements AutoCloseable {

    /** Pointer to the native resource. */
    protected final long ptr;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /** Sentinel value representing a null native pointer. */
    protected static final long NULL_POINTER = 0L;
    private final Cleaner.Cleanable cleanable;

    private static final Cleaner CLEANER = Cleaner.create();

    /**
     * Global registry of all live native pointers.
     * Used to detect use-after-free: if a pointer is not in this set, it has
     * either been closed or was never created by a NativeHandle.
     */
    private static final Set<Long> LIVE_HANDLES = ConcurrentHashMap.newKeySet();

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
        LIVE_HANDLES.add(ptr);
        this.cleanable = CLEANER.register(this, new CleanupAction(ptr, this::doClose));
    }

    /**
     * Ensures the handle is still open.
     * @throws IllegalStateException if the handle has been closed
     */
    public void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException(getClass().getSimpleName() + " already closed (ptr=0x" + Long.toHexString(ptr) + ")");
        }
    }

    /**
     * Gets the native pointer value.
     * @return the native pointer
     * @throws IllegalStateException if the handle has been closed
     */
    public long getPointer() {
        ensureOpen();
        assert LIVE_HANDLES.contains(ptr) : "pointer 0x" + Long.toHexString(ptr) + " not in live registry";
        return ptr;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            assert LIVE_HANDLES.contains(ptr) : "closing handle not in live registry: 0x" + Long.toHexString(ptr);
            LIVE_HANDLES.remove(ptr);
            cleanable.clean();
        }
    }

    /**
     * Releases the native resource.
     * Called once when the handle is closed.
     * Subclasses must implement this to free native memory.
     */
    protected abstract void doClose();

    // ---- Stale pointer detection (static API) ----

    /**
     * Checks if a raw pointer value corresponds to a live, open NativeHandle.
     * Use this before passing raw pointer values to native code to detect
     * use-after-free bugs.
     *
     * @param ptr the raw pointer value to check
     * @return true if the pointer is tracked and has not been closed
     */
    public static boolean isLivePointer(long ptr) {
        return LIVE_HANDLES.contains(ptr);
    }

    /**
     * Validates that a raw pointer value is live, throwing if it is stale or unknown.
     * Use this as a guard before FFM downcalls that accept raw pointer arguments.
     *
     * @param ptr the raw pointer value to validate
     * @param name a descriptive name for error messages (e.g., "stream", "reader")
     * @throws IllegalStateException if the pointer is not in the live handle registry
     */
    public static void validatePointer(long ptr, String name) {
        if (ptr == NULL_POINTER) {
            throw new IllegalArgumentException(name + " pointer is null (0)");
        }
        if (LIVE_HANDLES.contains(ptr) == false) {
            throw new IllegalStateException(
                name
                    + " pointer 0x"
                    + Long.toHexString(ptr)
                    + " is not a live handle — "
                    + "it may have been closed or was never created by NativeHandle"
            );
        }
    }

    /**
     * Returns the number of currently live handles. Useful for leak detection in tests.
     *
     * @return the count of open native handles
     */
    public static int liveHandleCount() {
        return LIVE_HANDLES.size();
    }

    /**
     * Cleans up the native resource.
     * Called by the cleaner when the handle is garbage collected without explicit close.
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
            LIVE_HANDLES.remove(ptr);
            doClose.run();
        }
    }
}
