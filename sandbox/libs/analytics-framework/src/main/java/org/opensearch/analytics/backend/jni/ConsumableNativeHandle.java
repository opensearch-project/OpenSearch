/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend.jni;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Specialisation of {@link NativeHandle} for pointers whose ownership is transferred to the
 * native side by a specific FFM call (for example, Rust's {@code Box::from_raw} inside a
 * consuming function). After the consuming call the native resource is freed internally;
 * calling the matching {@code close_X} entry a second time would be a double-free, while
 * not calling it on the error path would leak.
 *
 * <p>The bridge method that performs the consuming FFM call must invoke
 * {@link #markConsumed()} after the downcall returns (typically in a {@code finally} block).
 * This:
 * <ul>
 *   <li>flips an internal flag so the inherited {@link #doClose()} short-circuits;</li>
 *   <li>eagerly closes the Java wrapper — the pointer is removed from
 *       {@link NativeHandle# LIVE_HANDLES}, subsequent {@link #getPointer()} calls
 *       throw, and {@link NativeHandle#validatePointer(long, String) validatePointer} rejects
 *       the now-dangling pointer value.</li>
 * </ul>
 *
 * <p>On paths where the consuming call never happened (pre-dispatch Java error, aborted flow,
 * Cleaner-at-GC fallback), {@link #doClose()} delegates to {@link #doCloseNative()} which
 * subclasses implement to free the native resource via the appropriate {@code close_X} FFM entry.
 *
 * <p>{@link #markConsumed()} is idempotent and safe to call after {@link #close()}.
 */
public abstract class ConsumableNativeHandle extends NativeHandle {

    /**
     * Set once the native side has taken ownership of {@link #ptr} via the consuming FFM call.
     * When {@code true}, {@link #doClose()} skips the call to {@link #doCloseNative()} to avoid
     * a double-free.
     */
    private final AtomicBoolean consumed = new AtomicBoolean(false);

    protected ConsumableNativeHandle(long ptr) {
        super(ptr);
    }

    /**
     * Marks this handle as having had its native pointer consumed by the bridge's
     * ownership-transferring FFM call, then closes the Java wrapper. See the class javadoc
     * for the full contract and typical call pattern.
     */
    public final void markConsumed() {
        consumed.set(true);
        close();
    }

    /**
     * @return {@code true} if {@link #markConsumed()} has been called.
     */
    protected final boolean isConsumed() {
        return consumed.get();
    }

    /**
     * Template method: short-circuits to a no-op when {@link #isConsumed()} is {@code true}
     * (the native side already freed the resource), otherwise delegates to
     * {@link #doCloseNative()}. Marked {@code final} so subclasses cannot bypass the guard.
     */
    @Override
    protected final void doClose() {
        if (isConsumed()) {
            return;
        }
        doCloseNative();
    }

    /**
     * Releases the native resource via the appropriate {@code close_X} FFM entry.
     * Called by {@link #doClose()} only when the handle has <b>not</b> been marked consumed,
     * i.e. on the error / never-executed path. Must be safe to call at most once per pointer.
     */
    protected abstract void doCloseNative();
}
