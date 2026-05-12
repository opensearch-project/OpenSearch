/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.ConsumableNativeHandle;

/**
 * Type-safe wrapper for a native {@code SessionContext} pointer returned by
 * {@link NativeBridge#createSessionContext}.
 *
 * <h2>Ownership</h2>
 * <p>On the happy path, {@link NativeBridge#executeWithContextAsync} transfers ownership of the
 * pointer to Rust, which takes it via {@code Box::from_raw} on the first line of
 * {@code df_execute_with_context} and drops it when the stream finishes. The bridge method
 * calls {@link ConsumableNativeHandle#markConsumed()} after the FFM downcall so that the
 * inherited {@link #doClose()} short-circuits without calling
 * {@code df_close_session_context} — doing so would be a double-free.
 *
 * <p>On any path where execute is never reached (Java-side error before the downcall, aborted
 * search, context closed before execution), {@link #doCloseNative()} calls
 * {@link NativeBridge#closeSessionContext(long)} which invokes the Rust
 * {@code df_close_session_context} entry to free the handle. Both the explicit
 * {@link #close()} call from {@link org.opensearch.be.datafusion.DatafusionContext#close()} and
 * the {@link java.lang.ref.Cleaner} GC-time fallback route through this path.
 */
public class SessionContextHandle extends ConsumableNativeHandle {

    public SessionContextHandle(long ptr) {
        super(ptr);
    }

    @Override
    protected void doCloseNative() {
        NativeBridge.closeSessionContext(ptr);
    }
}
