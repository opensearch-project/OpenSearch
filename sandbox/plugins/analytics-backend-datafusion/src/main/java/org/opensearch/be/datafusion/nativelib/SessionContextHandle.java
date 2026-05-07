/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;

/**
 * Type-safe wrapper for a native SessionContext pointer returned by
 * {@link NativeBridge#createSessionContext}. The Rust side consumes this
 * handle when {@link NativeBridge#executeWithContextAsync} is called,
 * so {@link #doClose()} is a no-op — the pointer is freed by Rust internally.
 *
 * <p>This handle exists to participate in the {@link NativeHandle} live-pointer
 * registry so that {@link NativeHandle#validatePointer} passes for FFM calls.
 */
public class SessionContextHandle extends NativeHandle {

    public SessionContextHandle(long ptr) {
        super(ptr);
    }

    @Override
    protected void doClose() {
        // TODO: Handle error-path cleanup. Currently Rust consumes the handle in
        // execute_with_context (moves QueryTrackingContext into the stream). If execute
        // fails or is never called, this handle leaks on the Rust side.
        // Options: (a) AtomicBool 'consumed' flag on Rust handle — close_session_context
        // checks flag before freeing, (b) don't consume in Rust and use no-op tracking
        // on the stream, (c) markConsumed() on NativeHandle to skip doClose on happy path.
        // See df_close_session_context FFM entry which exists but is not yet wired here.
    }
}
