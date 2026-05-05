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
        // No-op: Rust consumes the SessionContext inside execute_with_context.
        // If execute is never called (error path), the Rust memory leaks — acceptable
        // for now since the process would be failing anyway.
    }
}
