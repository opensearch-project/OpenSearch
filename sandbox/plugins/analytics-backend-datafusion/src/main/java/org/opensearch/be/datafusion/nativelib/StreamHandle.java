/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.NativeRuntimeHandle;

/**
 * Type-safe handle for a native DataFusion result stream.
 * Wraps the stream pointer returned by {@link NativeBridge#executeQueryAsync}.
 */
public final class StreamHandle extends NativeHandle {

    private final NativeRuntimeHandle runtimeHandle;

    /**
     * Creates a stream handle wrapping the native pointers.
     * @param streamPtr the native stream pointer
     * @param runtimeHandle handle to the native runtime (for streamNext calls)
     */
    public StreamHandle(long streamPtr, NativeRuntimeHandle runtimeHandle) {
        super(streamPtr);
        this.runtimeHandle = runtimeHandle;
    }

    /** Returns the native runtime handle. */
    public NativeRuntimeHandle getRuntimeHandle() {
        return runtimeHandle;
    }

    @Override
    protected void doClose() {
        NativeBridge.streamClose(ptr);
    }
}
