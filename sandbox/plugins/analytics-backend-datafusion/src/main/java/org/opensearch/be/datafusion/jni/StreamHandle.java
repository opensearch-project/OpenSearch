/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.jni;

import org.opensearch.analytics.backend.jni.NativeHandle;

/**
 * Type-safe handle for a native DataFusion result stream.
 * Wraps the stream pointer returned by {@link NativeBridge#executeQuery}.
 */
public final class StreamHandle extends NativeHandle {

    private final long streamPtr;

    public StreamHandle(long ptr, long streamPtr) {
        super(ptr);
        this.streamPtr = streamPtr;
    }

    public long getStreamPtr() {
        return streamPtr;
    }

    @Override
    protected void doClose() {
        NativeBridge.streamClose(ptr);
    }
}
