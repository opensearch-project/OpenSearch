/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.handle;

import org.opensearch.datafusion.ObjectResultCallback;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.vectorized.execution.jni.NativeHandle;

/**
 * Type-safe handle for native stream.
 */
public final class StreamHandle extends NativeHandle {

    public StreamHandle(long ptr) {
        super(ptr);
    }

    @Override
    protected void doClose() {
        NativeBridge.streamClose(ptr);
    }

    /**
     * Gets the next batch from this stream.
     * @param runtimePtr the runtime pointer
     * @param callback callback to receive the batch result
     */
    public void next(long runtimePtr, ObjectResultCallback callback) {
        NativeBridge.streamNext(runtimePtr, getPointer(), callback);
    }

    /**
     * Gets the schema for this stream.
     * @param callback callback to receive the schema result
     */
    public void getSchema(ObjectResultCallback callback) {
        NativeBridge.streamGetSchema(getPointer(), callback);
    }

}
