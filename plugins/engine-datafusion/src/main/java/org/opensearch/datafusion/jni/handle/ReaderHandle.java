/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.handle;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.vectorized.execution.jni.RefCountedNativeHandle;

/**
 * Reference-counted handle for native reader.
 */
public final class ReaderHandle extends RefCountedNativeHandle {

    public ReaderHandle(String path, String[] files) {
        super(NativeBridge.createDatafusionReader(path, files));
    }

    @Override
    protected void doClose() {
        NativeBridge.closeDatafusionReader(ptr);
    }
}
