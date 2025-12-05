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

import java.io.Closeable;

/**
 * Reference-counted handle for native reader.
 */
public final class ReaderHandle extends RefCountedNativeHandle {

    private final Runnable onClose;

    public ReaderHandle(String path, String[] files, Runnable onClose) {
        super(NativeBridge.createDatafusionReader(path, files));
        this.onClose = onClose;
    }

    @Override
    protected void doClose() {
        NativeBridge.closeDatafusionReader(ptr);
        onClose.run();
    }
}
