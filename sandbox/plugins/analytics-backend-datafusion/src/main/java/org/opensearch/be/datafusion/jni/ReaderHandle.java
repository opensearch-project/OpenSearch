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
 * Type-safe handle for native reader.
 */
public final class ReaderHandle extends NativeHandle {

    public ReaderHandle(String path, String[] files) {
        super(NativeBridge.createDatafusionReader(path, files));
    }

    /**
     * Closes the datafusion reader and releases any associated resources.
     */
    @Override
    protected void doClose() {
        NativeBridge.closeDatafusionReader(ptr);
    }
}
