/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.handle;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.vectorized.execution.jni.NativeHandle;

/**
 * Type-safe handle for native runtime environment.
 */
public final class GlobalRuntimeHandle extends NativeHandle {

    public GlobalRuntimeHandle(long memoryLimit, long cacheManagerConfigPtr, String spillDir, long spillLimit) {
        super(NativeBridge.createGlobalRuntime(memoryLimit,cacheManagerConfigPtr, spillDir, spillLimit));
    }

    /**
     * Closes the runtime environment and releases any associated resources.
     */
    @Override
    protected void doClose() {
        NativeBridge.closeGlobalRuntime(ptr);
    }
}
