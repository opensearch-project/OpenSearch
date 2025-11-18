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

public class CacheHandle extends NativeHandle {


    /**
     * Creates a new native handle.
     *
     * @throws IllegalArgumentException if ptr is 0
     */
    public CacheHandle() {
        super(NativeBridge.createCustomCacheManager());
    }

    @Override
    protected void doClose() {
        NativeBridge.destroyCustomCacheManager(ptr);
    }
}
