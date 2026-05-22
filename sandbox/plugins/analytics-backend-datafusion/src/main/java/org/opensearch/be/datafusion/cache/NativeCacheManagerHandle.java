/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.cache;

import org.opensearch.analytics.backend.jni.ConsumableNativeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

/**
 * Handle for the native cache manager pointer.
 * Ownership transfers to the DataFusion runtime via createGlobalRuntime,
 * after which this handle is marked consumed.
 */
public class NativeCacheManagerHandle extends ConsumableNativeHandle {

    public NativeCacheManagerHandle(long ptr) {
        super(ptr);
    }

    @Override
    protected void doCloseNative() {
        NativeBridge.destroyCustomCacheManager(ptr);
    }
}
