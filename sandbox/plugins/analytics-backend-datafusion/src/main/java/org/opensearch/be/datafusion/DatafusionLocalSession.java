/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

/**
 * Type-safe wrapper around a native DataFusion {@code LocalSession} pointer, used by the
 * coordinator-reduce path ({@link DatafusionReduceSink}).
 *
 * <p>The session holds a DataFusion {@code SessionContext} bound to the node-global runtime's
 * memory pool and disk manager. It owns any input partition streams registered via
 * {@link NativeBridge#registerPartitionStream(long, String, byte[])} and drops them when the
 * session itself is closed.
 */
public final class DatafusionLocalSession extends NativeHandle {

    /**
     * Creates a new local session tied to the given global runtime pointer.
     *
     * @param runtimePtr pointer returned by {@link NativeBridge#createGlobalRuntime}
     * @param contextId  the parent {@code AnalyticsQueryTask.getId()} used to attribute this
     *                   reduce's native memory to the coordinator search task (0 disables
     *                   per-query tracking)
     */
    public DatafusionLocalSession(long runtimePtr, long contextId) {
        super(NativeBridge.createLocalSession(runtimePtr, contextId));
    }

    @Override
    protected void doClose() {
        NativeBridge.closeLocalSession(ptr);
    }
}
