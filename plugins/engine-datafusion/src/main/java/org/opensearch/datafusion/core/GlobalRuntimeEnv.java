/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import static org.opensearch.datafusion.DataFusionQueryJNI.closeGlobalRuntime;
import static org.opensearch.datafusion.DataFusionQueryJNI.createGlobalRuntime;
import static org.opensearch.datafusion.DataFusionQueryJNI.createTokioRuntime;

/**
 * Global runtime environment for DataFusion operations.
 * Manages the lifecycle of the native DataFusion runtime.
 */
public class GlobalRuntimeEnv implements AutoCloseable {
    // ptr to runtime environment in df
    private final long ptr;
    private final long tokio_runtime_ptr;

    /**
     * Creates a new global runtime environment.
     */
    public GlobalRuntimeEnv() {
        this.ptr = createGlobalRuntime();
        this.tokio_runtime_ptr = createTokioRuntime();
    }

    /**
     * Gets the native pointer to the runtime environment.
     * @return the native pointer
     */
    public long getPointer() {
        return ptr;
    }

    public long getTokioRuntimePtr() {
        return tokio_runtime_ptr;
    }

    @Override
    public void close() {
        closeGlobalRuntime(this.ptr);
    }
}
