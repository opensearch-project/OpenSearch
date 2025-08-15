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

/**
 * Global runtime environment for DataFusion operations.
 * Manages the lifecycle of the native DataFusion runtime.
 */
public class GlobalRuntimeEnv implements AutoCloseable {
    // ptr to runtime environment in df
    private final long ptr;

    /**
     * Creates a new global runtime environment.
     */
    public GlobalRuntimeEnv() {
        this.ptr = createGlobalRuntime();
    }

    /**
     * Gets the native pointer to the runtime environment.
     * @return the native pointer
     */
    public long getPointer() {
        return ptr;
    }

    @Override
    public void close() {
        closeGlobalRuntime(this.ptr);
    }
}
