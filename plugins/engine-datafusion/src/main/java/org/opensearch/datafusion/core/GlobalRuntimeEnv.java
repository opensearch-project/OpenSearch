/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.jni.handle.RuntimeHandle;

/**
 * Global runtime environment for DataFusion operations.
 * Manages the lifecycle of the native DataFusion runtime with automatic cleanup.
 * TODO Revisit Tokio runtime
 */
public final class GlobalRuntimeEnv implements AutoCloseable {

    private final RuntimeHandle runTimeHandle;
    private final long tokioRuntimePtr;

    /**
     * Creates a new global runtime environment.
     */
    public GlobalRuntimeEnv() {
        this.runTimeHandle = new RuntimeHandle();
        this.tokioRuntimePtr = NativeBridge.createTokioRuntime();
    }

    /**
     * Gets the native pointer to the runtime environment.
     * @return the native pointer
     */
    public long getPointer() {
        return runTimeHandle.getPointer();
    }

    /**
     * Gets the Tokio runtime pointer.
     * @return the Tokio runtime pointer
     */
    public long getTokioRuntimePtr() {
        return tokioRuntimePtr;
    }

    @Override
    public void close() {
        runTimeHandle.close();
    }
}
