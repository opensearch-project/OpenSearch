/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import org.opensearch.datafusion.jni.NativeBridge;

import java.lang.ref.Cleaner;

/**
 * Global runtime environment for DataFusion operations.
 * Manages the lifecycle of the native DataFusion runtime with automatic cleanup.
 */
public final class GlobalRuntimeEnv implements AutoCloseable {

    private static final Cleaner CLEANER = Cleaner.create();

    private final long ptr;
    private final long tokioRuntimePtr;
    private final Cleaner.Cleanable cleanable;

    /**
     * Creates a new global runtime environment.
     */
    public GlobalRuntimeEnv() {
        this.ptr = NativeBridge.createGlobalRuntime();
        this.tokioRuntimePtr = NativeBridge.createTokioRuntime();
        this.cleanable = CLEANER.register(this, new CleanupAction(ptr, tokioRuntimePtr));
    }

    /**
     * Gets the native pointer to the runtime environment.
     * @return the native pointer
     */
    public long getPointer() {
        return ptr;
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
        cleanable.clean();
    }

    private static final class CleanupAction implements Runnable {
        private final long ptr;
        private final long tokioPtr;

        CleanupAction(long ptr, long tokioPtr) {
            this.ptr = ptr;
            this.tokioPtr = tokioPtr;
        }

        @Override
        public void run() {
            if (ptr != 0) {
                NativeBridge.closeGlobalRuntime(ptr);
            }
        }
    }
}
