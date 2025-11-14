/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.jni.handle.MemoryPoolHandle;
import org.opensearch.datafusion.jni.handle.RuntimeHandle;
import org.opensearch.datafusion.jni.handle.TokioRunTimeHandle;

import static org.opensearch.datafusion.jni.NativeBridge.closeMemoryPool;
import static org.opensearch.datafusion.jni.NativeBridge.closeTokioRunTime;
import static org.opensearch.datafusion.jni.NativeBridge.createMemoryPool;

/**
 * Global runtime environment for DataFusion operations.
 * Manages the lifecycle of the native DataFusion runtime with automatic cleanup.
 * TODO Revisit Tokio runtime
 */
public final class GlobalRuntimeEnv implements AutoCloseable {

    private final RuntimeHandle runTimeHandle;
    private final MemoryPoolHandle memoryPoolHandle;
    private final TokioRunTimeHandle tokioRunTimeHandle;


    public static final Setting<Long> MEMORY_POOL_CONFIGURATION_DATAFUSION = Setting.longSetting(
        "datafusion.search.memory_pool",
        2_000_000_000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Creates a new global runtime environment.
     */
    public GlobalRuntimeEnv(ClusterService clusterService) {
        this.memoryPoolHandle = new MemoryPoolHandle(clusterService.getClusterSettings().get(MEMORY_POOL_CONFIGURATION_DATAFUSION));
        this.tokioRunTimeHandle = new TokioRunTimeHandle();
        this.runTimeHandle = new RuntimeHandle();
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
        return tokioRunTimeHandle.getPointer();
    }

    public long getMemoryPoolPtr() {
        return memoryPoolHandle.getPointer();
    }

    @Override
    public void close() {
        memoryPoolHandle.close();
        tokioRunTimeHandle.close();
        runTimeHandle.close();
    }
}
