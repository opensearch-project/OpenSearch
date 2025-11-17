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

import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.datafusion.jni.handle.GlobalRuntimeHandle;

/**
 * DataFusion runtime environment manager.
 * Manages the lifecycle of native DataFusion runtime (includes memory pool and Tokio runtime).
 */
public final class DataFusionRuntimeEnv implements AutoCloseable {

    private final GlobalRuntimeHandle runtimeHandle;

    public static final Setting<ByteSizeValue> MEMORY_POOL_CONFIGURATION_DATAFUSION = Setting.byteSizeSetting(
        "datafusion.search.memory_pool",
        new ByteSizeValue(2, ByteSizeUnit.GB),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Creates a new DataFusion runtime environment.
     */
    public DataFusionRuntimeEnv(ClusterService clusterService) {
        long memoryLimit = clusterService.getClusterSettings().get(MEMORY_POOL_CONFIGURATION_DATAFUSION).getBytes();
        this.runtimeHandle = new GlobalRuntimeHandle(memoryLimit);
    }

    /**
     * Gets the native pointer to the runtime environment.
     * @return the native pointer
     */
    public long getPointer() {
        return runtimeHandle.getPointer();
    }

    @Override
    public void close() {
        runtimeHandle.close();
    }
}
