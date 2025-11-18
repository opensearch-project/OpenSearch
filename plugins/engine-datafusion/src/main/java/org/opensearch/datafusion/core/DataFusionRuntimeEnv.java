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
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheUtils;

/**
 * DataFusion runtime environment manager.
 * Manages the lifecycle of native DataFusion runtime (includes memory pool and Tokio runtime).
 */
public final class DataFusionRuntimeEnv implements AutoCloseable {

    private final GlobalRuntimeHandle runtimeHandle;

    private CacheManager cacheManager;

    /**
     * Controls the memory used for the datafusion query execution
     */
    public static final Setting<ByteSizeValue> MEMORY_POOL_CONFIGURATION_DATAFUSION = Setting.byteSizeSetting(
        "datafusion.search.memory_pool",
        new ByteSizeValue(2, ByteSizeUnit.GB),
        Setting.Property.Final,
        Setting.Property.NodeScope
    );

    /**
     * Creates a new DataFusion runtime environment.
     */
    public DataFusionRuntimeEnv(ClusterService clusterService) {
        long memoryLimit = clusterService.getClusterSettings().get(MEMORY_POOL_CONFIGURATION_DATAFUSION).getBytes();
        long cacheManagerPtr = CacheUtils.createCacheConfig(clusterService.getClusterSettings());
        this.runtimeHandle = new GlobalRuntimeHandle(memoryLimit, cacheManagerPtr);
        this.cacheManager = new CacheManager(runtimeHandle.getPointer());
    }

    /**
     * Gets the native pointer to the runtime environment.
     * @return the native pointer
     */
    public long getPointer() {
        return runtimeHandle.getPointer();
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
        runtimeHandle.close();
    }
}
