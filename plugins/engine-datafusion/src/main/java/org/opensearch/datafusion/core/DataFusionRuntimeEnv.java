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
import org.opensearch.datafusion.jni.NativeBridge;
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
    public static final Setting<ByteSizeValue> DATAFUSION_MEMORY_POOL_CONFIGURATION = Setting.byteSizeSetting(
        "datafusion.search.memory_pool",
        new ByteSizeValue(10, ByteSizeUnit.GB),
        Setting.Property.Final,
        Setting.Property.NodeScope
    );

    /**
     * Controls the spill memory used for the datafusion query execution
     */
    public static final Setting<ByteSizeValue> DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION = Setting.byteSizeSetting(
        "datafusion.spill.memory_limit",
        new ByteSizeValue(20, ByteSizeUnit.GB),
        Setting.Property.Final,
        Setting.Property.NodeScope
    );

    /**
     * Creates a new DataFusion runtime environment.
     */
    public DataFusionRuntimeEnv(ClusterService clusterService, String spill_dir) {
        long memoryLimit = clusterService.getClusterSettings().get(DATAFUSION_MEMORY_POOL_CONFIGURATION).getBytes();
        long spillLimit = clusterService.getClusterSettings().get(DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION).getBytes();
        long cacheManagerConfigPtr = CacheUtils.createCacheConfig(clusterService.getClusterSettings());
        NativeBridge.initTokioRuntimeManager(Runtime.getRuntime().availableProcessors());
        NativeBridge.startTokioRuntimeMonitoring(); // TODO : do we need this control in java ?
        this.runtimeHandle = new GlobalRuntimeHandle(memoryLimit, cacheManagerConfigPtr, spill_dir, spillLimit);
        System.out.println("Runtime : " + this.runtimeHandle);
        this.cacheManager = new CacheManager(this.runtimeHandle);
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
        NativeBridge.shutdownTokioRuntimeManager();
    }
}
