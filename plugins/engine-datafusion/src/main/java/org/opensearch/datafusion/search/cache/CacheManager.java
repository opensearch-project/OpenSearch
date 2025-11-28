/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search.cache;


import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.datafusion.core.DataFusionRuntimeEnv;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.jni.handle.GlobalRuntimeHandle;


/**
 * Manages cache lifecycle for DataFusion caches.
 * Holds the cache manager pointer for runtime cache operations.
 */
public class CacheManager {
    private static final Logger logger = LogManager.getLogger(CacheManager.class);

    GlobalRuntimeHandle globalRuntimeHandle;

    public CacheManager(GlobalRuntimeHandle runtimeHandle) {
        this.globalRuntimeHandle = runtimeHandle;
    }

    public void addFilesToCacheManager(List<String> files){
        try {
            if (files == null || files.isEmpty()) {
                return;
            }
            String[] filesArray = files.toArray(new String[0]);
            NativeBridge.cacheManagerAddFiles(globalRuntimeHandle.getPointer(), filesArray);
        } catch (Exception e) {
            logger.error("Error adding files to cache manager: {}", e.getMessage(), e);
        }
    }

    public void removeFilesFromCacheManager(List<String> files){
        try {
            if (files == null || files.isEmpty()) {
                return;
            }
            String[] filesArray = files.toArray(new String[0]);
            NativeBridge.cacheManagerRemoveFiles(globalRuntimeHandle.getPointer(), filesArray);
        } catch (Exception e) {
            logger.error("Error removing files from cache manager: {}", e.getMessage(), e);
        }
    }

    public void clearAllCache(){
        try {
            NativeBridge.cacheManagerClear(globalRuntimeHandle.getPointer());
        } catch (Exception e) {
            logger.error("Error clearing cache manager: {}", e.getMessage(), e);
        }
    }

    public void clearCacheForCacheType(CacheUtils.CacheType cacheType){
        try {
            NativeBridge.cacheManagerClearByCacheType(globalRuntimeHandle.getPointer(), cacheType.getCacheTypeName());
        } catch (Exception e) {
            logger.error("Error clearing cache manager for cache type {}: {}", cacheType.getCacheTypeName(), e.getMessage(), e);
        }
    }

    public long getMemoryConsumed(CacheUtils.CacheType cacheType){
        try {
            return NativeBridge.cacheManagerGetMemoryConsumedForCacheType(globalRuntimeHandle.getPointer(), cacheType.getCacheTypeName());
        } catch (Exception e) {
            logger.error("Error getting memory consumed for cache type {}: {}", cacheType.getCacheTypeName(), e.getMessage(), e);
            return 0;
        }
    }

    public long getTotalMemoryConsumed(){
        try {
            return NativeBridge.cacheManagerGetTotalMemoryConsumed(globalRuntimeHandle.getPointer());
        } catch (Exception e) {
            logger.error("Error getting total memory consumed: {}", e.getMessage(), e);
            return 0;
        }
    }

    public void updateSizeLimit(CacheUtils.CacheType cacheType, long sizeLimit){
        try {
            NativeBridge.cacheManagerUpdateSizeLimitForCacheType(globalRuntimeHandle.getPointer(), cacheType.getCacheTypeName(), sizeLimit);
        } catch (Exception e) {
            logger.error("Error updating size limit for cache type {} to {}: {}", cacheType.getCacheTypeName(), sizeLimit, e.getMessage(), e);
        }
    }

    public boolean getEntryFromCacheType(CacheUtils.CacheType cacheType, String filePath){
        try {
            return NativeBridge.cacheManagerGetItemByCacheType(globalRuntimeHandle.getPointer(), cacheType.getCacheTypeName(), filePath);
        } catch (Exception e) {
            logger.error("Error getting entry from cache type {} for file {}: {}", cacheType.getCacheTypeName(), filePath, e.getMessage(), e);
            return false;
        }
    }

}
