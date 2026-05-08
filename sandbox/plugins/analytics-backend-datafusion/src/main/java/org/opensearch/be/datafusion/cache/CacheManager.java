/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.NativeRuntimeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

import java.util.List;

/**
 * Manages cache lifecycle for DataFusion caches.
 * Holds the cache manager pointer for runtime cache operations.
 */
public class CacheManager {
    private static final Logger logger = LogManager.getLogger(CacheManager.class);

    NativeRuntimeHandle runtimeHandle;

    public CacheManager(NativeRuntimeHandle runtimeHandle) {
        this.runtimeHandle = runtimeHandle;
    }

    public void addFilesToCacheManager(List<String> files) {
        try {
            if (files == null || files.isEmpty()) {
                return;
            }
            String[] filesArray = files.toArray(new String[0]);
            NativeBridge.cacheManagerAddFiles(runtimeHandle.get(), filesArray);
        } catch (Exception e) {
            logger.error("Error adding files to cache manager", e);
        }
    }

    public void removeFilesFromCacheManager(List<String> files) {
        try {
            if (files == null || files.isEmpty()) {
                return;
            }
            String[] filesArray = files.toArray(new String[0]);
            NativeBridge.cacheManagerRemoveFiles(runtimeHandle.get(), filesArray);
        } catch (Exception e) {
            logger.error("Error removing files from cache manager", e);
        }
    }

    public void clearAllCache() {
        try {
            NativeBridge.cacheManagerClear(runtimeHandle.get());
        } catch (Exception e) {
            logger.error("Error clearing cache manager", e);
        }
    }

    public void clearCacheForCacheType(CacheUtils.CacheType cacheType) {
        try {
            NativeBridge.cacheManagerClearByCacheType(runtimeHandle.get(), cacheType.getCacheTypeName());
        } catch (Exception e) {
            logger.error("Error clearing cache for cache type", e);
        }
    }

    public long getMemoryConsumed(CacheUtils.CacheType cacheType) {
        try {
            return NativeBridge.cacheManagerGetMemoryConsumedForCacheType(runtimeHandle.get(), cacheType.getCacheTypeName());
        } catch (Exception e) {
            logger.error("Error getting memory consumed for cache type", e);
            return 0;
        }
    }

    public long getTotalMemoryConsumed() {
        try {
            return NativeBridge.cacheManagerGetTotalMemoryConsumed(runtimeHandle.get());
        } catch (Exception e) {
            logger.error("Error getting total memory consumed", e);
            return 0;
        }
    }

    public void updateSizeLimit(CacheUtils.CacheType cacheType, long sizeLimit) {
        try {
            // TODO: Add updateSizeLimitForCacheType FFM function when needed
            logger.warn("updateSizeLimit not yet implemented for FFM bridge");
        } catch (Exception e) {
            logger.error("Error updating size limit", e);
        }
    }

    public boolean getEntryFromCacheType(CacheUtils.CacheType cacheType, String filePath) {
        try {
            return NativeBridge.cacheManagerGetItemByCacheType(runtimeHandle.get(), cacheType.getCacheTypeName(), filePath);
        } catch (Exception e) {
            logger.error("Error getting entry from cache", e);
            return false;
        }
    }
}
