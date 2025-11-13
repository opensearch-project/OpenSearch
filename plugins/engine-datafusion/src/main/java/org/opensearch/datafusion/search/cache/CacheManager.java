/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;

import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerAddFiles;
import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerClear;
import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerClearByCacheType;
import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerGetItemByCacheType;
import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerGetMemoryConsumedForCacheType;
import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerGetTotalMemoryConsumed;
import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerRemoveFiles;
import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerUpdateSizeLimitForCacheType;
import static org.opensearch.datafusion.DataFusionQueryJNI.destroyCustomCacheManager;

/**
 * Manages cache lifecycle for DataFusion caches.
 * Holds the cache manager pointer for runtime cache operations.
 */
public class CacheManager implements Closeable {
    private static final Logger logger = LogManager.getLogger(CacheManager.class);

    public CacheManager() {
    }

    public void addFilesToCacheManager(List<String> files){
        try {
            if (files == null || files.isEmpty()) {
                return;
            }
            String[] filesArray = files.toArray(new String[0]);
            cacheManagerAddFiles(filesArray);
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
            cacheManagerRemoveFiles(filesArray);
        } catch (Exception e) {
            logger.error("Error removing files from cache manager: {}", e.getMessage(), e);
        }
    }

    public void clearAllCache(){
        try {
            cacheManagerClear();
        } catch (Exception e) {
            logger.error("Error clearing cache manager: {}", e.getMessage(), e);
        }
    }

    public void clearCacheForCacheType(CacheUtils.CacheType cacheType){
        try {
            cacheManagerClearByCacheType(cacheType.getCacheTypeName());
        } catch (Exception e) {
            logger.error("Error clearing cache manager for cache type {}: {}", cacheType.getCacheTypeName(), e.getMessage(), e);
        }
    }

    public long getMemoryConsumed(CacheUtils.CacheType cacheType){
        try {
            return cacheManagerGetMemoryConsumedForCacheType(cacheType.getCacheTypeName());
        } catch (Exception e) {
            logger.error("Error getting memory consumed for cache type {}: {}", cacheType.getCacheTypeName(), e.getMessage(), e);
            return 0;
        }
    }

    public long getTotalMemoryConsumed(){
        try {
            return cacheManagerGetTotalMemoryConsumed();
        } catch (Exception e) {
            logger.error("Error getting total memory consumed: {}", e.getMessage(), e);
            return 0;
        }
    }

    public void updateSizeLimit(CacheUtils.CacheType cacheType, long sizeLimit){
        try {
            cacheManagerUpdateSizeLimitForCacheType(cacheType.getCacheTypeName(), sizeLimit);
        } catch (Exception e) {
            logger.error("Error updating size limit for cache type {} to {}: {}", cacheType.getCacheTypeName(), sizeLimit, e.getMessage(), e);
        }
    }

    public boolean getEntryFromCacheType(CacheUtils.CacheType cacheType, String filePath){
        try {
            return cacheManagerGetItemByCacheType(cacheType.getCacheTypeName(), filePath);
        } catch (Exception e) {
            logger.error("Error getting entry from cache type {} for file {}: {}", cacheType.getCacheTypeName(), filePath, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            destroyCustomCacheManager();
        } catch (Exception e) {
            logger.error("Error closing cache manager: {}", e.getMessage(), e);
            throw new IOException("Failed to close cache manager", e);
        }
    }
}
