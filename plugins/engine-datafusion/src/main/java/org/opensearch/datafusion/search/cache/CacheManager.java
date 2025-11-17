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
import static org.opensearch.datafusion.DataFusionQueryJNI.createCustomCacheManager;
import static org.opensearch.datafusion.DataFusionQueryJNI.destroyCustomCacheManager;
import static org.opensearch.datafusion.search.cache.CacheUtils.createCacheConfig;

/**
 * Manages cache lifecycle for DataFusion caches.
 * Holds the cache manager pointer for runtime cache operations.
 */
public class CacheManager implements Closeable {
    private static final Logger logger = LogManager.getLogger(CacheManager.class);

    private long cacheManagerPointer;

    public CacheManager(ClusterSettings clusterSettings) {
        this.cacheManagerPointer = createCacheConfig(clusterSettings);
        if (this.cacheManagerPointer == 0) {
            throw new IllegalStateException("Failed to create native cache manager");
        }
    }

    /**
     * Get the native cache manager pointer
     * @return the native pointer
     */
    public long getCacheManagerPointer() {
        return cacheManagerPointer;
    }

    public void addFilesToCacheManager(List<String> files){
        try {
            if (files == null || files.isEmpty()) {
                return;
            }
            String[] filesArray = files.toArray(new String[0]);
            cacheManagerAddFiles(cacheManagerPointer, filesArray);
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
            cacheManagerRemoveFiles(cacheManagerPointer, filesArray);
        } catch (Exception e) {
            logger.error("Error removing files from cache manager: {}", e.getMessage(), e);
        }
    }

    public void clearAllCache(){
        try {
            cacheManagerClear(cacheManagerPointer);
        } catch (Exception e) {
            logger.error("Error clearing cache manager: {}", e.getMessage(), e);
        }
    }

    public void clearCacheForCacheType(CacheUtils.CacheType cacheType){
        try {
            cacheManagerClearByCacheType(cacheManagerPointer, cacheType.getCacheTypeName());
        } catch (Exception e) {
            logger.error("Error clearing cache manager for cache type {}: {}", cacheType.getCacheTypeName(), e.getMessage(), e);
        }
    }

    public long getMemoryConsumed(CacheUtils.CacheType cacheType){
        try {
            return cacheManagerGetMemoryConsumedForCacheType(cacheManagerPointer, cacheType.getCacheTypeName());
        } catch (Exception e) {
            logger.error("Error getting memory consumed for cache type {}: {}", cacheType.getCacheTypeName(), e.getMessage(), e);
            return 0;
        }
    }

    public long getTotalMemoryConsumed(){
        try {
            return cacheManagerGetTotalMemoryConsumed(cacheManagerPointer);
        } catch (Exception e) {
            logger.error("Error getting total memory consumed: {}", e.getMessage(), e);
            return 0;
        }
    }

    public void updateSizeLimit(CacheUtils.CacheType cacheType, long sizeLimit){
        try {
            cacheManagerUpdateSizeLimitForCacheType(cacheManagerPointer, cacheType.getCacheTypeName(), sizeLimit);
        } catch (Exception e) {
            logger.error("Error updating size limit for cache type {} to {}: {}", cacheType.getCacheTypeName(), sizeLimit, e.getMessage(), e);
        }
    }

    public boolean getEntryFromCacheType(CacheUtils.CacheType cacheType, String filePath){
        try {
            return cacheManagerGetItemByCacheType(cacheManagerPointer, cacheType.getCacheTypeName(), filePath);
        } catch (Exception e) {
            logger.error("Error getting entry from cache type {} for file {}: {}", cacheType.getCacheTypeName(), filePath, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (cacheManagerPointer != -1) {
                destroyCustomCacheManager(cacheManagerPointer);
                this.cacheManagerPointer = -1;
            }
        } catch (Exception e) {
            logger.error("Error closing cache manager: {}", e.getMessage(), e);
            throw new IOException("Failed to close cache manager", e);
        }
    }
}
