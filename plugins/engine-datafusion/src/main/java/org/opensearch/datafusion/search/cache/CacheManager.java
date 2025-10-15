/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.common.settings.ClusterSettings;

import static org.opensearch.datafusion.DataFusionQueryJNI.initCacheManagerConfig;


public class CacheManager {

    private Map<CacheType, CacheAccessor> caches;
    private long cacheManagerPtr;
    private long totalSizeLimit;

    public long getCacheManagerPtr() {
        return this.cacheManagerPtr;
    }

    public long getTotalSizeLimit() {
        return totalSizeLimit;
    }

    public CacheManager(long cacheManagerPtr, Map<CacheType, CacheAccessor> cacheMap) {
        this.caches = new HashMap<>(cacheMap);
        this.cacheManagerPtr = cacheManagerPtr;
        this.totalSizeLimit = caches.values().stream()
                .mapToLong(CacheAccessor::getConfiguredSizeLimit)
                .sum();
    }

    // Factory method to create CacheManager from config
    public static CacheManager fromConfig(ClusterSettings clusterSettings) {
        long cacheManagerPtr = initCacheManagerConfig();
        Map<CacheType, CacheAccessor> cacheMap = new HashMap<>();
        for (CacheType type : CacheType.values()) {
            if (type.isEnabled(clusterSettings)) {
                cacheMap.put(type, type.createCache(cacheManagerPtr, clusterSettings));
            }
        }
        return new CacheManager(cacheManagerPtr, cacheMap);
    }

    public CacheAccessor getCacheAccessor(CacheType cacheType) {
        return caches.get(cacheType);
    }

    public List<CacheAccessor> getAllCaches() {
        return new ArrayList<>(caches.values());
    }

    public List<CacheAccessor> getCachesByType(CacheType... types) {
        return caches.entrySet().stream()
                .filter(entry -> List.of(types).contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    public boolean removeFiles(List<String> files) {
        boolean allSuccessful = true;
        for (CacheAccessor cache : getAllCaches()) {
            for (String filename : files) {
                allSuccessful &= cache.remove(filename);
            }
        }


        return allSuccessful;
    }

    public boolean removeFilesByDirectory(String path) {
        boolean allSuccessful = true;
        for (CacheAccessor cache : getAllCaches()) {
            allSuccessful &= cache.remove(path);
        }
        return allSuccessful;
    }

    public boolean addToCache(List<String> files) {
        boolean allSuccessful = true;
        for (CacheAccessor cache : getAllCaches()) {
            for (String filename : files) {
                allSuccessful &= cache.put(filename);
            }
        }
        return allSuccessful;
    }

    public void resetCache() {
        getAllCaches().forEach(CacheAccessor::clear);
    }

    public long getTotalUsedBytes() {
        return getAllCaches().stream().mapToLong(CacheAccessor::getMemoryConsumed).sum();
    }

    public boolean withinCacheLimit(CacheType cacheType) {
        CacheAccessor cacheAccessor = getCacheAccessor(cacheType);
        return cacheAccessor.getMemoryConsumed() < cacheAccessor.getConfiguredSizeLimit();
    }

    public boolean withinTotalLimit() {
        return getTotalUsedBytes() < totalSizeLimit;
    }

}
