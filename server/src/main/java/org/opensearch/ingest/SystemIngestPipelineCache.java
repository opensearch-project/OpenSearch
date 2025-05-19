/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import reactor.util.annotation.NonNull;

/**
 * Cache for system ingest pipeline
 */
public class SystemIngestPipelineCache {
    private final Map<String, CacheEntry> cache;

    private final int MAX_ENTRIES = 100;
    private final long EXPIRES_IN_MINUTES = 60;

    public SystemIngestPipelineCache() {
        this.cache = new ConcurrentHashMap<>();
    }

    /**
     * Cache an system ingest pipeline for an index.
     *
     * @param index                    [index_name/index_uuid]
     * @param systemIngestPipeline A pipeline created based on index configuration.
     * @param maxIngestProcessorCount
     */
    public void cachePipeline(
        @NonNull final String index,
        @NonNull final Pipeline systemIngestPipeline,
        final int maxIngestProcessorCount
    ) {
        if (systemIngestPipeline.getProcessors().size() > maxIngestProcessorCount) {
            throw new IllegalArgumentException("Too many system ingest processors for index: " + index);
        }
        if (cache.size() >= MAX_ENTRIES) {
            evictOldestAndExpiredCacheEntry();
        }
        cache.put(index, new CacheEntry(systemIngestPipeline));
    }

    // Evict the oldest and expired cache entry based on time
    private void evictOldestAndExpiredCacheEntry() {
        String oldestIndex = null;
        long oldestTimestamp = Long.MAX_VALUE;
        final List<String> expiredIndices = new ArrayList<>();

        for (Map.Entry<String, CacheEntry> entry : cache.entrySet()) {
            final CacheEntry cacheEntry = entry.getValue();
            if (cacheEntry.getLastAccessTimestamp() < oldestTimestamp) {
                oldestTimestamp = cacheEntry.getLastAccessTimestamp();
                oldestIndex = entry.getKey();
            }
            if (cacheEntry.isExpired()) {
                expiredIndices.add(entry.getKey());
            }
        }

        if (oldestIndex != null) {
            // Remove the oldest entry from both the cache and access order
            cache.remove(oldestIndex);
        }

        for (final String expiredIndex : expiredIndices) {
            cache.remove(expiredIndex);
        }
    }

    /**
     * Get the cached system ingest pipeline for an index.
     * @param index [index_name/index_uuid]
     * @return cached system ingest pipeline
     */
    public Pipeline getSystemIngestPipeline(@NonNull final String index) {
        // Check if the cache contains a valid entry for the index
        final CacheEntry entry = cache.get(index);
        if (entry != null) {
            if (entry.isExpired()) {
                cache.remove(index);
                return null;
            } else {
                entry.setLastAccessTimestamp(System.currentTimeMillis());
                return entry.getSystemIngestPipeline();
            }
        }
        return null;
    }

    public int size() {
        return cache.size();
    }

    /**
     * Invalidate the cache for an index.
     * @param index [index_name/index_uuid]
     */
    public void invalidateCacheForIndex(@NonNull final String index) {
        cache.remove(index);
    }

    private class CacheEntry {
        private final Pipeline systemIngestPipeline;
        private final long createTimestamp;
        private long lastAccessTimestamp;

        public CacheEntry(Pipeline systemIngestPipeline) {
            this.systemIngestPipeline = systemIngestPipeline;
            this.createTimestamp = System.currentTimeMillis();
            this.lastAccessTimestamp = createTimestamp;
        }

        public Pipeline getSystemIngestPipeline() {
            return systemIngestPipeline;
        }

        public boolean isExpired() {
            return System.currentTimeMillis() - createTimestamp > TimeUnit.MINUTES.toMillis(EXPIRES_IN_MINUTES);
        }

        public void setLastAccessTimestamp(final long lastAccessTimestamp) {
            this.lastAccessTimestamp = lastAccessTimestamp;
        }

        public long getLastAccessTimestamp() {
            return this.lastAccessTimestamp;
        }
    }
}
