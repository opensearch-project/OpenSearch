/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.unit.TimeValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache for field-level segment statistics to avoid recalculation
 * Uses OpenSearch's Cache with TTL-based expiration
 *
 * @opensearch.internal
 */
public class FieldLevelStatsCache {
    private static final Logger logger = LogManager.getLogger(FieldLevelStatsCache.class);

    // Cache configuration
    private static final long DEFAULT_CACHE_SIZE = 100; // Max entries
    private static final TimeValue DEFAULT_CACHE_EXPIRE = TimeValue.timeValueMinutes(30);

    // Cache implementation using OpenSearch's Cache builder
    private final Cache<String, Map<String, Map<String, Long>>> cache;

    public FieldLevelStatsCache() {
        this.cache = CacheBuilder.<String, Map<String, Map<String, Long>>>builder()
            .setMaximumWeight(DEFAULT_CACHE_SIZE)
            .setExpireAfterAccess(DEFAULT_CACHE_EXPIRE)
            .build();
    }

    /**
     * Get cached stats for a segment
     * @param reader The segment reader
     * @return Cached stats or null if not found
     */
    public Map<String, Map<String, Long>> get(SegmentReader reader) {
        return cache.get(reader.getSegmentName());
    }

    /**
     * Put stats into cache
     * @param reader The segment reader
     * @param fieldStats The calculated field statistics
     */
    public void put(SegmentReader reader, Map<String, Map<String, Long>> fieldStats) {
        cache.put(reader.getSegmentName(), new ConcurrentHashMap<>(fieldStats));
    }

    /**
     * Invalidate cache entry for a segment
     * @param reader The segment reader
     */
    public void invalidate(SegmentReader reader) {
        cache.invalidate(reader.getSegmentName());
    }

    /**
     * Clear all cache entries
     */
    public void clear() {
        cache.invalidateAll();
        logger.info("Cleared field-level stats cache");
    }
}
