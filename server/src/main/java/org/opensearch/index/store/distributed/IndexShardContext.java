/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.shard.IndexShard;

/**
 * Provides safe access to IndexShard for primary term information with caching.
 * This class handles cases where IndexShard may be unavailable and provides
 * fallback mechanisms for primary term routing.
 *
 * @opensearch.internal
 */
public class IndexShardContext {

    private static final Logger logger = LogManager.getLogger(IndexShardContext.class);

    /** Default primary term used when IndexShard is unavailable */
    public static final long DEFAULT_PRIMARY_TERM = 0L;

    /** Cache duration for primary term values (1 second) */
    private static final long CACHE_DURATION_MS = 1000L;

    private final IndexShard indexShard;
    private volatile long cachedPrimaryTerm = -1L;
    private volatile long cacheTimestamp = 0L;

    /**
     * Creates a new IndexShardContext with the given IndexShard reference.
     *
     * @param indexShard the IndexShard instance to access, may be null
     */
    public IndexShardContext(IndexShard indexShard) {
        this.indexShard = indexShard;
        logger.debug("Created IndexShardContext with IndexShard: {}", indexShard != null ? "available" : "null");
    }

    /**
     * Gets the current primary term from the IndexShard with caching.
     * Returns a cached value if it's recent (within CACHE_DURATION_MS),
     * otherwise fetches a fresh value from the IndexShard.
     *
     * @return the current primary term, or DEFAULT_PRIMARY_TERM if unavailable
     */
    public long getPrimaryTerm() {
        long currentTime = System.currentTimeMillis();

        // Use cached value if recent and valid
        if (cachedPrimaryTerm != -1L && (currentTime - cacheTimestamp) < CACHE_DURATION_MS) {
            logger.trace("Using cached primary term: {}", cachedPrimaryTerm);
            return cachedPrimaryTerm;
        }

        if (indexShard != null) {
            try {
                long primaryTerm = indexShard.getOperationPrimaryTerm();

                // Update cache
                cachedPrimaryTerm = primaryTerm;
                cacheTimestamp = currentTime;

                logger.debug("Retrieved primary term from IndexShard: {}", primaryTerm);
                return primaryTerm;
            } catch (Exception e) {
                logger.warn("Failed to get primary term from IndexShard, using default", e);
                // Don't cache failed attempts
            }
        } else {
            logger.debug("IndexShard is null, using default primary term");
        }

        return DEFAULT_PRIMARY_TERM;
    }

    /**
     * Checks if the IndexShard is available for primary term access.
     *
     * @return true if IndexShard is available, false otherwise
     */
    public boolean isAvailable() {
        return indexShard != null;
    }

    /**
     * Invalidates the cached primary term, forcing a fresh fetch on next access.
     * This can be useful when primary term changes are detected externally.
     */
    public void invalidateCache() {
        cachedPrimaryTerm = -1L;
        cacheTimestamp = 0L;
        logger.debug("Invalidated primary term cache");
    }

    /**
     * Gets the IndexShard instance (for testing purposes).
     *
     * @return the IndexShard instance, may be null
     */
    protected IndexShard getIndexShard() {
        return indexShard;
    }

    /**
     * Gets the cached primary term value (for testing purposes).
     *
     * @return the cached primary term, or -1 if not cached
     */
    protected long getCachedPrimaryTerm() {
        return cachedPrimaryTerm;
    }

    /**
     * Checks if the cache is valid (for testing purposes).
     *
     * @return true if cache is valid and recent, false otherwise
     */
    protected boolean isCacheValid() {
        if (cachedPrimaryTerm == -1L) {
            return false;
        }

        long currentTime = System.currentTimeMillis();
        return (currentTime - cacheTimestamp) < CACHE_DURATION_MS;
    }
}