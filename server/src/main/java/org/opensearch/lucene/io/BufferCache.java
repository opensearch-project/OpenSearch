/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class BufferCache {
    private static final Logger logger = LogManager.getLogger(BufferCache.class);

    private final ConcurrentHashMap<String, Page> cache;
    private final ScheduledExecutorService cleanupExecutor;
    private final BufferCacheCleaner cleaner;
    private volatile boolean closed = false;
    private final ScheduledExecutorService cacheStatsLogger;
    private ScheduledFuture<?> scheduledFuture;
    private ScheduledFuture<?> cacheStatsFuture;

    public BufferCache() {

        this.cache = new ConcurrentHashMap<>();
        this.cleaner = new BufferCacheCleaner(cache);

        // Create daemon thread for cleanup
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PageCache-Cleanup");
            t.setDaemon(true);
            return t;
        });

        this.cacheStatsLogger = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PageCache-Stats");
            t.setDaemon(true);
            return t;
        });

        this.scheduledFuture = cleanupExecutor.scheduleAtFixedRate(cleaner, 300000, 3000000, TimeUnit.SECONDS);
        logger.info("PageCache initialized with periodic cleanup every 30 seconds");
        this.cacheStatsFuture = this.cacheStatsLogger.scheduleAtFixedRate(() -> {
            CacheStats stats = getStats();
            logger.info("PageCache stats {}", stats);
        }, 30, 30, TimeUnit.SECONDS);


    }

    public Page getPage(String key, BiFunction<String, Page, Page> mappingFunction) {
        Page originalPage = getPageForReadAhead(key, mappingFunction);
        if (originalPage == null) {
            throw new IllegalStateException("Mapping function returned null for key: " + key);
        }

        // Return a clone to the caller (with automatic cleanup via Cleaner)
        return originalPage.clone();
    }

    //this will not clone as reader doesnt need it right now
    public Page getPageForReadAhead(String key, BiFunction<String, Page, Page> mappingFunction) {
        if (closed) {
            throw new IllegalStateException("PageCache is closed");
        }

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (mappingFunction == null) {
            throw new IllegalArgumentException("Mapping function cannot be null");
        }

        try {
            // Get or create the original page in cache
            Page originalPage = cache.compute(key, mappingFunction);

            if (originalPage == null) {
                throw new IllegalStateException("Mapping function returned null for key: " + key);
            }
            // Return original page to the caller (as this is probably for readaheads and no one needs it right now)
            return originalPage;

        } catch (Exception e) {
            logger.error("Failed to get/create page for key: {}", key, e);
            throw new RuntimeException("Page retrieval failed", e);
        }
    }

    /**
     * Check if a page with the given key exists in the cache
     */
    public boolean containsKey(String key) {
        if (closed) {
            return false;
        }
        return cache.containsKey(key);
    }

    /**
     * Get cache statistics
     */
    public CacheStats getStats() {
        int totalPages = cache.size();
        int totalRefCount = cache.values().stream()
            .mapToInt(Page::getRefCount)
            .sum();

        return new CacheStats(totalPages, totalRefCount);
    }

    /**
     * Manually trigger cleanup (for testing)
     */
    public void runCleanup() {
        if (!closed) {
            cleaner.run();
        }
    }

    /**
     * Close the cache and cleanup resources
     */
    public void close() {
        if (closed) {
            return;
        }
        //run manual cleanup once
        runCleanup();

        closed = true;
        logger.info("Closing PageCache...");

        // Cancel scheduled tasks first
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        if (cacheStatsFuture != null) {
            cacheStatsFuture.cancel(true);
        }

        // Shutdown executors
        shutdownExecutor(cleanupExecutor, "PageCache-Cleanup", 120);
        shutdownExecutor(cacheStatsLogger, "PageCache-Stats", 120);

        // Final cleanup of all remaining pages
        int remainingPages = cache.size();
        cache.values().forEach(page -> {
            try {
                if (page.getRefCount() > 0) {
                    page.decRef();
                }
            } catch (Exception e) {
                logger.warn("Error releasing page during shutdown", e);
            }
        });
        logger.info("PageCache closed remainging pages {} ", cache.size());
        // Clear the cache after releasing pages
        cache.clear();

        logger.info("PageCache closed. Released {} remaining pages", remainingPages);
    }

    private void shutdownExecutor(ScheduledExecutorService executor, String name, int timeoutSeconds) {
        if (executor == null) return;

        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                logger.warn("{} did not terminate gracefully, forcing shutdown", name);
                executor.shutdownNow();

                // Wait a bit more for forced shutdown
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    logger.error("{} did not terminate after forced shutdown", name);
                }
            } else {
                logger.debug("{} terminated gracefully", name);
            }
        } catch (InterruptedException e) {
            logger.warn("{} shutdown interrupted, forcing shutdown", name);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public Page get(String cacheKey) {
        return cache.get(cacheKey);
    }


    /**
     * Cache statistics holder
     */
    public static class CacheStats {
        public final int totalPages;
        public final int totalRefCount;

        public CacheStats(int totalPages, int totalRefCount) {
            this.totalPages = totalPages;
            this.totalRefCount = totalRefCount;
        }

        @Override
        public String toString() {
            return String.format("CacheStats{pages=%d, totalRefCount=%d}", totalPages, totalRefCount);
        }
    }
}
