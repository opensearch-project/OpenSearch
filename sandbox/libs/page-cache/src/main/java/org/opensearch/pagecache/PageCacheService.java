/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.pagecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.pagecache.foyer.FoyerBridge;

import java.io.Closeable;

/**
 * Node-level disk page cache service.
 *
 * <p>Created once by {@code Node} before any plugins start, and destroyed after
 * all plugins stop. Any plugin may read {@link #getCachePtr()} to obtain the
 * opaque native cache handle and pass it to its own native layer.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@code Node} calls {@link #init} during startup — creates the native cache.
 *   <li>Plugins call {@link #getCachePtr()} — returns 0 if disabled.
 *   <li>{@code Node} adds the instance to {@code toClose} <em>after</em> plugin
 *       lifecycle components — native cache is destroyed last.
 * </ol>
 *
 * @opensearch.experimental
 */
public final class PageCacheService implements Closeable {

    private static final Logger logger = LogManager.getLogger(PageCacheService.class);

    private final long cachePtr;

    private PageCacheService(long cachePtr) {
        this.cachePtr = cachePtr;
    }

    /**
     * Creates a {@code PageCacheService} with the given disk quota and directory.
     *
     * @param diskBytes maximum disk space the cache may use, in bytes
     * @param diskDir   directory in which Foyer stores cache data
     * @return a new service instance; {@link #close()} must be called at shutdown
     */
    public static PageCacheService create(long diskBytes, String diskDir) {
        long ptr = FoyerBridge.createCache(diskBytes, diskDir);
        logger.info("Page cache created: ptr={}, disk={}B, dir={}", ptr, diskBytes, diskDir);
        return new PageCacheService(ptr);
    }

    /**
     * Returns the opaque native cache pointer.
     *
     * <p>Plugins pass this value to their native layer to enable caching.
     * Returns 0 if the cache was not created (should not happen in normal operation).
     */
    public long getCachePtr() {
        return cachePtr;
    }

    /**
     * Destroys the native cache. Called by {@code Node.close()} after all plugins
     * have been stopped.
     */
    @Override
    public void close() {
        if (cachePtr != 0) {
            FoyerBridge.destroyCache(cachePtr);
            logger.info("Page cache destroyed: ptr={}", cachePtr);
        }
    }
}
