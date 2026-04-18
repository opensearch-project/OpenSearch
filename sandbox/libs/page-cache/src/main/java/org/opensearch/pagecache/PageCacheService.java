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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * <p>Thread-safe. {@code cachePtr} is {@code final} and written exactly once in
 * the constructor; per JLS §17.5, {@code final} fields have safe-publication
 * semantics — all threads that obtain a reference to this object after construction
 * are guaranteed to see the correct value without {@code volatile} or additional
 * synchronisation. {@link #close()} is guarded by an {@link java.util.concurrent.atomic.AtomicBoolean}
 * to make it idempotent and safe for concurrent or repeated invocations.
 *
 * @opensearch.experimental
 */
public final class PageCacheService implements Closeable {

    private static final Logger logger = LogManager.getLogger(PageCacheService.class);

    private final long cachePtr;
    /** Guards against double-close. {@link Closeable} contract requires idempotent close(). */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private PageCacheService(long cachePtr) {
        this.cachePtr = cachePtr;
    }

    /**
     * Creates a {@code PageCacheService} with the given disk quota and directory.
     *
     * @param diskBytes maximum disk space the cache may use, in bytes; must be {@code > 0}
     * @param diskDir   directory in which Foyer stores cache data; must not be null or blank
     * @return a new service instance; {@link #close()} must be called at shutdown
     * @throws IllegalArgumentException if {@code diskBytes <= 0} or {@code diskDir} is null/blank
     */
    public static PageCacheService create(long diskBytes, String diskDir) {
        if (diskBytes <= 0) {
            throw new IllegalArgumentException("diskBytes must be > 0, got: " + diskBytes);
        }
        Objects.requireNonNull(diskDir, "diskDir must not be null");
        if (diskDir.isBlank()) {
            throw new IllegalArgumentException("diskDir must not be blank");
        }
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
     *
     * <p>Idempotent: safe to call multiple times. Only the first call destroys
     * the native cache; subsequent calls are no-ops. This satisfies the
     * {@link Closeable} contract and prevents use-after-free in the native layer.
     */
    @Override
    public void close() {
        if (cachePtr != 0 && closed.compareAndSet(false, true)) {
            FoyerBridge.destroyCache(cachePtr);
            logger.info("Page cache destroyed: ptr={}", cachePtr);
        }
    }
}
