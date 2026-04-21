/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.pagecache.foyer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.pagecache.PageCache;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Foyer-backed implementation of {@link PageCache}.
 *
 * <p>Holds the native cache handle privately. Callers interact with this class
 * through the {@link PageCache} interface. Native-aware callers that need the
 * underlying handle must cast to {@code FoyerPageCache} and call
 * {@link #nativeCachePtr()}.
 *
 * @opensearch.experimental
 */
public final class FoyerPageCache implements PageCache {

    private static final Logger logger = LogManager.getLogger(FoyerPageCache.class);

    /** Opaque native handle returned by {@code foyer_create_cache}. Always positive. */
    private final long cachePtr;

    /** Guards against double-close per the {@link AutoCloseable} contract. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create the native Foyer cache and acquire its handle.
     *
     * @param diskBytes maximum disk capacity in bytes; must be {@code > 0}
     * @param diskDir   directory where Foyer stores cache data; must not be null or blank
     * @throws IllegalArgumentException if {@code diskBytes <= 0} or {@code diskDir} is blank
     * @throws NullPointerException     if {@code diskDir} is null
     * @throws IllegalStateException    if the native call fails to return a valid handle
     */
    public FoyerPageCache(long diskBytes, String diskDir) {
        if (diskBytes <= 0) {
            throw new IllegalArgumentException("diskBytes must be > 0, got: " + diskBytes);
        }
        Objects.requireNonNull(diskDir, "diskDir must not be null");
        if (diskDir.isBlank()) {
            throw new IllegalArgumentException("diskDir must not be blank");
        }
        this.cachePtr = FoyerBridge.createCache(diskBytes, diskDir);
    }

    /**
     * Returns the opaque native cache pointer.
     *
     * <p><strong>Native-aware callers only.</strong> This method lives outside the
     * {@link PageCache} interface to prevent leakage of the native handle into
     * general-purpose code. Callers must first verify the runtime type with
     * {@code instanceof FoyerPageCache} before calling this method.
     *
     * @return the positive {@code long} handle to the native cache instance
     */
    public long nativeCachePtr() {
        return cachePtr;
    }

    /**
     * Destroys the native cache. Idempotent — safe to call multiple times.
     *
     * <p>Only the first invocation actually destroys the cache; subsequent calls
     * are no-ops. This satisfies the {@link AutoCloseable} contract.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            FoyerBridge.destroyCache(cachePtr);
            logger.info("FoyerPageCache closed");
        }
    }
}
