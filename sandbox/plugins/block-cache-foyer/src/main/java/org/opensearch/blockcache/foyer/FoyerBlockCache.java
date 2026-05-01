/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheStats;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Foyer-backed implementation of {@link BlockCache}.
 *
 * <p>Holds the native cache handle privately. Callers interact with this
 * class through the {@link BlockCache} interface. Native-aware callers that
 * need the underlying handle must cast to {@code FoyerBlockCache} and call
 * {@link #nativeCachePtr()}. Core code never performs that cast.
 *
 * @opensearch.experimental
 */
public final class FoyerBlockCache implements BlockCache {

    private static final Logger logger = LogManager.getLogger(FoyerBlockCache.class);

    /** Opaque native handle returned by {@code foyer_create_cache}. Always positive. */
    private final long cachePtr;

    /** Guards against double-close per the {@link AutoCloseable} contract. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create the native Foyer cache and acquire its handle.
     *
     * @param diskBytes      maximum disk capacity in bytes; must be {@code > 0}
     * @param diskDir        directory where Foyer stores cache data; must not be null or blank
     * @param blockSizeBytes Foyer disk block size in bytes; must be {@code > 0}.
     *                       Typically read from {@code format_cache.block_size} (default 64 MB).
     * @param ioEngine       I/O engine selection: {@code "auto"}, {@code "io_uring"}, or
     *                       {@code "psync"}. Typically read from {@code format_cache.io_engine}.
     * @throws IllegalArgumentException if {@code diskBytes <= 0}, {@code blockSizeBytes <= 0},
     *                                  or {@code diskDir} is blank
     * @throws NullPointerException     if {@code diskDir} or {@code ioEngine} is null
     * @throws IllegalStateException    if the native call fails to return a valid handle
     */
    public FoyerBlockCache(long diskBytes, String diskDir, long blockSizeBytes, String ioEngine) {
        if (diskBytes <= 0) {
            throw new IllegalArgumentException("diskBytes must be > 0, got: " + diskBytes);
        }
        Objects.requireNonNull(diskDir, "diskDir must not be null");
        if (diskDir.isBlank()) {
            throw new IllegalArgumentException("diskDir must not be blank");
        }
        if (blockSizeBytes <= 0) {
            throw new IllegalArgumentException("blockSizeBytes must be > 0, got: " + blockSizeBytes);
        }
        Objects.requireNonNull(ioEngine, "ioEngine must not be null");
        this.cachePtr = FoyerBridge.createCache(diskBytes, diskDir, blockSizeBytes, ioEngine);
    }

    /**
     * Returns the opaque native cache pointer.
     *
     * <p><strong>Native-aware callers only.</strong> This method lives outside
     * the {@link BlockCache} interface to prevent leakage of the native handle
     * into general-purpose code. Callers must first verify the runtime type
     * with {@code instanceof FoyerBlockCache} before calling this method.
     *
     * @return the positive {@code long} handle to the native cache instance
     */
    public long nativeCachePtr() {
        return cachePtr;
    }

    /**
     * Returns a point-in-time snapshot of cache counters.
     *
     * <p>Foyer exposes its counters through the native library; bridging them
     * into this record is a follow-up. Until then, this method returns a
     * zero-valued snapshot so that callers that poll stats for logging or
     * node-stats reporting continue to function without special-casing.
     *
     * @return zero-valued snapshot; never {@code null}
     */
    @Override
    public BlockCacheStats stats() {
        // TODO: bridge real Foyer counters through FFM once the Rust-side accessor exists.
        return new BlockCacheStats(0L, 0L, 0L, 0L, 0L);
    }

    /**
     * Destroys the native cache. Idempotent — safe to call multiple times.
     *
     * <p>Only the first invocation actually destroys the cache; subsequent
     * calls are no-ops. This satisfies the {@link BlockCache#close()} contract.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            FoyerBridge.destroyCache(cachePtr);
            logger.info("FoyerBlockCache closed");
        }
    }
}
