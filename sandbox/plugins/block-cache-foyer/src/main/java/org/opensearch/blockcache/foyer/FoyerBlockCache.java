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

    /** The configured disk capacity in bytes */
    private final long diskBytes;

    /** Guards against double-close per the {@link AutoCloseable} contract. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create the native Foyer cache and acquire its handle.
     *
     * @param diskBytes       maximum disk capacity in bytes; must be {@code > 0}
     * @param diskDir         directory where Foyer stores cache data; must not be null or blank
     * @param blockSizeBytes  Foyer disk block size in bytes; must be {@code > 0}
     * @param ioEngine        I/O engine: {@code "auto"}, {@code "io_uring"}, or {@code "psync"}
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
        this.diskBytes = diskBytes;
        this.cachePtr = FoyerBridge.createCache(diskBytes, diskDir, blockSizeBytes, ioEngine);
    }

    /**
     * Returns the opaque handle to the underlying native cache instance.
     *
     * <p>Absent from {@link BlockCache} to confine native handle access to code
     * that explicitly narrows the type to {@code FoyerBlockCache}.
     */
    public long nativeCachePtr() {
        return cachePtr;
    }

    /**
     * Snapshots cache statistics via FFM call to the native Foyer runtime.
     * Returns a flat {@link BlockCacheStats} record compatible with the SPI.
     *
     * <p>Delegates to {@link #foyerStats()} and projects via
     * {@link FoyerAggregatedStats#toSpiStats()}. Core code uses this method;
     * Foyer-aware code that needs per-tier counters (hit bytes, miss bytes,
     * eviction bytes, capacity, disk-tier breakdown) should call
     * {@link #foyerStats()} directly.
     */
    @Override
    public BlockCacheStats stats() {
        return foyerStats().toSpiStats();
    }

    /**
     * Snapshots the full two-section Foyer stats from the native runtime:
     * <ul>
     *   <li>section 0 — cross-tier overall rollup</li>
     *   <li>section 1 — disk-tier (block-level) only</li>
     * </ul>
     *
     * <p>Returns richer counters than {@link #stats()}: per-tier hit/miss/eviction
     * byte counts, configured capacity, and the disk-tier breakdown. Intended for
     * Foyer-internal logging, node-stats contributions, and any caller that
     * explicitly narrows the {@link org.opensearch.plugins.BlockCache} reference
     * to {@code FoyerBlockCache}.
     *
     * @return two-section snapshot; never {@code null}
     */
    public FoyerAggregatedStats foyerStats() {
        long[] raw = FoyerBridge.snapshotStats(cachePtr);
        return FoyerAggregatedStats.fromRaw(raw, diskBytes);
    }

    /**
     * Destroys the native cache. Idempotent — safe to call multiple times.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            FoyerBridge.destroyCache(cachePtr);
            logger.info("FoyerBlockCache closed");
        }
    }
}
