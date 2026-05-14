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
import org.opensearch.plugins.BuiltInBlockCaches;
import org.opensearch.plugins.NativeCacheHandle;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Foyer-backed implementation of {@link BlockCache}.
 *
 * @opensearch.experimental
 */
public final class FoyerBlockCache implements BlockCache {

    private static final Logger logger = LogManager.getLogger(FoyerBlockCache.class);

    /** Opaque {@code Box<Arc<dyn BlockCache>>} fat pointer returned by {@code foyer_create_cache}. Always positive. */
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

    @Override
    public String cacheName() {
        return BuiltInBlockCaches.FOYER;
    }

    /**
     * Snapshots cache statistics via FFM call to the native Foyer runtime.
     * Returns the cross-tier rollup {@link BlockCacheStats} (section 0 of the
     * native stats buffer) for core consumption.
     *
     * <p>Delegates to {@link #foyerStats()} and returns the overall section
     * directly — no projection step needed. Core code uses this method;
     * Foyer-aware code that needs the disk-tier breakdown (section 1) should
     * call {@link #foyerStats()} directly.
     */
    @Override
    public BlockCacheStats stats() {
        return foyerStats().overallStats();
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
        return FoyerAggregatedStats.snapshot(raw, diskBytes);
    }

    /**
     * Returns a borrowed, non-owning {@link NativeCacheHandle} for this Foyer cache.
     *
     * <p>The handle wraps the raw fat pointer created by {@code foyer_create_cache}.
     * It is valid for the entire lifetime of this {@code FoyerBlockCache} instance.
     * {@code FoyerBlockCache} owns the cache and destroys it via
     * {@code foyer_destroy_cache} in {@link #close()} — callers must never free the pointer.
     *
     * @return a borrowed handle; never {@code null}, always {@link NativeCacheHandle#isLive()}
     */
    @Override
    public NativeCacheHandle nativeCacheHandle() {
        return NativeCacheHandle.of(cachePtr);
    }

    /**
     * Destroys the native cache. Idempotent — safe to call multiple times.
     */
    @Override
    public void evictPrefix(String prefix) {
        if (closed.get() == false) {
            FoyerBridge.evictPrefix(cachePtr, prefix);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            FoyerBridge.destroyCache(cachePtr);
            logger.info("FoyerBlockCache closed");
        }
    }
}
