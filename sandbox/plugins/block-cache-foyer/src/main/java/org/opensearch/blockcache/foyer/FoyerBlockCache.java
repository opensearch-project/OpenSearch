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
import org.opensearch.plugins.BlockCacheConstants;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.plugins.BlockCacheTieredStats;
import org.opensearch.plugins.NativeStoreHandle;

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

    /** Data cache capacity in tiered mode. 0 in single-cache mode. */
    private final long dataDiskBytes;

    /** Metadata cache capacity in tiered mode. 0 in single-cache mode. */
    private final long metadataDiskBytes;

    /** Whether this instance is a tiered cache (data + metadata on separate SSDs). */
    private final boolean tiered;

    /** Guards against double-close per the {@link AutoCloseable} contract. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create the native Foyer cache and acquire its handle.
     *
     * @param diskBytes              maximum disk capacity in bytes; must be {@code > 0}
     * @param diskDir                directory where Foyer stores cache data; must not be null or blank
     * @param blockSizeBytes         Foyer disk block size in bytes; must be {@code > 0}
     * @param ioEngine               I/O engine: {@code "auto"}, {@code "io_uring"}, or {@code "psync"}
     * @param sweepIntervalSecs      background key_index sweep interval in seconds;
     *                               {@code 0} = disabled (no background sweep task is spawned).
     *                               Maps to {@code block_cache.foyer.key_index_sweep_interval_seconds}.
     * @param sweepThresholdRatio    minimum {@code used_bytes / disk_bytes} ratio to run the sweep;
     *                               {@code 0.0} = disabled (always sweep).
     *                               Maps to {@code block_cache.foyer.key_index_sweep_threshold}.
     * @param persistIntervalSecs    how often (seconds) the independent persist task flushes the
     *                               key_index to disk; {@code 0} = disabled (only {@code Drop} persists).
     *                               Maps to {@code block_cache.foyer.key_index_persist_interval_seconds}.
     * @throws IllegalArgumentException if {@code diskBytes <= 0}, {@code blockSizeBytes <= 0},
     *                                  {@code sweepIntervalSecs < 0}, {@code persistIntervalSecs < 0},
     *                                  {@code sweepThresholdRatio} outside {@code [0.0, 1.0]},
     *                                  or {@code diskDir} is blank
     * @throws NullPointerException     if {@code diskDir} or {@code ioEngine} is null
     * @throws IllegalStateException    if the native call fails to return a valid handle
     */
    public FoyerBlockCache(
        long diskBytes,
        String diskDir,
        long blockSizeBytes,
        long bufferPoolSizeBytes,
        long submitQueueSizeThresholdBytes,
        String ioEngine,
        long sweepIntervalSecs,
        double sweepThresholdRatio,
        long persistIntervalSecs
    ) {
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
        if (bufferPoolSizeBytes <= 0) {
            throw new IllegalArgumentException("bufferPoolSizeBytes must be > 0, got: " + bufferPoolSizeBytes);
        }
        if (submitQueueSizeThresholdBytes <= 0) {
            throw new IllegalArgumentException("submitQueueSizeThresholdBytes must be > 0, got: " + submitQueueSizeThresholdBytes);
        }
        Objects.requireNonNull(ioEngine, "ioEngine must not be null");
        if (sweepIntervalSecs < 0) {
            throw new IllegalArgumentException("sweepIntervalSecs must be >= 0, got: " + sweepIntervalSecs);
        }
        if (sweepThresholdRatio < 0.0 || sweepThresholdRatio > 1.0) {
            throw new IllegalArgumentException("sweepThresholdRatio must be in [0.0, 1.0], got: " + sweepThresholdRatio);
        }
        if (persistIntervalSecs < 0) {
            throw new IllegalArgumentException("persistIntervalSecs must be >= 0, got: " + persistIntervalSecs);
        }
        this.diskBytes = diskBytes;
        this.dataDiskBytes = 0L;
        this.metadataDiskBytes = 0L;
        this.tiered = false;
        this.cachePtr = FoyerBridge.createCache(
            diskBytes,
            diskDir,
            blockSizeBytes,
            bufferPoolSizeBytes,
            submitQueueSizeThresholdBytes,
            ioEngine,
            sweepIntervalSecs,
            sweepThresholdRatio,
            persistIntervalSecs
        );
    }

    /**
     * Create a tiered Foyer cache with separate data and metadata SSDs.
     *
     * @param dataDiskBytes             data cache disk capacity
     * @param dataDiskDir               directory for data cache
     * @param dataBlockSizeBytes        data cache block size
     * @param dataBufferPoolSizeBytes   data cache buffer pool
     * @param dataSubmitQueueSizeBytes  data cache submit queue threshold
     * @param metaDiskBytes             metadata cache disk capacity
     * @param metaDiskDir               directory for metadata cache
     * @param metaBlockSizeBytes        metadata cache block size
     * @param metaBufferPoolSizeBytes   metadata cache buffer pool
     * @param metaSubmitQueueSizeBytes  metadata cache submit queue threshold
     * @param ioEngine                  I/O engine for both caches
     * @param sweepIntervalSecs         sweep interval; 0 = disabled
     * @param sweepThresholdRatio       sweep threshold; 0.0 = always
     * @param persistIntervalSecs       persist interval; 0 = disabled
     */
    public FoyerBlockCache(
        long dataDiskBytes,
        String dataDiskDir,
        long dataBlockSizeBytes,
        long dataBufferPoolSizeBytes,
        long dataSubmitQueueSizeBytes,
        long metaDiskBytes,
        String metaDiskDir,
        long metaBlockSizeBytes,
        long metaBufferPoolSizeBytes,
        long metaSubmitQueueSizeBytes,
        String ioEngine,
        long sweepIntervalSecs,
        double sweepThresholdRatio,
        long persistIntervalSecs
    ) {
        if (dataDiskBytes <= 0) {
            throw new IllegalArgumentException("dataDiskBytes must be > 0, got: " + dataDiskBytes);
        }
        if (metaDiskBytes <= 0) {
            throw new IllegalArgumentException("metaDiskBytes must be > 0, got: " + metaDiskBytes);
        }
        Objects.requireNonNull(dataDiskDir, "dataDiskDir must not be null");
        Objects.requireNonNull(metaDiskDir, "metaDiskDir must not be null");
        Objects.requireNonNull(ioEngine, "ioEngine must not be null");
        this.diskBytes = dataDiskBytes + metaDiskBytes;
        this.dataDiskBytes = dataDiskBytes;
        this.metadataDiskBytes = metaDiskBytes;
        this.tiered = true;
        this.cachePtr = FoyerBridge.createTieredCache(
            dataDiskBytes,
            dataDiskDir,
            dataBlockSizeBytes,
            dataBufferPoolSizeBytes,
            dataSubmitQueueSizeBytes,
            metaDiskBytes,
            metaDiskDir,
            metaBlockSizeBytes,
            metaBufferPoolSizeBytes,
            metaSubmitQueueSizeBytes,
            ioEngine,
            sweepIntervalSecs,
            sweepThresholdRatio,
            persistIntervalSecs
        );
    }

    @Override
    public String cacheName() {
        return BlockCacheConstants.FOYER;
    }

    /**
     * Snapshots cache statistics via FFM call to the native Foyer runtime.
     * Returns the cross-tier rollup {@link BlockCacheStats} for core consumption.
     * In tiered mode, this is the merged (data + metadata) view.
     */
    @Override
    public BlockCacheStats stats() {
        return foyerStats().overallStats();
    }

    /**
     * Snapshots the full Foyer stats from the native runtime.
     *
     * <p>In single-cache mode: section 0 = overall, section 1 = block-level (identical).
     * In tiered mode: section 0 = data cache, section 1 = metadata cache,
     * {@code overallStats()} returns merged counters.
     *
     * @return stats snapshot; never {@code null}
     */
    public FoyerAggregatedStats foyerStats() {
        long[] raw = FoyerBridge.snapshotStats(cachePtr);
        if (tiered) {
            return FoyerAggregatedStats.snapshotTiered(raw, dataDiskBytes, metadataDiskBytes);
        }
        return FoyerAggregatedStats.snapshot(raw, diskBytes);
    }

    /** Whether this is a tiered cache (data + metadata on separate SSDs). */
    public boolean isTiered() {
        return tiered;
    }

    @Override
    public BlockCacheTieredStats tieredStats() {
        if (tiered == false) {
            return null;
        }
        FoyerAggregatedStats s = foyerStats();
        BlockCacheStats data = s.dataCacheStats();
        BlockCacheStats meta = s.metadataCacheStats();
        return new BlockCacheTieredStats(
            data.hits(),
            data.misses(),
            data.hitBytes(),
            data.missBytes(),
            data.evictions(),
            data.evictionBytes(),
            data.diskBytesUsed(),
            data.totalBytes(),
            data.activeInBytes(),
            meta.hits(),
            meta.misses(),
            meta.hitBytes(),
            meta.missBytes(),
            meta.evictions(),
            meta.evictionBytes(),
            meta.diskBytesUsed(),
            meta.totalBytes(),
            meta.activeInBytes()
        );
    }

    /**
     * Returns a borrowed, non-owning {@link NativeStoreHandle} for this Foyer cache.
     *
     * <p>The handle uses a no-op destructor — calling {@link NativeStoreHandle#close()} is safe
     * but does nothing. The raw fat pointer created by {@code foyer_create_cache} is valid for
     * the entire lifetime of this {@code FoyerBlockCache} instance. {@code FoyerBlockCache}
     * owns the cache and destroys it via {@code foyer_destroy_cache} in {@link #close()} —
     * callers must never free the pointer directly.
     *
     * @return a borrowed handle; never {@code null}, always {@link NativeStoreHandle#isLive()}
     */
    @Override
    public NativeStoreHandle nativeCacheHandle() {
        // No-op destructor: this is a borrowed reference — FoyerBlockCache owns the lifecycle.
        return new NativeStoreHandle(cachePtr, ptr -> {});
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
    public boolean clear() {
        if (closed.get() == false && cachePtr > 0) {
            return FoyerBridge.clearCache(cachePtr);
        }
        return false;
    }

    public void updateSweepThreshold(double newRatio) {
        if (closed.get() == false) {
            FoyerBridge.updateSweepThreshold(cachePtr, newRatio);
        }
    }

    public void updateSweepInterval(long newSecs) {
        if (closed.get() == false) {
            FoyerBridge.updateSweepInterval(cachePtr, newSecs);
        }
    }

    public void updatePersistInterval(long newSecs) {
        if (closed.get() == false) {
            FoyerBridge.updatePersistInterval(cachePtr, newSecs);
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
