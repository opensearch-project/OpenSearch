/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.fs.FsProbe;
import org.opensearch.node.Node;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Single authority for all warm-node SSD caches on this node.
 *
 * <p>Owns the lifecycle, disk budget, validation, and stats aggregation for:
 * <ul>
 *   <li>{@link FileCache} — Lucene block-file cache. Always present on warm nodes.</li>
 *   <li>{@link BlockCache} list (optional) — block caches for variable-size byte ranges,
 *       registered after node startup by block-cache plugins via {@link #addBlockCache}.</li>
 * </ul>
 *
 * <p>Only instantiated on warm nodes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class UnifiedCacheService implements Closeable {

    private static final Logger logger = LogManager.getLogger(UnifiedCacheService.class);

    /** LRU cache for Lucene index files. Always present on warm nodes. */
    private final FileCache fileCache;

    /**
     * Block caches registered by plugins after node startup via {@link #addBlockCache}.
     * Guarded by {@code this}.
     */
    private final List<BlockCache> blockCaches = new ArrayList<>();

    // Private constructor — use create().
    private UnifiedCacheService(FileCache fileCache) {
        this.fileCache = fileCache;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Static factory
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates a {@code UnifiedCacheService} for a warm node.
     *
     * <p>Creates and validates the SSD budget, creates FileCache, and restores
     * surviving files from disk. Block caches are registered later via
     * {@link #addBlockCache} during plugin initialisation.
     *
     * <p>Must only be called when {@code DiscoveryNode.isWarmNode(settings) == true}.
     *
     * @param blockCacheBytes the total SSD bytes requested by all registered
     *        {@link org.opensearch.plugins.BlockCacheProvider} plugins, computed by
     *        Node.java before plugins' {@code createComponents()} are called
     */
    public static UnifiedCacheService create(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        long blockCacheBytes
    ) throws IOException {
        // Step 1: Read total SSD capacity from the fileCacheNodePath.
        NodeEnvironment.NodePath fileCacheNodePath = nodeEnvironment.fileCacheNodePath();
        long totalSSDBytes = ExceptionsHelper.catchAsRuntimeException(() -> FsProbe.getTotalSize(fileCacheNodePath));

        // Step 2: Compute the total warm-cache SSD budget from node.search.cache.size.
        String warmCacheSizeRaw = Node.NODE_SEARCH_CACHE_SIZE_SETTING.get(settings);
        long totalBudgetBytes = calculateFileCacheSize(warmCacheSizeRaw, totalSSDBytes);

        // Step 3: Partition the budget — plugins told Node.java how much they need.
        long fileCacheBytes = totalBudgetBytes - blockCacheBytes;

        // Step 4: Validate before creating anything.
        validate(fileCacheBytes, blockCacheBytes, totalSSDBytes);

        // Step 5: Create FileCache.
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(fileCacheBytes);
        fileCacheNodePath.fileCacheReservedSize = new ByteSizeValue(fileCacheBytes, ByteSizeUnit.BYTES);
        restoreFileCacheFromDisk(settings, fileCacheNodePath, fileCache);

        // Block caches are provided by plugins via addBlockCache() after createComponents().
        return new UnifiedCacheService(fileCache);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Validation
    // ─────────────────────────────────────────────────────────────────────────

    static void validate(long fileCacheBytes, long blockCacheBytes, long totalSSDBytes) {
        if (totalSSDBytes <= 0) {
            throw new IllegalArgumentException(
                "Unable to determine SSD capacity; got: " + totalSSDBytes
                    + ". Ensure the filecache path is mounted and readable."
            );
        }
        if (blockCacheBytes < 0) {
            throw new IllegalArgumentException("blockCacheBytes must be >= 0; got: " + blockCacheBytes);
        }
        if (fileCacheBytes + blockCacheBytes > totalSSDBytes) {
            throw new IllegalArgumentException(
                "Warm node SSD allocation exceeds available capacity. "
                    + "file_cache=" + new ByteSizeValue(fileCacheBytes)
                    + ", block_cache=" + new ByteSizeValue(blockCacheBytes)
                    + ", total SSD=" + new ByteSizeValue(totalSSDBytes)
                    + ". Reduce block_cache.size or node.search.cache.size."
            );
        }
        if (fileCacheBytes <= 0) {
            throw new IllegalArgumentException(
                "After allocating " + new ByteSizeValue(blockCacheBytes)
                    + " to block_cache, no SSD remains for FileCache. Reduce block_cache.size."
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    /** Returns the {@link FileCache} instance. */
    public FileCache fileCache() {
        return fileCache;
    }

    /**
     * Registers a block cache. Called by block-cache plugins after
     * {@code createComponents()} constructs their {@link BlockCache} instance.
     */
    public synchronized void addBlockCache(BlockCache blockCache) {
        if (blockCache != null) {
            blockCaches.add(blockCache);
            logger.info("Block cache registered (disk bytes used so far: {})",
                new ByteSizeValue(blockCache.stats().diskBytesUsed()));
        }
    }

    /** Returns an unmodifiable snapshot of all registered block caches. */
    public synchronized List<BlockCache> blockCaches() {
        return Collections.unmodifiableList(new ArrayList<>(blockCaches));
    }

    /**
     * Returns the first registered block cache, or {@code null} if none registered.
     * Convenience method for the single-cache case.
     */
    @Nullable
    public synchronized BlockCache blockCache() {
        return blockCaches.isEmpty() ? null : blockCaches.get(0);
    }

    /**
     * Returns the total configured capacity (in bytes) across all registered block caches.
     * Used by {@code WarmFsService} to compute the virtual warm-node capacity.
     */
    public synchronized long blockCacheCapacityBytes() {
        return blockCaches.stream().mapToLong(bc -> bc.stats().totalBytes()).sum();
    }

    /**
     * Returns total SSD bytes currently used by all block caches combined.
     */
    public synchronized long blockCacheDiskBytesUsed() {
        return blockCaches.stream().mapToLong(bc -> bc.stats().diskBytesUsed()).sum();
    }

    /** Returns total bytes currently used across all caches (FileCache + block caches). */
    public synchronized long cacheUtilizedBytes() {
        long used = fileCache.usage();
        for (BlockCache bc : blockCaches) {
            BlockCacheStats s = bc.stats();
            used += s.memoryBytesUsed() + s.diskBytesUsed();
        }
        return used;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Stats aggregation
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns aggregate stats across all caches for {@code _nodes/stats aggregate_file_cache}.
     * Block cache counters are folded into the FileCache stats sections.
     */
    public synchronized AggregateFileCacheStats aggregateStats() {
        AggregateFileCacheStats fileCacheStats = fileCache.cacheStats();
        if (blockCaches.isEmpty()) {
            return fileCacheStats;
        }
        AggregateFileCacheStats result = fileCacheStats;
        for (BlockCache bc : blockCaches) {
            result = mergeStats(result, bc.stats());
        }
        return result;
    }

    private static AggregateFileCacheStats mergeStats(AggregateFileCacheStats fc, BlockCacheStats bc) {
        FileCacheStats mergedOverall = new FileCacheStats(
            fc.getActive().getBytes(),
            fc.getTotal().getBytes()   + bc.totalBytes(),             // configured capacity (not usage)
            fc.getUsed().getBytes()    + bc.diskBytesUsed() + bc.memoryBytesUsed(),
            fc.getPinnedUsage().getBytes(),
            fc.getEvicted().getBytes() + bc.evictionBytes(),          // bytes displaced, not count
            fc.getRemoved().getBytes() + bc.removedBytes(),           // explicit removal bytes
            fc.getCacheHits()          + bc.hits(),
            fc.getCacheMisses()        + bc.misses(),
            AggregateFileCacheStats.FileCacheStatsType.OVER_ALL_STATS
        );
        FileCacheStats fcBlock = fc.getBlockFileCacheStats();
        FileCacheStats mergedBlock = new FileCacheStats(
            fcBlock.getActive(),
            fcBlock.getTotal()   + bc.totalBytes(),                   // configured capacity
            fcBlock.getUsed()    + bc.diskBytesUsed() + bc.memoryBytesUsed(),
            fcBlock.getPinnedUsage(),
            fcBlock.getEvicted() + bc.evictionBytes(),                // bytes displaced
            fcBlock.getRemoved() + bc.removedBytes(),                 // explicit removal bytes
            fcBlock.getCacheHits()   + bc.hits(),
            fcBlock.getCacheMisses() + bc.misses(),
            AggregateFileCacheStats.FileCacheStatsType.BLOCK_FILE_STATS
        );
        return new AggregateFileCacheStats(
            fc.getTimestamp(),
            mergedOverall,
            fc.getFullFileCacheStats(),
            mergedBlock,
            fc.getPinnedFileCacheStats()
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /** Closes all registered block caches, then lets FileCache be GC'd. */
    @Override
    public synchronized void close() throws IOException {
        if (!blockCaches.isEmpty()) {
            IOUtils.closeWhileHandlingException(blockCaches);
            logger.info("Block caches closed ({})", blockCaches.size());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static void restoreFileCacheFromDisk(
        Settings settings,
        NodeEnvironment.NodePath fileCacheNodePath,
        FileCache fileCache
    ) throws IOException {
        ForkJoinPool loadFileCacheThreadpool = new ForkJoinPool(
            Runtime.getRuntime().availableProcessors(),
            Node.CustomForkJoinWorkerThread::new,
            null,
            false
        );
        SetOnce<UncheckedIOException> exception = new SetOnce<>();
        ForkJoinTask<Void> fileCacheFilesLoadTask = loadFileCacheThreadpool.submit(
            new FileCache.LoadTask(fileCacheNodePath.fileCachePath, fileCache, exception)
        );
        if (org.opensearch.cluster.node.DiscoveryNode.isDedicatedWarmNode(settings)) {
            ForkJoinTask<Void> indicesFilesLoadTask = loadFileCacheThreadpool.submit(
                new FileCache.LoadTask(fileCacheNodePath.indicesPath, fileCache, exception)
            );
            indicesFilesLoadTask.join();
        }
        fileCacheFilesLoadTask.join();
        loadFileCacheThreadpool.shutdown();
        if (exception.get() != null) {
            logger.error("File cache initialization failed.", exception.get());
            throw new OpenSearchException(exception.get());
        }
    }


    /**
     * Computes the total warm-cache SSD budget in bytes.
     * Called by {@code Node.java} before plugins' {@code createComponents()} so that
     * {@link org.opensearch.plugins.BlockCacheProvider#requestedCapacityBytes} can receive
     * the correct budget value.
     */
    public static long computeTotalBudgetBytes(Settings settings, NodeEnvironment nodeEnvironment) {
        long totalSSDBytes = ExceptionsHelper.catchAsRuntimeException(
            () -> FsProbe.getTotalSize(nodeEnvironment.fileCacheNodePath()));
        String warmCacheSizeRaw = Node.NODE_SEARCH_CACHE_SIZE_SETTING.get(settings);
        return calculateFileCacheSize(warmCacheSizeRaw, totalSSDBytes);
    }

    private static long calculateFileCacheSize(String capacityRaw, long availableSpace) {
        try {
            RatioValue ratioValue = RatioValue.parseRatioValue(capacityRaw);
            return Math.round(availableSpace * ratioValue.getAsRatio());
        } catch (OpenSearchParseException e) {
            try {
                return ByteSizeValue.parseBytesSizeValue(capacityRaw, Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey()).getBytes();
            } catch (OpenSearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
    }
}
