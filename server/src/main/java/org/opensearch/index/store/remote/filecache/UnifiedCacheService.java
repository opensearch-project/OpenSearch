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
import org.opensearch.blockcache.BlockCacheHandle;
import org.opensearch.blockcache.foyer.FoyerBlockCache;
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
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Single authority for all warm-node SSD caches on this node.
 *
 * <p>Owns the lifecycle, disk budget, validation, and stats aggregation for:
 * <ul>
 *   <li>{@link FileCache} — Lucene block-file cache. Always present on warm nodes.</li>
 *   <li>{@link BlockCacheHandle} (optional) — block cache for variable-size byte ranges.
 *       Present when {@code block_cache.size > 0%}.</li>
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

    /** Block cache for variable-size byte ranges. Nullable — present only when {@code block_cache.size > 0%}. */
    @Nullable
    private final BlockCacheHandle blockCacheHandle;

    // Private constructor — use create().
    private UnifiedCacheService(FileCache fileCache, @Nullable BlockCacheHandle blockCacheHandle) {
        this.fileCache = fileCache;
        this.blockCacheHandle = blockCacheHandle;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Static factory
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates a {@code UnifiedCacheService} for a warm node.
     *
     * <p>This is the single place where:
     * <ol>
     *   <li>Total SSD capacity is read from {@code fileCacheNodePath}.</li>
     *   <li>Disk budget is split between FileCache and the block cache.</li>
     *   <li>The combined allocation is validated against available SSD —
     *       fails fast before creating either cache.</li>
     *   <li>FileCache is created and surviving files are restored from disk.</li>
     *   <li>Block cache is created if {@code block_cache.size > 0%} and the node is warm.</li>
     * </ol>
     *
     * <p>Must only be called when {@code DiscoveryNode.isWarmNode(settings) == true}.
     *
     * @param settings        node settings
     * @param nodeEnvironment node environment (provides fileCacheNodePath)
     * @return a fully initialised {@code UnifiedCacheService}
     * @throws IllegalArgumentException if configured capacities exceed available SSD
     * @throws IOException              if FileCache restoration from disk fails
     */
    public static UnifiedCacheService create(Settings settings, NodeEnvironment nodeEnvironment) throws IOException {
        // Step 1: Read total SSD capacity from the fileCacheNodePath.
        NodeEnvironment.NodePath fileCacheNodePath = nodeEnvironment.fileCacheNodePath();
        long totalSSDBytes = ExceptionsHelper.catchAsRuntimeException(() -> FsProbe.getTotalSize(fileCacheNodePath));

        // Step 2: Compute the total warm-cache SSD budget from node.search.cache.size.
        // This is the ceiling for ALL caches combined (FileCache + block cache).
        // Default on warm nodes: 80% of SSD.
        String warmCacheSizeRaw = Node.NODE_SEARCH_CACHE_SIZE_SETTING.get(settings);
        long totalBudgetBytes = calculateFileCacheSize(warmCacheSizeRaw, totalSSDBytes);

        // Step 3: Split the budget between block cache and FileCache.
        // block_cache.size is a percentage of the total budget (default: 25%).
        // Set to 0% to disable the block cache and give all budget to FileCache.
        long blockCacheBytes = resolveBlockCacheBytes(settings, totalBudgetBytes);
        long fileCacheBytes = totalBudgetBytes - blockCacheBytes;

        // Step 4: Validate before creating anything (fails fast, no partial state).
        validate(fileCacheBytes, blockCacheBytes, totalSSDBytes);

        // Step 5: Create FileCache
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(fileCacheBytes);
        fileCacheNodePath.fileCacheReservedSize = new ByteSizeValue(fileCacheBytes, ByteSizeUnit.BYTES);
        restoreFileCacheFromDisk(settings, fileCacheNodePath, fileCache);

        // Step 6: Create block cache if capacity is allocated and the node is warm.
        // TODO: also gate on a Parquet/DataFusion feature flag — nodes without Parquet
        //       workloads should not allocate SSD to the block cache even if block_cache.size > 0%.
        BlockCacheHandle blockCacheHandle = null;

        if (blockCacheBytes > 0 && org.opensearch.cluster.node.DiscoveryNode.isWarmNode(settings)) {
            String diskDir = fileCacheNodePath.fileCachePath.resolve("column-cache").toString();
            long blockSizeBytes = BlockCacheSettings.BLOCK_SIZE_SETTING.get(settings).getBytes();
            String ioEngine = BlockCacheSettings.IO_ENGINE_SETTING.get(settings);
            blockCacheHandle = new BlockCacheHandle(new FoyerBlockCache(blockCacheBytes, diskDir, blockSizeBytes, ioEngine));
            logger.info("Block cache created: {} at {}", new ByteSizeValue(blockCacheBytes), diskDir);
        }

        return new UnifiedCacheService(fileCache, blockCacheHandle);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Validation
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Validates that the combined cache allocation fits within available SSD capacity.
     * Called before either cache is created — fails fast with no partial state.
     */
    static void validate(long fileCacheBytes, long blockCacheBytes, long totalSSDBytes) {
        if (totalSSDBytes <= 0) {
            throw new IllegalArgumentException(
                "Unable to determine SSD capacity; got: " + totalSSDBytes
                    + ". Ensure the filecache path is mounted and readable."
            );
        }
        if (blockCacheBytes < 0) {
            throw new IllegalArgumentException(
                "blockCacheBytes must be >= 0; got: " + blockCacheBytes
            );
        }
        if (fileCacheBytes + blockCacheBytes > totalSSDBytes) {
            throw new IllegalArgumentException(
                "Warm node SSD allocation exceeds available capacity. "
                    + "file_cache="
                    + new ByteSizeValue(fileCacheBytes)
                    + ", block_cache="
                    + new ByteSizeValue(blockCacheBytes)
                    + ", total SSD="
                    + new ByteSizeValue(totalSSDBytes)
                    + ". Reduce block_cache.size or node.search.cache.size."
            );
        }
        if (fileCacheBytes <= 0) {
            throw new IllegalArgumentException(
                "After allocating "
                    + new ByteSizeValue(blockCacheBytes)
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

    /** Returns the block cache handle, or {@code null} if not configured. */
    @Nullable
    public BlockCacheHandle blockCacheHandle() {
        return blockCacheHandle;
    }

    /**
     * Returns total SSD bytes reserved by all caches combined (FileCache + block cache).
     * Used by {@code WarmFsService} for correct disk watermark calculations.
     */
    public long totalCacheSSDBytes() {
        return fileCache.capacity() + blockCacheCapacityBytes();
    }

    /** Returns SSD bytes reserved by the block cache, or {@code 0} if not configured. */
    public long blockCacheCapacityBytes() {
        return blockCacheHandle != null ? blockCacheHandle.diskCapacityBytes() : 0L;
    }

    /** Returns total bytes currently used across all caches (FileCache + block cache). */
    public long cacheUtilizedBytes() {
        long used = fileCache.usage();
        if (blockCacheHandle != null) {
            used += ((org.opensearch.blockcache.stats.AggregateBlockCacheStats) blockCacheHandle.cacheStats())
                .overallStats().usedBytes();
        }
        return used;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Stats aggregation
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns aggregate stats across all caches for {@code _nodes/stats aggregate_file_cache}.
     * When no block cache is configured, returns FileCache stats unchanged (backwards compatible).
     */
    public AggregateFileCacheStats aggregateStats() {
        AggregateFileCacheStats fileCacheStats = fileCache.fileCacheStats();
        if (blockCacheHandle == null) {
            return fileCacheStats;
        }
        org.opensearch.blockcache.stats.AggregateBlockCacheStats blockStats =
            (org.opensearch.blockcache.stats.AggregateBlockCacheStats) blockCacheHandle.cacheStats();
        return mergeStats(fileCacheStats, blockStats);
    }

    /**
     * Merges {@link FileCache} stats with block cache stats.
     * Lives here because only this class has knowledge of both concrete types.
     *
     * <p>Merge rules:
     * <ul>
     *   <li>{@code over_all_stats} = FileCache overall + block cache overall</li>
     *   <li>{@code block_file_stats} = FileCache block-file stats + block cache block-level stats</li>
     *   <li>{@code full_file_stats}, {@code pinned_file_stats} = FileCache only, unchanged</li>
     * </ul>
     */
    private static AggregateFileCacheStats mergeStats(
        AggregateFileCacheStats fc,
        org.opensearch.blockcache.stats.AggregateBlockCacheStats bc
    ) {
        org.opensearch.blockcache.stats.BlockCacheStats bcOverall = bc.overallStats();
        org.opensearch.blockcache.stats.BlockCacheStats bcBlock   = bc.blockLevelStats();

        // Fold block cache overall stats into the FileCache overall section.
        FileCacheStats mergedOverall = new FileCacheStats(
            fc.getActive().getBytes(),
            fc.getTotal().getBytes()   + bcOverall.capacityBytes(),
            fc.getUsed().getBytes()    + bcOverall.usedBytes(),
            fc.getPinnedUsage().getBytes(),
            fc.getEvicted().getBytes() + bcOverall.evictionBytes(),
            fc.getRemoved().getBytes(),
            fc.getCacheHits()          + bcOverall.hitCount(),
            fc.getCacheMisses()        + bcOverall.missCount(),
            AggregateFileCacheStats.FileCacheStatsType.OVER_ALL_STATS
        );
        // Fold block cache block-level stats into the FileCache block-file section.
        FileCacheStats fcBlock = fc.getBlockFileCacheStats();
        FileCacheStats mergedBlock = new FileCacheStats(
            fcBlock.getActive(),
            fcBlock.getTotal()   + bcBlock.capacityBytes(),
            fcBlock.getUsed()    + bcBlock.usedBytes(),
            fcBlock.getPinnedUsage(),
            fcBlock.getEvicted() + bcBlock.evictionBytes(),
            fcBlock.getRemoved(),
            fcBlock.getCacheHits()   + bcBlock.hitCount(),
            fcBlock.getCacheMisses() + bcBlock.missCount(),
            AggregateFileCacheStats.FileCacheStatsType.BLOCK_FILE_STATS
        );
        // full_file_stats and pinned_file_stats are FileCache-only — no block cache equivalent.
        return new AggregateFileCacheStats(
            fc.getTimestamp(),
            mergedOverall,
            fc.getFullFileCacheStats(),    // FileCache only, unchanged
            mergedBlock,
            fc.getPinnedFileCacheStats()   // FileCache only, unchanged
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /** Closes the block cache first (async I/O in flight), then lets FileCache be GC'd. */
    @Override
    public void close() throws IOException {
        if (blockCacheHandle != null) {
            IOUtils.closeWhileHandlingException(Collections.singleton(blockCacheHandle));
            logger.info("Block cache closed");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Restores FileCache entries from surviving files on disk.
     * Mirrors the ForkJoinPool logic currently in {@code Node.java.initializeFileCache()}.
     */
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
     * Resolves the block cache byte allocation from {@code block_cache.size}.
     *
     * <p>{@code block_cache.size} is a percentage/ratio of {@code totalBudgetBytes}
     * (the total warm-cache SSD budget from {@code node.search.cache.size}).
     * Default: {@code 25%} — block cache gets 25% of the budget, FileCache gets 75%.
     * Set to {@code 0%} to disable the block cache entirely.
     *
     * @param settings         node settings
     * @param totalBudgetBytes total warm-cache SSD budget in bytes
     * @return bytes to allocate to the block cache; {@code 0} if {@code block_cache.size=0%}
     */
    static long resolveBlockCacheBytes(Settings settings, long totalBudgetBytes) {
        String cacheSizeRaw = BlockCacheSettings.CACHE_SIZE_SETTING.get(settings);
        RatioValue ratio = RatioValue.parseRatioValue(cacheSizeRaw);
        return Math.round(totalBudgetBytes * ratio.getAsRatio());
    }

    /**
     * Parses the raw capacity string (percentage or absolute bytes) into a byte count
     * applied against {@code availableSpace}. Used to compute the total warm-cache SSD
     * budget from {@code node.search.cache.size}.
     */
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
