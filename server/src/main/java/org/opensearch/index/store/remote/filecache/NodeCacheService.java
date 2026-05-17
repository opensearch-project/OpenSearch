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
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.fs.FsProbe;
import org.opensearch.node.Node;
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheProvider;
import org.opensearch.plugins.BlockCacheRegistry;
import org.opensearch.plugins.BlockCacheStats;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Single authority for all warm-node SSD caches on this node.
 *
 * <p>Owns the lifecycle, disk budget, validation, and stats aggregation for:
 * <ul>
 *   <li>{@link FileCache} — Lucene block-file cache. Always present on warm nodes.</li>
 *   <li>{@link BlockCache} list (optional) — block caches for variable-size byte ranges,
 *       registered after node startup by block-cache plugins via {@link #registerProviders}.</li>
 * </ul>
 *
 * <p>Only instantiated on warm nodes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NodeCacheService implements Closeable, BlockCacheRegistry {

    private static final Logger logger = LogManager.getLogger(NodeCacheService.class);

    /** LRU cache for Lucene index files. Always present on warm nodes. */
    private final FileCache fileCache;

    /**
     * Pre-computed virtual bytes that all registered block-cache plugins can serve.
     * Equals sum(plugin.reservedBytes * plugin.dataToCapacityRatio), computed once at creation.
     */
    private final long virtualBlockCacheBytes;

    /**
     * Block caches registered by plugins after node startup via {@link #registerProviders}.
     * Uses CopyOnWriteArrayList so reads (stats aggregation on every _nodes/stats call)
     * are lock-free while writes (plugin registration at startup) are rare.
     */
    private final List<BlockCache> blockCaches = new CopyOnWriteArrayList<>();

    /** Package-private for testing — use {@link #create} in production. */
    NodeCacheService(FileCache fileCache, long virtualBlockCacheBytes) {
        this.fileCache = fileCache;
        this.virtualBlockCacheBytes = virtualBlockCacheBytes;
    }

    /** Package-private constructor for testing — creates an orchestrator with no block-cache budget. */
    NodeCacheService(FileCache fileCache) {
        this(fileCache, 0L);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Static factory
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates a {@code NodeCacheService} for a warm node.
     *
     * <p>Performs the full budget phase: queries each provider for its requested
     * capacity, validates the budget, creates FileCache, restores surviving files
     * from disk, and computes virtual block-cache bytes for capacity reporting.
     *
     * <p>After creation, call {@link #registerProviders} once all plugins have
     * finished {@code createComponents()} to wire in the live BlockCache instances.
     *
     * <p>Must only be called when {@code DiscoveryNode.isWarmNode(settings) == true}.
     *
     * @param providers all discovered {@link BlockCacheProvider} plugins
     */
    public static NodeCacheService create(Settings settings, NodeEnvironment nodeEnvironment, Map<String, BlockCacheProvider> providers)
        throws IOException {
        NodeEnvironment.NodePath fileCacheNodePath = nodeEnvironment.fileCacheNodePath();
        long totalSSDBytes = ExceptionsHelper.catchAsRuntimeException(() -> FsProbe.getTotalSize(fileCacheNodePath));

        String warmCacheSizeRaw = Node.NODE_SEARCH_CACHE_SIZE_SETTING.get(settings);
        long totalBudgetBytes = calculateFileCacheSize(warmCacheSizeRaw, totalSSDBytes);

        // Budget phase: ask each provider how much SSD it needs, then partition.
        long blockCacheBytes = computeBlockCacheBudget(providers, settings, totalBudgetBytes);
        long fileCacheBytes = totalBudgetBytes - blockCacheBytes;

        validate(fileCacheBytes, blockCacheBytes, totalSSDBytes);

        // Inform each provider of its exact reserved capacity to avoid re-derivation.
        for (BlockCacheProvider provider : providers.values()) {
            long reserved = provider.requestedCapacityBytes(settings, totalBudgetBytes);
            provider.setReservedCapacityBytes(reserved);
        }

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(fileCacheBytes);
        fileCacheNodePath.fileCacheReservedSize = new ByteSizeValue(fileCacheBytes, ByteSizeUnit.BYTES);
        restoreFileCacheFromDisk(settings, fileCacheNodePath, fileCache);

        // Pre-compute virtual capacity: each plugin's reserved bytes × its amplification ratio.
        long virtualBytes = computeVirtualBlockCacheBytes(providers, settings, totalBudgetBytes);

        return new NodeCacheService(fileCache, virtualBytes);
    }

    /**
     * Sums the SSD bytes requested by all registered block-cache providers.
     */
    public static long computeBlockCacheBudget(Map<String, BlockCacheProvider> providers, Settings settings, long totalBudgetBytes) {
        return providers.values().stream().mapToLong(p -> p.requestedCapacityBytes(settings, totalBudgetBytes)).sum();
    }

    /**
     * Computes the virtual data bytes that all block caches can serve, accounting
     * for each provider's data-to-cache amplification ratio.
     */
    static long computeVirtualBlockCacheBytes(Map<String, BlockCacheProvider> providers, Settings settings, long totalBudgetBytes) {
        return providers.values()
            .stream()
            .mapToLong(p -> Math.round(p.requestedCapacityBytes(settings, totalBudgetBytes) * p.dataToCapacityRatio(settings)))
            .sum();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Validation
    // ─────────────────────────────────────────────────────────────────────────

    static void validate(long fileCacheBytes, long blockCacheBytes, long totalSSDBytes) {
        if (totalSSDBytes <= 0) {
            throw new SettingsException(
                "Unable to determine SSD capacity; got: " + totalSSDBytes + ". Ensure the filecache path is mounted and readable."
            );
        }
        if (blockCacheBytes < 0) {
            throw new SettingsException(
                "Block cache allocation must be >= 0; got: " + blockCacheBytes + ". Check block cache plugin configuration."
            );
        }

        if (fileCacheBytes <= 0) {
            throw new SettingsException(
                "After allocating "
                    + new ByteSizeValue(blockCacheBytes)
                    + " to block cache(s), no SSD budget remains for FileCache. "
                    + "Reduce block cache allocation or increase node.search.cache.size."
            );
        }

        if (fileCacheBytes + blockCacheBytes > totalSSDBytes) {
            throw new SettingsException(
                "Warm node SSD allocation exceeds available capacity. "
                    + "file_cache="
                    + new ByteSizeValue(fileCacheBytes)
                    + ", block_cache="
                    + new ByteSizeValue(blockCacheBytes)
                    + ", total SSD="
                    + new ByteSizeValue(totalSSDBytes)
                    + ". Reduce node.search.cache.size or block cache allocation."
            );
        }

    }

    // ─────────────────────────────────────────────────────────────────────────
    // Provider registration
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Wires live BlockCache instances from providers after {@code createComponents()}.
     * Call once after all plugins have created their components.
     */
    public void registerProviders(Map<String, BlockCacheProvider> providers) {
        for (BlockCacheProvider p : providers.values()) {
            p.getBlockCache().ifPresent(this::addBlockCache);
        }
    }

    /** Package-private for testing. */
    void addBlockCache(BlockCache blockCache) {
        if (blockCache == null) return;
        blockCaches.add(blockCache);
        logger.info("Block cache registered (disk bytes used so far: {})", new ByteSizeValue(blockCache.stats().diskBytesUsed()));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors (lock-free reads — blockCaches is CopyOnWriteArrayList)
    // ─────────────────────────────────────────────────────────────────────────

    /** Returns the {@link FileCache} instance. */
    public FileCache fileCache() {
        return fileCache;
    }

    @Override
    public java.util.Optional<BlockCache> get(String name) {
        return blockCaches.stream().filter(bc -> name.equals(bc.cacheName())).findFirst();
    }

    /** Returns a snapshot of all registered block caches. */
    public List<BlockCache> blockCaches() {
        return List.copyOf(blockCaches);
    }

    /**
     * Returns the pre-computed virtual bytes that all block caches can serve.
     * Used by {@link org.opensearch.monitor.fs.WarmFsService} for capacity reporting.
     */
    public long virtualBlockCacheBytes() {
        return virtualBlockCacheBytes;
    }

    /**
     * Returns the total configured capacity (in bytes) across all registered block caches.
     */
    public long blockCacheCapacityBytes() {
        long total = 0;
        for (BlockCache bc : blockCaches) {
            total += bc.stats().totalBytes();
        }
        return total;
    }

    /**
     * Returns total SSD bytes currently used by all block caches combined.
     */
    public long blockCacheDiskBytesUsed() {
        long total = 0;
        for (BlockCache bc : blockCaches) {
            total += bc.stats().diskBytesUsed();
        }
        return total;
    }

    /** Returns total bytes currently used across all caches (FileCache + block caches). */
    public long cacheUtilizedBytes() {
        long used = fileCache != null ? fileCache.usage() : 0L;
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
     *
     * <p>Block cache counters are folded into the merged overall/block sections so that
     * the top-level and sub-section numbers reflect the true cross-cache rollup.
     */
    public AggregateFileCacheStats aggregateStats() {
        AggregateFileCacheStats fileCacheStats = fileCache.fileCacheStats();
        if (blockCaches.isEmpty()) {
            return fileCacheStats;
        }

        BlockCacheStats combined = combineBlockCacheStats();
        AggregateFileCacheStats merged = mergeStats(fileCacheStats, combined);

        return new AggregateFileCacheStats(
            merged.getTimestamp(),
            new FileCacheStats(
                merged.getActive().getBytes(),
                merged.getTotal().getBytes(),
                merged.getUsed().getBytes(),
                merged.getPinnedUsage().getBytes(),
                merged.getEvicted().getBytes(),
                merged.getRemoved().getBytes(),
                merged.getCacheHits(),
                merged.getCacheMisses(),
                AggregateFileCacheStats.FileCacheStatsType.OVER_ALL_STATS
            ),
            merged.getFullFileCacheStats(),
            merged.getBlockFileCacheStats(),
            merged.getPinnedFileCacheStats()
        );
    }

    private BlockCacheStats combineBlockCacheStats() {
        long hits = 0, misses = 0, hitBytes = 0, missBytes = 0;
        long evictions = 0, evictionBytes = 0, removed = 0, removedBytes = 0;
        long memoryUsed = 0, diskUsed = 0, totalCapacity = 0, activeInBytes = 0;

        for (BlockCache bc : blockCaches) {
            BlockCacheStats s = bc.stats();
            hits += s.hits();
            misses += s.misses();
            hitBytes += s.hitBytes();
            missBytes += s.missBytes();
            evictions += s.evictions();
            evictionBytes += s.evictionBytes();
            removed += s.removed();
            removedBytes += s.removedBytes();
            memoryUsed += s.memoryBytesUsed();
            diskUsed += s.diskBytesUsed();
            totalCapacity += s.totalBytes();
            activeInBytes += s.activeInBytes();
        }
        return new BlockCacheStats(
            hits,
            misses,
            hitBytes,
            missBytes,
            evictions,
            evictionBytes,
            removed,
            removedBytes,
            memoryUsed,
            diskUsed,
            totalCapacity,
            activeInBytes
        );
    }

    private static AggregateFileCacheStats mergeStats(AggregateFileCacheStats fc, BlockCacheStats bc) {
        FileCacheStats mergedOverall = new FileCacheStats(
            fc.getActive().getBytes() + bc.activeInBytes(),
            fc.getTotal().getBytes() + bc.totalBytes(),
            fc.getUsed().getBytes() + bc.diskBytesUsed() + bc.memoryBytesUsed(),
            fc.getPinnedUsage().getBytes(),
            fc.getEvicted().getBytes() + bc.evictionBytes(),
            fc.getRemoved().getBytes() + bc.removedBytes(),
            fc.getCacheHits() + bc.hits(),
            fc.getCacheMisses() + bc.misses(),
            AggregateFileCacheStats.FileCacheStatsType.OVER_ALL_STATS
        );
        FileCacheStats fcBlock = fc.getBlockFileCacheStats();
        FileCacheStats mergedBlock = new FileCacheStats(
            fcBlock.getActive() + bc.activeInBytes(),
            fcBlock.getTotal() + bc.totalBytes(),
            fcBlock.getUsed() + bc.diskBytesUsed() + bc.memoryBytesUsed(),
            fcBlock.getPinnedUsage(),
            fcBlock.getEvicted() + bc.evictionBytes(),
            fcBlock.getRemoved() + bc.removedBytes(),
            fcBlock.getCacheHits() + bc.hits(),
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
    public void close() throws IOException {
        if (!blockCaches.isEmpty()) {
            IOUtils.closeWhileHandlingException(blockCaches);
            logger.info("Block caches closed ({})", blockCaches.size());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static void restoreFileCacheFromDisk(Settings settings, NodeEnvironment.NodePath fileCacheNodePath, FileCache fileCache)
        throws IOException {
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
        long totalSSDBytes = ExceptionsHelper.catchAsRuntimeException(() -> FsProbe.getTotalSize(nodeEnvironment.fileCacheNodePath()));
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
