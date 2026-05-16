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
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheConstants;
import org.opensearch.plugins.BlockCacheProvider;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Plugin entry point for the Foyer-backed node-level block cache.
 *
 * <p>Implements {@link BlockCacheProvider} so core can discover this plugin via
 * {@code pluginsService.filterPlugins(BlockCacheProvider.class)}.
 *
 * <p>All Foyer settings are owned here and registered via {@link #getSettings()}:
 * <ul>
 *   <li>{@code block_cache.size} — fraction of the warm-cache SSD budget given to Foyer.</li>
 *   <li>{@code block_cache.block_size} — Foyer disk block size.</li>
 *   <li>{@code block_cache.io_engine} — Foyer I/O engine (auto / io_uring / psync).</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public class BlockCacheFoyerPlugin extends Plugin implements BlockCacheProvider {

    private static final Logger logger = LogManager.getLogger(BlockCacheFoyerPlugin.class);

    private static final String DEFAULT_DISK_DIR_NAME = "foyer-block-cache";

    private final AtomicBoolean componentsCreated = new AtomicBoolean(false);
    private volatile FoyerBlockCache cache;
    private volatile long reservedCapacityBytes;

    /** Settings constructor required by the plugin framework. */
    public BlockCacheFoyerPlugin(final Settings settings) {}

    // ─── BlockCacheProvider ───────────────────────────────────────────────────

    @Override
    public void setReservedCapacityBytes(long bytes) {
        this.reservedCapacityBytes = bytes;
    }

    @Override
    public Optional<BlockCache> getBlockCache() {
        return Optional.ofNullable(cache);
    }

    // ─── Plugin.getSettings ───────────────────────────────────────────────────

    /**
     * Registers Foyer-specific settings with the OpenSearch settings framework.
     * Includes {@code block_cache.size} (capacity fraction) and Foyer-internal settings
     * ({@code block_cache.block_size}, {@code block_cache.io_engine}).
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            FoyerBlockCacheSettings.CACHE_SIZE_SETTING,
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING,
            FoyerBlockCacheSettings.IO_ENGINE_SETTING,
            FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING,
            FoyerBlockCacheSettings.KEY_INDEX_SWEEP_INTERVAL_SETTING
        );
    }

    /**
     * Returns the data-to-cache amplification ratio for this plugin's block cache.
     * Used by {@code WarmFsService} to compute virtual warm-node capacity for shard placement.
     */
    @Override
    public double dataToCapacityRatio(Settings settings) {
        return FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(settings);
    }

    @Override
    public String cacheName() {
        return BlockCacheConstants.DISK_CACHE;
    }

    /**
     * Reports the SSD bytes requested by this plugin from the total warm-cache budget.
     *
     * @param settings         node settings
     * @param totalBudgetBytes total warm-cache SSD budget
     * @return bytes requested; 0 if the block cache is disabled (size = 0%)
     */
    @Override
    public long requestedCapacityBytes(Settings settings, long totalBudgetBytes) {
        String cacheSizeRaw = FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(settings);
        RatioValue ratio = RatioValue.parseRatioValue(cacheSizeRaw);
        return Math.round(totalBudgetBytes * ratio.getAsRatio());
    }

    // ─── Plugin lifecycle ─────────────────────────────────────────────────────

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        if (componentsCreated.compareAndSet(false, true) == false) {
            throw new IllegalStateException("BlockCacheFoyerPlugin.createComponents called more than once");
        }

        final Settings settings = clusterService.getSettings();
        final long blockSizeBytes = FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(settings).getBytes();
        final String ioEngine = FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(settings);
        final long sweepIntervalSecs = FoyerBlockCacheSettings.KEY_INDEX_SWEEP_INTERVAL_SETTING.get(settings);
        // Use the exact capacity reserved by NodeCacheOrchestrator during budget phase.
        final long diskCapacityBytes = reservedCapacityBytes;

        final String diskDir;
        if (environment.dataFiles().length == 0) {
            diskDir = System.getProperty("java.io.tmpdir") + "/" + DEFAULT_DISK_DIR_NAME;
        } else {
            diskDir = environment.dataFiles()[0].resolve(DEFAULT_DISK_DIR_NAME).toString();
        }

        if (diskCapacityBytes <= 0) {
            logger.info("BlockCacheFoyerPlugin: block_cache.size=0, Foyer block cache disabled");
            return List.of();
        }

        try {
            cache = new FoyerBlockCache(diskCapacityBytes, diskDir, blockSizeBytes, ioEngine, sweepIntervalSecs);
        } catch (final Throwable t) {
            throw new IllegalStateException("Failed to initialise Foyer block cache (diskDir=" + diskDir + ")", t);
        }
        logger.info(
            "BlockCacheFoyerPlugin created FoyerBlockCache (diskDir={}, blockSize={}, ioEngine={}, sweepIntervalSecs={})",
            diskDir,
            blockSizeBytes,
            ioEngine,
            sweepIntervalSecs == 0 ? "default(30s)" : sweepIntervalSecs + "s"
        );
        return List.of(cache);
    }

    /**
     * Close the cache. Idempotent; safe to call multiple times.
     */
    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            final FoyerBlockCache c = cache;
            if (c != null) {
                c.close();
                logger.info("BlockCacheFoyerPlugin closed");
            }
        }
    }
}
