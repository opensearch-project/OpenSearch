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
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.BlockCache;
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
 * <p>Implements {@link BlockCacheProvider}: core publishes this SPI as an
 * extension point for consumers to discover via
 * {@code pluginsService.filterPlugins(BlockCacheProvider.class)} when they
 * need a node-level block cache. Consumers are responsible for resolving
 * the cache themselves.
 *
 * <p>{@code extendedPlugins = []} — this plugin does not extend any other
 * plugin, and no other plugin extends it.
 *
 * @opensearch.experimental
 */
public class BlockCacheFoyerPlugin extends Plugin implements BlockCacheProvider {

    private static final Logger logger = LogManager.getLogger(BlockCacheFoyerPlugin.class);

    private static final String DEFAULT_DISK_DIR_NAME = "foyer-block-cache";

    /** Total disk capacity for the Foyer block cache (default 1 GiB). */
    public static final Setting<Long> DISK_CACHE_SIZE_BYTES = Setting.longSetting(
        "foyer.cache.disk_size_bytes",
        1L << 30,
        0L,
        Setting.Property.NodeScope
    );

    /** Block size for cache entries (default 64 MiB). Smaller blocks reduce read amplification. */
    public static final Setting<Long> BLOCK_SIZE_BYTES = Setting.longSetting(
        "foyer.cache.block_size_bytes",
        64L * 1024L * 1024L,
        1L,
        Setting.Property.NodeScope
    );

    /** IO engine for disk cache. Valid: "auto", "libaio", "io_uring", "std" (default "auto"). */
    public static final Setting<String> IO_ENGINE = Setting.simpleString(
        "foyer.cache.io_engine",
        "auto",
        Setting.Property.NodeScope
    );

    private final AtomicBoolean componentsCreated = new AtomicBoolean(false);
    private volatile FoyerBlockCache cache;

    /** No-arg constructor required by the plugin framework. */
    public BlockCacheFoyerPlugin() {}

    /**
     * Settings constructor (alternate signature used by PluginsService).
     *
     * @param settings node settings
     */
    public BlockCacheFoyerPlugin(final Settings settings) {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(DISK_CACHE_SIZE_BYTES, BLOCK_SIZE_BYTES, IO_ENGINE);
    }

    @Override
    public Optional<BlockCache> getBlockCache() {
        return Optional.ofNullable(cache);
    }

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

        Settings settings = environment.settings();
        long diskBytes = DISK_CACHE_SIZE_BYTES.get(settings);
        long blockSize = BLOCK_SIZE_BYTES.get(settings);
        String ioEngine = IO_ENGINE.get(settings);

        final String diskDir;
        if (environment.dataFiles().length == 0) {
            diskDir = System.getProperty("java.io.tmpdir") + "/" + DEFAULT_DISK_DIR_NAME;
        } else {
            diskDir = environment.dataFiles()[0].resolve(DEFAULT_DISK_DIR_NAME).toString();
        }

        try {
            cache = new FoyerBlockCache(diskBytes, diskDir, blockSize, ioEngine);
        } catch (final Throwable t) {
            throw new IllegalStateException("Failed to initialise Foyer block cache (diskDir=" + diskDir + ")", t);
        }
        logger.info("BlockCacheFoyerPlugin created FoyerBlockCache (diskDir={})", diskDir);
        return List.of(cache);
    }

    /**
     * Close the cache. Idempotent; safe to call multiple times. {@link
     * FoyerBlockCache#close()} is itself idempotent via an {@code AtomicBoolean}.
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
