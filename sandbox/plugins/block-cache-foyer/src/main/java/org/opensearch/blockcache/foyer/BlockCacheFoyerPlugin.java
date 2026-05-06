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
import org.opensearch.action.ActionRequest;
import org.opensearch.blockcache.foyer.action.FoyerCacheStatsAction;
import org.opensearch.blockcache.foyer.action.FoyerCacheStatsRequest;
import org.opensearch.blockcache.foyer.action.FoyerCacheStatsResponse;
import org.opensearch.blockcache.foyer.action.RestFoyerCacheStatsAction;
import org.opensearch.blockcache.foyer.action.TransportFoyerCacheStatsAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheProvider;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
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
 * need a node-level block cache.
 *
 * <p>Also implements {@link ActionPlugin} to register the
 * {@code GET /_nodes/foyer_cache/stats} REST + transport action, which
 * exposes rich per-node Foyer stats (two-section FFM snapshot) without
 * touching {@code NodeStats} or the core transport protocol.
 *
 * @opensearch.experimental
 */
public class BlockCacheFoyerPlugin extends Plugin implements BlockCacheProvider, ActionPlugin {

    private static final Logger logger = LogManager.getLogger(BlockCacheFoyerPlugin.class);

    // Foyer cache defaults. Pinned here for deterministic bootstrap; can be promoted
    // to node settings in a follow-up without changing the SPI surface.
    private static final long DEFAULT_DISK_BYTES = 1L << 30; // 1 GiB
    private static final String DEFAULT_DISK_DIR_NAME = "foyer-block-cache";
    private static final long DEFAULT_BLOCK_SIZE_BYTES = 64L * 1024L * 1024L; // 64 MiB
    private static final String DEFAULT_IO_ENGINE = "auto";

    private final AtomicBoolean componentsCreated = new AtomicBoolean(false);
    private volatile FoyerBlockCache cache;

    /** No-arg constructor required by the plugin framework. */
    public BlockCacheFoyerPlugin() {}

    /**
     * Settings constructor (alternate signature used by PluginsService).
     *
     * @param settings node settings; currently unused — Foyer defaults are pinned
     */
    public BlockCacheFoyerPlugin(final Settings settings) {}

    // ─── BlockCacheProvider ───────────────────────────────────────────────────

    @Override
    public Optional<BlockCache> getBlockCache() {
        return Optional.ofNullable(cache);
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

        final String diskDir;
        if (environment.dataFiles().length == 0) {
            diskDir = System.getProperty("java.io.tmpdir") + "/" + DEFAULT_DISK_DIR_NAME;
        } else {
            diskDir = environment.dataFiles()[0].resolve(DEFAULT_DISK_DIR_NAME).toString();
        }

        try {
            cache = new FoyerBlockCache(DEFAULT_DISK_BYTES, diskDir, DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_IO_ENGINE);
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

    // ─── ActionPlugin ─────────────────────────────────────────────────────────

    /**
     * Registers the {@code cluster:monitor/foyer_cache/stats} transport action.
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(FoyerCacheStatsAction.INSTANCE, TransportFoyerCacheStatsAction.class)
        );
    }

    /**
     * Registers the {@code GET /_nodes/foyer_cache/stats} REST handler.
     */
    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestFoyerCacheStatsAction());
    }
}
