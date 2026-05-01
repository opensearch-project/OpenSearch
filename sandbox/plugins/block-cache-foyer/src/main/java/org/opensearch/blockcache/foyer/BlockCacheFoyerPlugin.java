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
import org.opensearch.blockcache.BlockCacheHandle;
import org.opensearch.blockcache.spi.BlockCacheProvider;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Plugin entry point for the Foyer-backed node-level block cache.
 *
 * <p>Owns:
 * <ul>
 *   <li>Loading {@code libopensearch_native} via the shared FFM loader.</li>
 *   <li>Constructing the singleton {@link FoyerBlockCache}
 *       via {@link FoyerBridge}.</li>
 *   <li>Publishing a {@link BlockCacheHandle} to {@code createComponents} so core
 *       can inject it into downstream plugins.</li>
 *   <li>Discovering {@link BlockCacheProvider} implementations from extending
 *       plugins via {@link ExtensiblePlugin#loadExtensions}.</li>
 * </ul>
 *
 * <p>Per-repository {@code attach} wiring is performed by each native-repository
 * plugin at repo-registration time — the {@code repositoriesServiceSupplier}
 * returns {@code null} at {@code createComponents} time, so this plugin cannot
 * enumerate registered repositories from here. The providers list populated by
 * {@link #loadExtensions(ExtensionLoader)} is exposed to extending plugins so
 * they can invoke {@code attach} themselves once their repositories are live.
 *
 * <p>{@code extendedPlugins = []} — this plugin does not extend any other plugin.
 * Other plugins extend it.
 *
 * @opensearch.experimental
 */
public class BlockCacheFoyerPlugin extends Plugin implements ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(BlockCacheFoyerPlugin.class);

    // Foyer cache defaults. These constants mirror the pre-refactor defaults
    // embedded in the previous lib-side construction. They can be promoted to
    // node settings in a follow-up; pinned here for deterministic bootstrap.
    private static final long DEFAULT_DISK_BYTES = 1L << 30; // 1 GiB
    private static final String DEFAULT_DISK_DIR_NAME = "foyer-block-cache";
    private static final long DEFAULT_BLOCK_SIZE_BYTES = 64L * 1024L * 1024L; // 64 MiB
    private static final String DEFAULT_IO_ENGINE = "auto";

    private final List<BlockCacheProvider> providers = new ArrayList<>();
    private final AtomicBoolean componentsCreated = new AtomicBoolean(false);
    private volatile BlockCacheHandle handle;

    /** No-arg constructor required by the plugin framework. */
    public BlockCacheFoyerPlugin() {}

    /**
     * Settings constructor (alternate signature used by PluginsService).
     *
     * @param settings node settings; currently unused — Foyer defaults are pinned
     */
    public BlockCacheFoyerPlugin(final Settings settings) {}

    @Override
    public void loadExtensions(final ExtensionLoader loader) {
        providers.addAll(loader.loadExtensions(BlockCacheProvider.class));
        logger.info("BlockCacheFoyerPlugin discovered {} BlockCacheProvider extension(s)", providers.size());
    }

    /**
     * Read-only view of the discovered {@link BlockCacheProvider} extensions.
     * Exposed so native-repository plugins can invoke {@code attach} at
     * repo-registration time (Phase 6 of the refactor).
     *
     * @return unmodifiable list of discovered providers; never null
     */
    public List<BlockCacheProvider> providers() {
        return List.copyOf(providers);
    }

    /**
     * Returns the node-level block cache handle once {@link #createComponents}
     * has run, or {@code null} if the node has not yet initialised.
     *
     * @return the node-level {@link BlockCacheHandle}, or {@code null} before init
     */
    public BlockCacheHandle handle() {
        return handle;
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

        final String diskDir;
        if (environment.dataFiles().length == 0) {
            diskDir = System.getProperty("java.io.tmpdir") + "/" + DEFAULT_DISK_DIR_NAME;
        } else {
            diskDir = environment.dataFiles()[0].resolve(DEFAULT_DISK_DIR_NAME).toString();
        }

        final FoyerBlockCache cache;
        try {
            cache = new FoyerBlockCache(DEFAULT_DISK_BYTES, diskDir, DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_IO_ENGINE);
        } catch (final Throwable t) {
            throw new IllegalStateException("Failed to initialise Foyer block cache (diskDir=" + diskDir + ")", t);
        }

        handle = new BlockCacheHandle(cache);
        logger.info(
            "BlockCacheFoyerPlugin created BlockCacheHandle (diskDir={}); {} provider(s) discovered; "
                + "per-repo attach is deferred to repo registration",
            diskDir,
            providers.size()
        );

        return List.of(handle);
    }

    /**
     * Close the {@link BlockCacheHandle}. Idempotent; safe to call multiple times.
     *
     * <p>The handle delegates to {@link FoyerBlockCache#close()}, which is itself
     * idempotent via an {@code AtomicBoolean}. Per design.md, this plugin's
     * {@code close()} runs AFTER every native-repository plugin's {@code close()}
     * because the plugin framework closes plugins in reverse-dependency order
     * (children first, parents second — block-cache-foyer is a parent).
     */
    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            final BlockCacheHandle h = handle;
            if (h != null) {
                h.close();
                logger.info("BlockCacheFoyerPlugin closed");
            }
        }
    }
}
