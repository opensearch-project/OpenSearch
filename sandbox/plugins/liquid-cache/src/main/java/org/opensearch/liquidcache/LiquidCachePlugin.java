/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.liquidcache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.liquidcache.action.LiquidCacheClearAction;
import org.opensearch.liquidcache.action.LiquidCacheStatsAction;
import org.opensearch.plugins.ActionPlugin;
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
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Liquid Cache plugin. Owns the {@code datafusion.liquid_cache.*} settings, the
 * native {@link LiquidCacheBridge}, and the REST surface, and contributes a
 * native DataFusion session optimizer through the
 * {@link org.opensearch.analytics.spi.NativeQueryOptimizerProvider} SPI. Gated
 * behind {@link FeatureFlags#LIQUID_CACHE_EXPERIMENTAL_SETTING}.
 *
 * @opensearch.experimental
 */
public class LiquidCachePlugin extends Plugin implements ActionPlugin {

    private static final Logger logger = LogManager.getLogger(LiquidCachePlugin.class);

    // Native provider handle from lc_create_optimizer; 0 when disabled.
    private volatile long optimizerHandle = 0L;

    public LiquidCachePlugin(final Settings settings) {}

    @Override
    public List<Setting<?>> getSettings() {
        return LiquidCacheSettings.ALL_SETTINGS;
    }

    /**
     * Initialize the native runtime and return a provider handle, or {@code 0}
     * when the experimental flag is off. Idempotent.
     */
    public synchronized long createNativeOptimizer(Settings settings) {
        if (!FeatureFlags.isEnabled(FeatureFlags.LIQUID_CACHE_EXPERIMENTAL_SETTING)) {
            return 0L;
        }
        if (optimizerHandle != 0L) {
            return optimizerHandle;
        }
        long size = LiquidCacheSettings.LIQUID_CACHE_SIZE.get(settings);
        String policy = LiquidCacheSettings.LIQUID_CACHE_EVICTION_POLICY.get(settings);
        boolean enabled = LiquidCacheSettings.LIQUID_CACHE_ENABLED.get(settings);

        long handle = LiquidCacheBridge.createOptimizer(size, policy, enabled);
        if (handle == 0L) {
            logger.warn("LiquidCachePlugin: native runtime failed to initialize");
            return 0L;
        }
        LiquidCacheBridge.setIndexedMaxColumns(LiquidCacheSettings.LIQUID_CACHE_INDEXED_QUERY_MAX_COLUMNS.get(settings));
        LiquidCacheBridge.setListingMaxColumns(LiquidCacheSettings.LIQUID_CACHE_LISTING_TABLE_MAX_COLUMNS.get(settings));
        this.optimizerHandle = handle;
        return handle;
    }

    public void clearCache() {
        LiquidCacheBridge.resetCache();
    }

    public long[] stats() {
        return LiquidCacheBridge.stats();
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
        if (!FeatureFlags.isEnabled(FeatureFlags.LIQUID_CACHE_EXPERIMENTAL_SETTING)) {
            return Collections.emptyList();
        }
        // lc_set_* are inert until the runtime is initialized, so registration order does not matter.
        final ClusterSettings cs = clusterService.getClusterSettings();
        cs.addSettingsUpdateConsumer(LiquidCacheSettings.LIQUID_CACHE_ENABLED, LiquidCacheBridge::setEnabled);
        cs.addSettingsUpdateConsumer(LiquidCacheSettings.LIQUID_CACHE_SIZE, LiquidCacheBridge::setMemoryLimit);
        cs.addSettingsUpdateConsumer(
            LiquidCacheSettings.LIQUID_CACHE_INDEXED_QUERY_MAX_COLUMNS,
            count -> LiquidCacheBridge.setIndexedMaxColumns((long) count)
        );
        cs.addSettingsUpdateConsumer(
            LiquidCacheSettings.LIQUID_CACHE_LISTING_TABLE_MAX_COLUMNS,
            count -> LiquidCacheBridge.setListingMaxColumns((long) count)
        );
        return Collections.emptyList();
    }

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings settings,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (!FeatureFlags.isEnabled(FeatureFlags.LIQUID_CACHE_EXPERIMENTAL_SETTING)) {
            return Collections.emptyList();
        }
        return List.of(new LiquidCacheClearAction(this), new LiquidCacheStatsAction(this));
    }

    @Override
    public void close() throws IOException {
        final long handle = optimizerHandle;
        optimizerHandle = 0L;
        if (handle != 0L) {
            LiquidCacheBridge.destroyOptimizer(handle);
            logger.info("LiquidCachePlugin closed");
        }
    }
}
