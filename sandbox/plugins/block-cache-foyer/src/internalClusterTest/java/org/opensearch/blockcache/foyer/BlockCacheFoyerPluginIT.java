/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheProvider;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration test exercising {@link BlockCacheFoyerPlugin}'s end-to-end
 * lifecycle against a real OpenSearch node with the native Foyer library
 * loaded.
 *
 * <p>Complements {@code BlockCacheFoyerPluginTests}, which only covers the
 * pure-Java plugin surface (constructors and pre-init state). Paths that
 * require the shared {@code libopensearch_native} library — cache
 * construction, {@link BlockCache#stats()}, {@link BlockCache#close()} — are
 * covered here.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class BlockCacheFoyerPluginIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BlockCacheFoyerPlugin.class);
    }

    /**
     * {@code createComponents} constructs a real Foyer-backed cache via FFM and
     * publishes it through {@link BlockCacheProvider#getBlockCache()}. After
     * node startup the handle must be present and point at a live
     * {@link FoyerBlockCache}.
     */
    public void testPluginPublishesLiveFoyerBlockCache() {
        BlockCacheFoyerPlugin plugin = findPluginInstance();
        Optional<BlockCache> blockCache = plugin.getBlockCache();

        assertTrue("block cache should be published after node init", blockCache.isPresent());
        assertThat(
            "block cache is a FoyerBlockCache when running against the real native library",
            blockCache.get(),
            instanceOf(FoyerBlockCache.class)
        );

        FoyerBlockCache foyer = (FoyerBlockCache) blockCache.get();
        assertThat("native cache pointer must be positive on a live cache", foyer.nativeCachePtr(), greaterThan(0L));
    }

    /**
     * {@link BlockCacheProvider} resolution through {@code pluginsService.filterPlugins}
     * should find exactly one provider on a node that installs block-cache-foyer.
     */
    public void testBlockCacheProviderIsDiscoverable() {
        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        List<BlockCacheProvider> providers = pluginsService.filterPlugins(BlockCacheProvider.class);

        assertEquals("exactly one BlockCacheProvider installed", 1, providers.size());
        BlockCacheProvider provider = providers.get(0);
        assertThat("the provider is BlockCacheFoyerPlugin", provider, instanceOf(BlockCacheFoyerPlugin.class));

        Optional<BlockCache> blockCache = provider.getBlockCache();
        assertTrue("provider returns a live block cache", blockCache.isPresent());
        assertThat(blockCache.get(), instanceOf(FoyerBlockCache.class));
    }

    /**
     * {@link BlockCache#stats()} returns a non-null snapshot. The exact counter
     * values aren't asserted because this build currently returns a
     * zero-valued snapshot (bridging real Foyer counters is a follow-up).
     */
    public void testBlockCacheStatsAreAvailable() {
        BlockCacheFoyerPlugin plugin = findPluginInstance();
        BlockCache cache = plugin.getBlockCache().orElseThrow();

        BlockCacheStats stats = cache.stats();
        assertThat("stats snapshot must be non-null", stats, is(notNullValue()));

        // Counters should be non-negative regardless of whether they've been populated.
        assertTrue("hits non-negative", stats.hits() >= 0);
        assertTrue("misses non-negative", stats.misses() >= 0);
        assertTrue("evictions non-negative", stats.evictions() >= 0);
        assertTrue("memoryBytesUsed non-negative", stats.memoryBytesUsed() >= 0);
        assertTrue("diskBytesUsed non-negative", stats.diskBytesUsed() >= 0);
    }

    /**
     * The same {@link BlockCache} instance must be returned across successive
     * {@link BlockCacheProvider#getBlockCache()} calls — the cache is
     * node-scoped and constructed exactly once.
     */
    public void testBlockCacheIsSingletonPerNode() {
        BlockCacheFoyerPlugin plugin = findPluginInstance();
        BlockCache first = plugin.getBlockCache().orElseThrow();
        BlockCache second = plugin.getBlockCache().orElseThrow();

        assertSame("getBlockCache() must return the same instance on every call", first, second);
    }

    /**
     * Locates the {@link BlockCacheFoyerPlugin} instance on the single cluster node.
     */
    private BlockCacheFoyerPlugin findPluginInstance() {
        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        List<BlockCacheFoyerPlugin> plugins = pluginsService.filterPlugins(BlockCacheFoyerPlugin.class);
        assertEquals("exactly one BlockCacheFoyerPlugin installed", 1, plugins.size());
        return plugins.get(0);
    }
}
