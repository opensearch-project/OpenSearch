/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.mockito.Mockito.mock;

public class TieredStoragePluginTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        threadPool = new TestThreadPool("TieredStoragePluginTests");
        clusterService = ClusterServiceUtils.createClusterService(Settings.EMPTY, clusterSettings, threadPool);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testConstructor() {
        TieredStoragePlugin plugin = new TieredStoragePlugin();
        assertNotNull(plugin);
    }

    public void testGetPrefetchSettingsSupplier_BeforeCreateComponents() {
        TieredStoragePlugin plugin = new TieredStoragePlugin();
        Supplier<TieredStoragePrefetchSettings> supplier = plugin.getPrefetchSettingsSupplier();
        assertNotNull(supplier);
        assertNull(supplier.get());
    }

    public void testCreateComponents() {
        TieredStoragePlugin plugin = new TieredStoragePlugin();
        Collection<Object> components = plugin.createComponents(
            mock(Client.class),
            clusterService,
            threadPool,
            mock(ResourceWatcherService.class),
            mock(ScriptService.class),
            mock(NamedXContentRegistry.class),
            mock(Environment.class),
            null,
            mock(NamedWriteableRegistry.class),
            mock(IndexNameExpressionResolver.class),
            () -> mock(RepositoriesService.class)
        );
        assertNotNull(components);
        assertTrue(components.isEmpty());

        Supplier<TieredStoragePrefetchSettings> supplier = plugin.getPrefetchSettingsSupplier();
        assertNotNull(supplier.get());
    }

    public void testGetPrefetchSettingsSupplier_AfterCreateComponents() {
        TieredStoragePlugin plugin = new TieredStoragePlugin();
        plugin.createComponents(
            mock(Client.class),
            clusterService,
            threadPool,
            mock(ResourceWatcherService.class),
            mock(ScriptService.class),
            mock(NamedXContentRegistry.class),
            mock(Environment.class),
            null,
            mock(NamedWriteableRegistry.class),
            mock(IndexNameExpressionResolver.class),
            () -> mock(RepositoriesService.class)
        );
        TieredStoragePrefetchSettings settings = plugin.getPrefetchSettingsSupplier().get();
        assertNotNull(settings);
        assertTrue(settings.isStoredFieldsPrefetchEnabled());
        assertEquals(TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT, settings.getReadAheadBlockCount());
    }

    public void testOnIndexModule_WhenFeatureFlagDisabled() {
        TieredStoragePlugin plugin = new TieredStoragePlugin();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test-index", Settings.EMPTY);
        IndexModule indexModule = new IndexModule(
            indexSettings,
            null,
            new InternalEngineFactory(),
            new EngineConfigFactory(indexSettings),
            Collections.emptyMap(),
            () -> true,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            Collections.emptyMap()
        );
        plugin.onIndexModule(indexModule);
    }

    public void testOnIndexModule_WhenFeatureFlagEnabled() throws Exception {
        TieredStoragePlugin plugin = new TieredStoragePlugin();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test-index", Settings.EMPTY);
        IndexModule indexModule = new IndexModule(
            indexSettings,
            null,
            new InternalEngineFactory(),
            new EngineConfigFactory(indexSettings),
            Collections.emptyMap(),
            () -> true,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            Collections.emptyMap()
        );
        FeatureFlags.TestUtils.with(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, () -> { plugin.onIndexModule(indexModule); });
    }
}
