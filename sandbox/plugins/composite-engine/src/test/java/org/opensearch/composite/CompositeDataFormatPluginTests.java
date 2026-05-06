/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompositeDataFormatPlugin}.
 */
public class CompositeDataFormatPluginTests extends OpenSearchTestCase {

    // ---- Setting registration ----

    public void testGetSettingsReturnsAllFourSettings() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        List<Setting<?>> settings = plugin.getSettings();
        assertEquals(5, settings.size());
        assertTrue(settings.contains(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT));
        assertTrue(settings.contains(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS));
        assertTrue(settings.contains(CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT));
        assertTrue(settings.contains(CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS));
        assertTrue(settings.contains(CompositeDataFormatPlugin.CLUSTER_RESTRICT_COMPOSITE_DATAFORMAT_SETTING));
    }

    // ---- Setting defaults and value parsing ----

    public void testPrimaryDataFormatDefaultsToLucene() {
        assertEquals("lucene", CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.get(Settings.EMPTY));
    }

    public void testPrimaryDataFormatReadsExplicitValue() {
        Settings settings = Settings.builder().put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet").build();
        assertEquals("parquet", CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.get(settings));
    }

    public void testSecondaryDataFormatsDefaultsToEmpty() {
        assertTrue(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.get(Settings.EMPTY).isEmpty());
    }

    public void testSecondaryDataFormatsReadsExplicitList() {
        Settings settings = Settings.builder()
            .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "parquet", "arrow")
            .build();
        assertEquals(List.of("parquet", "arrow"), CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.get(settings));
    }

    public void testClusterDefaultPrimaryDataFormatDefaultsToLucene() {
        assertEquals("lucene", CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT.get(Settings.EMPTY));
    }

    public void testClusterDefaultPrimaryDataFormatReadsExplicitValue() {
        Settings settings = Settings.builder().put(CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT.getKey(), "parquet").build();
        assertEquals("parquet", CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT.get(settings));
    }

    public void testClusterDefaultSecondaryDataFormatsDefaultsToEmpty() {
        assertTrue(CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS.get(Settings.EMPTY).isEmpty());
    }

    public void testClusterDefaultSecondaryDataFormatsReadsExplicitList() {
        Settings settings = Settings.builder()
            .putList(CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS.getKey(), "parquet", "arrow")
            .build();
        assertEquals(List.of("parquet", "arrow"), CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS.get(settings));
    }

    // ---- IndexSettingProvider behavior ----

    public void testIndexSettingProviderReturnsEmptyBeforeCreateComponents() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        IndexSettingProvider provider = singleProvider(plugin);
        // createComponents has not run, so clusterService is null and the provider must
        // contribute nothing rather than NPE — allowing fallback to per-setting defaults.
        Settings out = provider.getAdditionalIndexSettings("some-index", false, Settings.EMPTY);
        assertEquals(Settings.EMPTY, out);
    }

    public void testIndexSettingProviderStampsBothClusterDefaultsWhenIndexLevelAbsent() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        Settings clusterBag = Settings.builder()
            .put(CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT.getKey(), "parquet")
            .putList(CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS.getKey(), "arrow")
            .build();
        injectClusterService(plugin, clusterBag);

        IndexSettingProvider provider = singleProvider(plugin);
        Settings out = provider.getAdditionalIndexSettings("some-index", false, Settings.EMPTY);

        assertEquals("parquet", CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.get(out));
        assertEquals(List.of("arrow"), CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.get(out));
    }

    public void testIndexSettingProviderSkipsPrimaryWhenAlreadySet() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        Settings clusterBag = Settings.builder()
            .put(CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT.getKey(), "parquet")
            .putList(CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS.getKey(), "arrow")
            .build();
        injectClusterService(plugin, clusterBag);

        Settings requestOrTemplate = Settings.builder().put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "lucene").build();

        IndexSettingProvider provider = singleProvider(plugin);
        Settings out = provider.getAdditionalIndexSettings("some-index", false, requestOrTemplate);

        assertFalse(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.exists(out));
        assertEquals(List.of("arrow"), CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.get(out));
    }

    public void testIndexSettingProviderSkipsSecondaryWhenAlreadySet() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        Settings clusterBag = Settings.builder()
            .put(CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT.getKey(), "parquet")
            .putList(CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS.getKey(), "arrow")
            .build();
        injectClusterService(plugin, clusterBag);

        Settings requestOrTemplate = Settings.builder()
            .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "parquet")
            .build();

        IndexSettingProvider provider = singleProvider(plugin);
        Settings out = provider.getAdditionalIndexSettings("some-index", false, requestOrTemplate);

        assertEquals("parquet", CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.get(out));
        assertFalse(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.exists(out));
    }

    public void testIndexSettingProviderSkipsBothWhenBothAlreadySet() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        Settings clusterBag = Settings.builder()
            .put(CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT.getKey(), "parquet")
            .putList(CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS.getKey(), "arrow")
            .build();
        injectClusterService(plugin, clusterBag);

        Settings requestOrTemplate = Settings.builder()
            .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "lucene")
            .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "parquet")
            .build();

        IndexSettingProvider provider = singleProvider(plugin);
        Settings out = provider.getAdditionalIndexSettings("some-index", false, requestOrTemplate);

        // Provider contributes nothing when both settings are already explicit.
        assertEquals(Settings.EMPTY, out);
    }

    public void testIndexSettingProviderReadsLiveClusterSettingsOnEachCall() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();

        // Seed cluster settings with empty defaults, then flip them and verify the provider
        // picks up the new values on the next call without any re-init of the plugin.
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(
                CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT,
                CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS,
                CompositeDataFormatPlugin.CLUSTER_RESTRICT_COMPOSITE_DATAFORMAT_SETTING,
                IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_RESTRICT_SKIPLIST
            )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        setClusterServiceField(plugin, clusterService);

        IndexSettingProvider provider = singleProvider(plugin);

        Settings first = provider.getAdditionalIndexSettings("idx-1", false, Settings.EMPTY);
        assertEquals("lucene", CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.get(first));
        assertTrue(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.get(first).isEmpty());

        // Simulate a PUT /_cluster/settings updating the dynamic cluster defaults.
        clusterSettings.applySettings(
            Settings.builder()
                .put(CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT.getKey(), "parquet")
                .putList(CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS.getKey(), "arrow")
                .build()
        );

        Settings second = provider.getAdditionalIndexSettings("idx-2", false, Settings.EMPTY);
        assertEquals("parquet", CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.get(second));
        assertEquals(List.of("arrow"), CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.get(second));
    }

    // ---- Existing getFormatDescriptors coverage ----

    public void testGetFormatDescriptorsDelegatestoPlugins() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put("index.composite.primary_data_format", "lucene")
                .putList("index.composite.secondary_data_formats", "parquet")
                .build()
        );

        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        DataFormat parquetFormat = CompositeTestHelper.stubFormat("parquet", 2, java.util.Set.of());
        when(registry.format("parquet")).thenReturn(parquetFormat);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, java.util.Set.of()));
        when(registry.getFormatDescriptors(indexSettings, parquetFormat)).thenReturn(
            Map.of(
                "parquet",
                (Supplier<DataFormatDescriptor>) () -> new DataFormatDescriptor(
                    "parquet",
                    new org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler()
                )
            )
        );

        Map<String, Supplier<DataFormatDescriptor>> descriptors = plugin.getFormatDescriptors(indexSettings, registry);
        assertEquals(1, descriptors.size());
        assertTrue(descriptors.containsKey("parquet"));
        assertEquals("parquet", descriptors.get("parquet").get().getFormatName());
    }

    public void testGetFormatDescriptorsEmptyWhenNoPluginsMatch() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IndexSettings indexSettings = buildIndexSettings(Settings.EMPTY);

        Map<String, Supplier<DataFormatDescriptor>> descriptors = plugin.getFormatDescriptors(indexSettings, registry);
        assertTrue(descriptors.isEmpty());
    }

    public void testGetStoreStrategiesEmptyWhenNoSubPlugins() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IndexSettings indexSettings = buildIndexSettings(Settings.builder().put("index.composite.primary_data_format", "parquet").build());
        DataFormat parquetFormat = CompositeTestHelper.stubFormat("parquet", 2, java.util.Set.of());
        when(registry.format("parquet")).thenReturn(parquetFormat);
        when(registry.getStoreStrategies(indexSettings, parquetFormat)).thenReturn(Map.of());

        Map<DataFormat, StoreStrategy> result = plugin.getStoreStrategies(indexSettings, registry);
        assertTrue("Should return empty when no sub-plugin found", result.isEmpty());
    }

    public void testGetStoreStrategiesCollectsFromPrimaryPlugin() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IndexSettings indexSettings = buildIndexSettings(Settings.builder().put("index.composite.primary_data_format", "parquet").build());

        DataFormat parquetFormat = CompositeTestHelper.stubFormat("parquet", 2, java.util.Set.of());
        StoreStrategy parquetStrategy = mock(StoreStrategy.class);
        when(registry.format("parquet")).thenReturn(parquetFormat);
        when(registry.getStoreStrategies(indexSettings, parquetFormat)).thenReturn(Map.of(parquetFormat, parquetStrategy));

        Map<DataFormat, StoreStrategy> result = plugin.getStoreStrategies(indexSettings, registry);
        assertEquals(1, result.size());
        assertSame(parquetStrategy, result.get(parquetFormat));
    }

    public void testGetStoreStrategiesCollectsPrimaryAndSecondary() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put("index.composite.primary_data_format", "lucene")
                .putList("index.composite.secondary_data_formats", "parquet")
                .build()
        );

        DataFormat luceneFormat = CompositeTestHelper.stubFormat("lucene", 1, java.util.Set.of());
        DataFormat parquetFormat = CompositeTestHelper.stubFormat("parquet", 2, java.util.Set.of());
        StoreStrategy parquetStrategy = mock(StoreStrategy.class);

        when(registry.format("lucene")).thenReturn(luceneFormat);
        when(registry.format("parquet")).thenReturn(parquetFormat);
        when(registry.getStoreStrategies(indexSettings, luceneFormat)).thenReturn(Map.of());
        when(registry.getStoreStrategies(indexSettings, parquetFormat)).thenReturn(Map.of(parquetFormat, parquetStrategy));

        Map<DataFormat, StoreStrategy> result = plugin.getStoreStrategies(indexSettings, registry);
        assertEquals(1, result.size());
        assertSame(parquetStrategy, result.get(parquetFormat));
    }

    public void testGetStoreStrategiesEmptyForDefaultPrimaryWithoutPlugin() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IndexSettings indexSettings = buildIndexSettings(Settings.EMPTY);
        DataFormat luceneFormat = CompositeTestHelper.stubFormat("lucene", 1, java.util.Set.of());
        when(registry.format("lucene")).thenReturn(luceneFormat);
        when(registry.getStoreStrategies(indexSettings, luceneFormat)).thenReturn(Map.of());

        Map<DataFormat, StoreStrategy> result = plugin.getStoreStrategies(indexSettings, registry);
        assertTrue("Should return empty when lucene sub-plugin not found", result.isEmpty());
    }

    // ---- Helpers ----

    private static IndexSettings buildIndexSettings(Settings extra) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(extra)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
    }

    private static IndexSettingProvider singleProvider(CompositeDataFormatPlugin plugin) {
        Collection<IndexSettingProvider> providers = plugin.getAdditionalIndexSettingProviders();
        assertEquals(1, providers.size());
        return providers.iterator().next();
    }

    private static void injectClusterService(CompositeDataFormatPlugin plugin, Settings clusterBag) {
        ClusterSettings clusterSettings = new ClusterSettings(
            clusterBag,
            Set.of(
                CompositeDataFormatPlugin.CLUSTER_PRIMARY_DATA_FORMAT,
                CompositeDataFormatPlugin.CLUSTER_SECONDARY_DATA_FORMATS,
                CompositeDataFormatPlugin.CLUSTER_RESTRICT_COMPOSITE_DATAFORMAT_SETTING,
                IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_RESTRICT_SKIPLIST
            )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        setClusterServiceField(plugin, clusterService);
    }

    private static void setClusterServiceField(CompositeDataFormatPlugin plugin, ClusterService clusterService) {
        plugin.createComponents(null, clusterService, null, null, null, null, null, null, null, null, null);
    }
}
