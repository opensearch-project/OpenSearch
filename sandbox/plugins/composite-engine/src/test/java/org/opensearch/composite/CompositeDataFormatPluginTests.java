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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompositeDataFormatPlugin}.
 */
public class CompositeDataFormatPluginTests extends OpenSearchTestCase {

    public void testGetSettingsReturnsBothSettings() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        List<Setting<?>> settings = plugin.getSettings();
        assertEquals(2, settings.size());
        assertTrue(settings.contains(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT));
        assertTrue(settings.contains(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS));
    }

    public void testPrimaryDataFormatDefaultsToLucene() {
        Settings settings = Settings.builder().build();
        assertEquals("lucene", CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.get(settings));
    }

    public void testSecondaryDataFormatsDefaultsToEmpty() {
        Settings settings = Settings.builder().build();
        assertTrue(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.get(settings).isEmpty());
    }

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
        // Registry resolves the format but the composite pipeline finds no plugin → empty map.
        DataFormat parquetFormat = CompositeTestHelper.stubFormat("parquet", 2, java.util.Set.of());
        when(registry.format("parquet")).thenReturn(parquetFormat);
        when(registry.getStoreStrategies(indexSettings, parquetFormat)).thenReturn(Map.of());

        Map<String, StoreStrategy> result = plugin.getStoreStrategies(indexSettings, registry);
        assertTrue("Should return empty when no sub-plugin found", result.isEmpty());
    }

    public void testGetStoreStrategiesCollectsFromPrimaryPlugin() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IndexSettings indexSettings = buildIndexSettings(Settings.builder().put("index.composite.primary_data_format", "parquet").build());

        DataFormat parquetFormat = CompositeTestHelper.stubFormat("parquet", 2, java.util.Set.of());
        StoreStrategy parquetStrategy = mock(StoreStrategy.class);
        when(registry.format("parquet")).thenReturn(parquetFormat);
        when(registry.getStoreStrategies(indexSettings, parquetFormat)).thenReturn(Map.of("parquet", parquetStrategy));

        Map<String, StoreStrategy> result = plugin.getStoreStrategies(indexSettings, registry);
        assertEquals(1, result.size());
        assertSame(parquetStrategy, result.get("parquet"));
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
        when(registry.getStoreStrategies(indexSettings, parquetFormat)).thenReturn(Map.of("parquet", parquetStrategy));

        Map<String, StoreStrategy> result = plugin.getStoreStrategies(indexSettings, registry);
        assertEquals(1, result.size());
        assertSame(parquetStrategy, result.get("parquet"));
    }

    public void testGetStoreStrategiesEmptyForDefaultPrimaryWithoutPlugin() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IndexSettings indexSettings = buildIndexSettings(Settings.EMPTY); // defaults: primary=lucene, secondary=[]
        DataFormat luceneFormat = CompositeTestHelper.stubFormat("lucene", 1, java.util.Set.of());
        when(registry.format("lucene")).thenReturn(luceneFormat);
        when(registry.getStoreStrategies(indexSettings, luceneFormat)).thenReturn(Map.of());

        Map<String, StoreStrategy> result = plugin.getStoreStrategies(indexSettings, registry);
        assertTrue("Should return empty when lucene sub-plugin not found", result.isEmpty());
    }

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
}
