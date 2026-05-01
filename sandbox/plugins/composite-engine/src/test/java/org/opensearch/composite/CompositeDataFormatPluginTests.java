/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
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

        // Build index settings with parquet as secondary
        Settings settings = Settings.builder()
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats", "parquet")
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder("test-index")
            .settings(settings)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        DataFormat parquetFormat = CompositeTestHelper.stubFormat("parquet", 2, java.util.Set.of());
        when(registry.format("parquet")).thenReturn(parquetFormat);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, java.util.Set.of()));
        when(registry.getFormatDescriptors(indexSettings, parquetFormat)).thenReturn(
            Map.of(
                "parquet",
                (Supplier<
                    org.opensearch.index.engine.dataformat.DataFormatDescriptor>) () -> new org.opensearch.index.engine.dataformat.DataFormatDescriptor(
                        "parquet",
                        new org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler()
                    )
            )
        );

        Map<String, Supplier<org.opensearch.index.engine.dataformat.DataFormatDescriptor>> descriptors = plugin.getFormatDescriptors(
            indexSettings,
            registry
        );
        assertEquals(1, descriptors.size());
        assertTrue(descriptors.containsKey("parquet"));
        assertEquals("parquet", descriptors.get("parquet").get().getFormatName());
    }

    public void testGetFormatDescriptorsEmptyWhenNoPluginsMatch() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        Settings settings = Settings.builder()
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder("test-index")
            .settings(settings)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        Map<String, Supplier<org.opensearch.index.engine.dataformat.DataFormatDescriptor>> descriptors = plugin.getFormatDescriptors(
            indexSettings,
            registry
        );
        assertTrue(descriptors.isEmpty());
    }

    public void testGetDataFormatAwareStoreHandlerReturnsNullWhenNoSubPlugins() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        Settings settings = Settings.builder()
            .put("index.composite.primary_data_format", "parquet")
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder("test-index")
            .settings(settings)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        // No plugin registered for "parquet"
        when(registry.getPlugin("parquet")).thenReturn(null);

        java.util.Map<DataFormat, org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler> result = plugin
            .getDataFormatAwareStoreHandlers(indexSettings, registry);
        assertTrue("Should return empty when no sub-plugin found", result.isEmpty());
    }

    public void testGetDataFormatAwareStoreHandlerDelegatesToSubPlugin() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        Settings settings = Settings.builder()
            .put("index.composite.primary_data_format", "parquet")
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder("test-index")
            .settings(settings)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        // Mock a sub-plugin that provides a handler
        org.opensearch.index.engine.dataformat.DataFormatPlugin subPlugin = mock(
            org.opensearch.index.engine.dataformat.DataFormatPlugin.class
        );
        org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler mockHandler = mock(
            org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler.class
        );
        DataFormat mockDataFormat = mock(DataFormat.class);
        when(subPlugin.getDataFormatAwareStoreHandlers(indexSettings, registry)).thenReturn(java.util.Map.of(mockDataFormat, mockHandler));
        when(registry.getPlugin("parquet")).thenReturn(subPlugin);

        java.util.Map<DataFormat, org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler> result = plugin
            .getDataFormatAwareStoreHandlers(indexSettings, registry);
        assertNotNull("Should return handler from sub-plugin", result);
        assertEquals(1, result.size());
        assertSame(mockHandler, result.get(mockDataFormat));
    }

    public void testGetDataFormatAwareStoreHandlerReturnsNullForDefaultPrimary() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        // Default primary is "lucene", no sub-plugin registered
        Settings settings = Settings.builder()
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder("test-index")
            .settings(settings)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        when(registry.getPlugin("lucene")).thenReturn(null);

        java.util.Map<DataFormat, org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler> result = plugin
            .getDataFormatAwareStoreHandlers(indexSettings, registry);
        assertTrue("Should return empty when lucene sub-plugin not found", result.isEmpty());
    }
}
