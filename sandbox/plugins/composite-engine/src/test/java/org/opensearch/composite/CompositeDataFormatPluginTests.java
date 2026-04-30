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
}
