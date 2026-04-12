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
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    public void testGetDataFormatReturnsNull() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        assertNull(plugin.getDataFormat());
    }

    public void testLoadExtensionsRegistersPlugins() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatPlugin lucenePlugin = CompositeTestHelper.stubPlugin("lucene", 1);
        DataFormatPlugin parquetPlugin = CompositeTestHelper.stubPlugin("parquet", 2);

        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType == DataFormatPlugin.class) {
                    return (List<T>) List.of(lucenePlugin, parquetPlugin);
                }
                return Collections.emptyList();
            }
        });

        Map<String, DataFormatPlugin> plugins = plugin.getDataFormatPlugins();
        assertEquals(2, plugins.size());
        assertTrue(plugins.containsKey("lucene"));
        assertTrue(plugins.containsKey("parquet"));
    }

    public void testLoadExtensionsHigherPriorityWins() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatPlugin lowPriority = CompositeTestHelper.stubPlugin("lucene", 1);
        DataFormatPlugin highPriority = CompositeTestHelper.stubPlugin("lucene", 100);

        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType == DataFormatPlugin.class) {
                    return (List<T>) List.of(lowPriority, highPriority);
                }
                return Collections.emptyList();
            }
        });

        Map<String, DataFormatPlugin> plugins = plugin.getDataFormatPlugins();
        assertEquals(1, plugins.size());
        // The high priority one should win
        assertEquals(100, plugins.get("lucene").getDataFormat().priority());
    }

    public void testLoadExtensionsSkipsNullDataFormat() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        DataFormatPlugin nullPlugin = new DataFormatPlugin() {
            @Override
            public DataFormat getDataFormat() {
                return null;
            }

            @Override
            public org.opensearch.index.engine.dataformat.IndexingExecutionEngine<?, ?> indexingEngine(
                org.opensearch.index.engine.dataformat.IndexingEngineConfig settings,
                org.opensearch.index.store.FormatChecksumStrategy checksumStrategy
            ) {
                return null;
            }
        };

        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType == DataFormatPlugin.class) {
                    return (List<T>) List.of(nullPlugin);
                }
                return Collections.emptyList();
            }
        });

        assertTrue(plugin.getDataFormatPlugins().isEmpty());
    }

    public void testLoadExtensionsWithEmptyList() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return Collections.emptyList();
            }
        });

        assertTrue(plugin.getDataFormatPlugins().isEmpty());
    }

    public void testGetFormatDescriptorsDelegatestoPlugins() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();

        // Create a plugin that returns a descriptor
        DataFormatPlugin parquetPlugin = new DataFormatPlugin() {
            @Override
            public DataFormat getDataFormat() {
                return CompositeTestHelper.stubFormat("parquet", 2, java.util.Set.of());
            }

            @Override
            public org.opensearch.index.engine.dataformat.IndexingExecutionEngine<?, ?> indexingEngine(
                org.opensearch.index.engine.dataformat.IndexingEngineConfig settings,
                org.opensearch.index.store.FormatChecksumStrategy checksumStrategy
            ) {
                return null;
            }

            @Override
            public Map<String, org.opensearch.index.engine.dataformat.DataFormatDescriptor> getFormatDescriptors(
                IndexSettings indexSettings
            ) {
                return Map.of(
                    "parquet",
                    new org.opensearch.index.engine.dataformat.DataFormatDescriptor(
                        "parquet",
                        new org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler()
                    )
                );
            }
        };

        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType == DataFormatPlugin.class) {
                    return (List<T>) List.of(parquetPlugin);
                }
                return Collections.emptyList();
            }
        });

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

        Map<String, org.opensearch.index.engine.dataformat.DataFormatDescriptor> descriptors = plugin.getFormatDescriptors(indexSettings);
        assertEquals(1, descriptors.size());
        assertTrue(descriptors.containsKey("parquet"));
        assertEquals("parquet", descriptors.get("parquet").getFormatName());
    }

    public void testGetFormatDescriptorsEmptyWhenNoPluginsMatch() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return Collections.emptyList();
            }
        });

        Settings settings = Settings.builder()
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata = org.opensearch.cluster.metadata.IndexMetadata.builder("test-index")
            .settings(settings)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        Map<String, org.opensearch.index.engine.dataformat.DataFormatDescriptor> descriptors = plugin.getFormatDescriptors(indexSettings);
        assertTrue(descriptors.isEmpty());
    }

    public void testGetDataFormatPluginsReturnsUnmodifiableMap() {
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType == DataFormatPlugin.class) {
                    return (List<T>) List.of(CompositeTestHelper.stubPlugin("lucene", 1));
                }
                return Collections.emptyList();
            }
        });

        Map<String, DataFormatPlugin> plugins = plugin.getDataFormatPlugins();
        expectThrows(UnsupportedOperationException.class, () -> plugins.put("new", CompositeTestHelper.stubPlugin("new", 1)));
    }
}
