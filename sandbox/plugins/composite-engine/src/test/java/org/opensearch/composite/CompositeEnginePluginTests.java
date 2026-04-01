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
 * Tests for {@link CompositeEnginePlugin}.
 */
public class CompositeEnginePluginTests extends OpenSearchTestCase {

    public void testGetSettingsReturnsBothSettings() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        List<Setting<?>> settings = plugin.getSettings();
        assertEquals(2, settings.size());
        assertTrue(settings.contains(CompositeEnginePlugin.PRIMARY_DATA_FORMAT));
        assertTrue(settings.contains(CompositeEnginePlugin.SECONDARY_DATA_FORMATS));
    }

    public void testPrimaryDataFormatDefaultsToLucene() {
        Settings settings = Settings.builder().build();
        assertEquals("lucene", CompositeEnginePlugin.PRIMARY_DATA_FORMAT.get(settings));
    }

    public void testSecondaryDataFormatsDefaultsToEmpty() {
        Settings settings = Settings.builder().build();
        assertTrue(CompositeEnginePlugin.SECONDARY_DATA_FORMATS.get(settings).isEmpty());
    }

    public void testGetDataFormatReturnsNull() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        assertNull(plugin.getDataFormat());
    }

    public void testLoadExtensionsRegistersPlugins() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
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
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
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
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        DataFormatPlugin nullPlugin = new DataFormatPlugin() {
            @Override
            public DataFormat getDataFormat() {
                return null;
            }

            @Override
            public org.opensearch.index.engine.dataformat.IndexingExecutionEngine<?, ?> indexingEngine(
                org.opensearch.index.mapper.MapperService mapperService,
                org.opensearch.index.shard.ShardPath shardPath,
                IndexSettings indexSettings
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
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return Collections.emptyList();
            }
        });

        assertTrue(plugin.getDataFormatPlugins().isEmpty());
    }

    public void testGetDataFormatPluginsReturnsUnmodifiableMap() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
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
