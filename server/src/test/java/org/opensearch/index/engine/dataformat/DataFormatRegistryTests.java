/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFormatRegistryTests extends OpenSearchTestCase {

    private PluginsService pluginsService;
    private MapperService mapperService;
    private ShardPath shardPath;
    private IndexSettings indexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        pluginsService = mock(PluginsService.class);
        mapperService = mock(MapperService.class);
        shardPath = new ShardPath(false, Path.of("/tmp/uuid/0"), Path.of("/tmp/uuid/0"), new ShardId("index", "uuid", 0));
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .build();
        indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);
    }

    public void testConstructionWithSinglePlugin() {
        MockDataFormat format = new MockDataFormat(
            "columnar",
            100L,
            Set.of(new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormatPlugin plugin = new MockDataFormatPlugin(format);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        Set<DataFormat> registered = registry.getRegisteredFormats();
        assertEquals(1, registered.size());
        assertTrue(registered.contains(format));
    }

    public void testConstructionWithMultiplePlugins() {
        MockDataFormat format1 = new MockDataFormat(
            "columnar",
            100L,
            Set.of(new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormat format2 = new MockDataFormat(
            "lucene",
            50L,
            Set.of(new FieldTypeCapabilities("text", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)))
        );
        MockDataFormatPlugin plugin1 = new MockDataFormatPlugin(format1);
        MockDataFormatPlugin plugin2 = new MockDataFormatPlugin(format2);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format1, format2));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin1, plugin2));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        assertEquals(2, registry.getRegisteredFormats().size());
    }

    public void testConstructionWithEmptyPlugins() {
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of());
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of());

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        assertTrue(registry.getRegisteredFormats().isEmpty());
    }

    public void testDuplicateDataFormatThrows() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockDataFormatPlugin plugin1 = new MockDataFormatPlugin(format);
        // Second plugin with same format name
        MockDataFormatPlugin plugin2 = new MockDataFormatPlugin(new MockDataFormat("columnar", 200L, Set.of()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin1, plugin2));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new DataFormatRegistry(pluginsService));
        assertTrue(e.getMessage().contains("columnar"));
        assertTrue(e.getMessage().contains("already registered"));
    }

    public void testMismatchedFormatsAndReaderManagersThrows() {
        MockDataFormat format1 = new MockDataFormat("columnar", 100L, Set.of());
        MockDataFormat format2 = new MockDataFormat("lucene", 50L, Set.of());
        MockDataFormatPlugin plugin1 = new MockDataFormatPlugin(format1);
        // Only register reader manager for format2, not format1
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format2));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin1));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new DataFormatRegistry(pluginsService));
        assertTrue(e.getMessage().contains("missing indexing engine/reader managers"));
    }

    public void testGetIndexingEngine() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockDataFormatPlugin plugin = new MockDataFormatPlugin(format);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        IndexingExecutionEngine<?, ?> engine = registry.getIndexingEngine(null, format, mapperService, shardPath, indexSettings);
        assertNotNull(engine);
        assertEquals(format, engine.getDataFormat());
    }

    public void testGetIndexingEngineForUnregisteredFormatThrows() {
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of());
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of());

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);
        MockDataFormat unregistered = new MockDataFormat("unknown", 1L, Set.of());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.getIndexingEngine(null, unregistered, mapperService, shardPath, indexSettings)
        );
        assertTrue(e.getMessage().contains("unknown"));
    }

    public void testSupportsCapabilityReturnsSortedByPriority() {
        MockDataFormat lowPriority = new MockDataFormat(
            "format-low",
            10L,
            Set.of(new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormat highPriority = new MockDataFormat(
            "format-high",
            100L,
            Set.of(new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormatPlugin plugin1 = new MockDataFormatPlugin(lowPriority);
        MockDataFormatPlugin plugin2 = new MockDataFormatPlugin(highPriority);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(lowPriority, highPriority));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin1, plugin2));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        List<DataFormat> result = registry.supportsCapability("integer", FieldTypeCapabilities.Capability.COLUMNAR_STORAGE);
        assertEquals(2, result.size());
        assertEquals("format-low", result.get(0).name());
        assertEquals("format-high", result.get(1).name());
    }

    public void testSupportsCapabilityFiltersCorrectly() {
        MockDataFormat columnar = new MockDataFormat(
            "columnar",
            100L,
            Set.of(new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormat textSearch = new MockDataFormat(
            "lucene",
            50L,
            Set.of(new FieldTypeCapabilities("text", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)))
        );
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(columnar, textSearch));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(
            List.of(new MockDataFormatPlugin(columnar), new MockDataFormatPlugin(textSearch))
        );
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        // Only columnar supports integer + COLUMNAR_STORAGE
        List<DataFormat> columnarResults = registry.supportsCapability("integer", FieldTypeCapabilities.Capability.COLUMNAR_STORAGE);
        assertEquals(1, columnarResults.size());
        assertEquals("columnar", columnarResults.get(0).name());

        // Only lucene supports text + FULL_TEXT_SEARCH
        List<DataFormat> textResults = registry.supportsCapability("text", FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH);
        assertEquals(1, textResults.size());
        assertEquals("lucene", textResults.get(0).name());

        // No format supports integer + FULL_TEXT_SEARCH
        List<DataFormat> noResults = registry.supportsCapability("integer", FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH);
        assertTrue(noResults.isEmpty());

        // No format supports unknown field type
        List<DataFormat> unknownField = registry.supportsCapability("unknown_type", FieldTypeCapabilities.Capability.COLUMNAR_STORAGE);
        assertTrue(unknownField.isEmpty());
    }

    public void testSupportsCapabilityWithMultipleCapabilitiesPerFormat() {
        MockDataFormat format = new MockDataFormat(
            "multi-cap",
            100L,
            Set.of(
                new FieldTypeCapabilities(
                    "integer",
                    Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.POINT_RANGE)
                ),
                new FieldTypeCapabilities("text", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH))
            )
        );
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(new MockDataFormatPlugin(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        assertEquals(1, registry.supportsCapability("integer", FieldTypeCapabilities.Capability.COLUMNAR_STORAGE).size());
        assertEquals(1, registry.supportsCapability("integer", FieldTypeCapabilities.Capability.POINT_RANGE).size());
        assertEquals(1, registry.supportsCapability("text", FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH).size());
        assertTrue(registry.supportsCapability("text", FieldTypeCapabilities.Capability.COLUMNAR_STORAGE).isEmpty());
    }

    public void testGetReaderManagers() throws IOException {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockDataFormatPlugin plugin = new MockDataFormatPlugin(format);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        Map<DataFormat, EngineReaderManager<?>> managers = registry.getReaderManagers(null, mapperService, indexSettings, shardPath);
        assertEquals(1, managers.size());
        assertNotNull(managers.get(format));
    }

    public void testGetRegisteredFormatsIsUnmodifiable() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(new MockDataFormatPlugin(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);
        Set<DataFormat> formats = registry.getRegisteredFormats();

        expectThrows(UnsupportedOperationException.class, () -> formats.add(new MockDataFormat("new", 1L, Set.of())));
    }
}
