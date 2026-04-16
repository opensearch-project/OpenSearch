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
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

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
        MockDataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

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
        MockDataFormatPlugin plugin1 = MockDataFormatPlugin.of(format1);
        MockDataFormatPlugin plugin2 = MockDataFormatPlugin.of(format2);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format1.name(), format2.name()));

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
        MockDataFormatPlugin plugin1 = MockDataFormatPlugin.of(format);
        // Second plugin with same format name
        MockDataFormatPlugin plugin2 = MockDataFormatPlugin.of(new MockDataFormat("columnar", 200L, Set.of()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin1, plugin2));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new DataFormatRegistry(pluginsService));
        assertTrue(e.getMessage().contains("columnar"));
        assertTrue(e.getMessage().contains("already registered"));
    }

    public void testMismatchedFormatsAndReaderManagersAllowed() {
        // DataFormatPlugin and SearchBackEndPlugin may register different formats.
        // The registry no longer validates that they match — a format can have an
        // indexing engine without a reader manager (or vice-versa).
        MockDataFormat format1 = new MockDataFormat("columnar", 100L, Set.of());
        MockDataFormat format2 = new MockDataFormat("lucene", 50L, Set.of());
        MockDataFormatPlugin plugin1 = MockDataFormatPlugin.of(format1);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format2.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin1));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);
        assertEquals(1, registry.getRegisteredFormats().size());
    }

    public void testGetIndexingEngine() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockDataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        IndexingExecutionEngine<?, ?> engine = registry.getIndexingEngine(
            new IndexingEngineConfig(null, mapperService, indexSettings, null, null, Map.of()),
            format
        );
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
            () -> registry.getIndexingEngine(
                new IndexingEngineConfig(null, mapperService, indexSettings, null, null, Map.of()),
                unregistered
            )
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
        MockDataFormatPlugin plugin1 = MockDataFormatPlugin.of(lowPriority);
        MockDataFormatPlugin plugin2 = MockDataFormatPlugin.of(highPriority);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(lowPriority.name(), highPriority.name()));

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
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(columnar.name(), textSearch.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(
            List.of(MockDataFormatPlugin.of(columnar), MockDataFormatPlugin.of(textSearch))
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
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(MockDataFormatPlugin.of(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        assertEquals(1, registry.supportsCapability("integer", FieldTypeCapabilities.Capability.COLUMNAR_STORAGE).size());
        assertEquals(1, registry.supportsCapability("integer", FieldTypeCapabilities.Capability.POINT_RANGE).size());
        assertEquals(1, registry.supportsCapability("text", FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH).size());
        assertTrue(registry.supportsCapability("text", FieldTypeCapabilities.Capability.COLUMNAR_STORAGE).isEmpty());
    }

    public void testGetReaderManagers() throws IOException {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockDataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        Map<DataFormat, EngineReaderManager<?>> managers = registry.getReaderManager(
            new ReaderManagerConfig(Optional.empty(), format, registry, shardPath, Map.of())
        );
        assertEquals(1, managers.size());
        assertNotNull(managers.get(format));
    }

    public void testGetRegisteredFormatsIsUnmodifiable() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(MockDataFormatPlugin.of(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);
        Set<DataFormat> formats = registry.getRegisteredFormats();

        expectThrows(UnsupportedOperationException.class, () -> formats.add(new MockDataFormat("new", 1L, Set.of())));
    }

    public void testGetFormatDescriptorsByDataFormatReturnsDescriptors() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockDataFormatPlugin plugin = MockDataFormatPlugin.of(format);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of("columnar"));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        Map<String, Supplier<DataFormatDescriptor>> descriptors = registry.getFormatDescriptors(indexSettings, format);
        assertNotNull(descriptors);
    }

    public void testGetFormatDescriptorsByDataFormatReturnsEmptyForUnregisteredFormat() {
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of());
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of());

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);
        MockDataFormat unregistered = new MockDataFormat("unknown", 1L, Set.of());

        Map<String, Supplier<DataFormatDescriptor>> descriptors = registry.getFormatDescriptors(indexSettings, unregistered);
        assertTrue(descriptors.isEmpty());
    }

    public void testGetStoreStrategiesEmptyWhenNoPluggableDataformat() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(MockDataFormatPlugin.of(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        Map<DataFormat, StoreStrategy> result = registry.getStoreStrategies(indexSettings);
        assertTrue("Should return empty map when no pluggable_dataformat setting", result.isEmpty());
    }

    public void testGetStoreStrategiesEmptyWhenPluginReturnsNone() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(MockDataFormatPlugin.of(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .put("index.pluggable.dataformat", "columnar")
            .put("index.pluggable.dataformat.enabled", true)
            .build();
        IndexSettings settingsWithFormat = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);

        // MockDataFormatPlugin does not override getStoreStrategies, so the default returns
        // an empty map.
        Map<DataFormat, StoreStrategy> result = registry.getStoreStrategies(settingsWithFormat);
        assertTrue("Should return empty map when plugin provides no strategy", result.isEmpty());
    }

    public void testGetStoreStrategiesEmptyWhenFormatNameNotRegistered() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(MockDataFormatPlugin.of(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .put("index.pluggable.dataformat", "unknown")
            .put("index.pluggable.dataformat.enabled", true)
            .build();
        IndexSettings settingsWithFormat = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings);

        Map<DataFormat, StoreStrategy> result = registry.getStoreStrategies(settingsWithFormat);
        assertTrue("Should return empty map when format name not registered", result.isEmpty());
    }

    public void testGetPluginReturnsPluginForRegisteredFormat() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));
        MockDataFormatPlugin plugin = MockDataFormatPlugin.of(format);

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        DataFormatPlugin result = registry.getPlugin("columnar");
        assertNotNull("Should return plugin for registered format", result);
        assertSame("Should return the same plugin instance", plugin, result);
    }

    public void testGetPluginReturnsNullForUnknownFormat() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(MockDataFormatPlugin.of(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        assertNull("Should return null for unknown format", registry.getPlugin("unknown"));
    }

    public void testGetPluginReturnsNullForNullName() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(MockDataFormatPlugin.of(format)));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        assertNull("Should return empty map for null name", registry.getPlugin(null));
    }

    public void testGetDeleteExecutionEngineThrowsWhenMultiplePluginsProvide() {
        MockDataFormat format1 = new MockDataFormat("format1", 100L, Set.of());
        MockDataFormat format2 = new MockDataFormat("format2", 50L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format1.name(), format2.name()));

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(
            List.of(MockDataFormatPlugin.of(format1), MockDataFormatPlugin.of(format2))
        );
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.getDeleteExecutionEngine(mock(Committer.class)));
        assertTrue(e.getMessage().contains("Multiple DataFormatPlugins provide a DeleteExecutionEngine"));
    }

    public void testGetDeleteExecutionEngineThrowsWhenNoPluginProvides() {
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of());
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of());

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.getDeleteExecutionEngine(mock(Committer.class)));
        assertTrue(e.getMessage().contains("No DataFormatPlugin provides a DeleteExecutionEngine"));
    }

    public void testGetDeleteExecutionEngineSkipsPluginReturningNull() {
        MockDataFormat format = new MockDataFormat("columnar", 100L, Set.of());
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));

        DataFormatPlugin nullDeletePlugin = mock(DataFormatPlugin.class);
        when(nullDeletePlugin.getDataFormat()).thenReturn(format);
        when(nullDeletePlugin.getDeleteExecutionEngine(org.mockito.ArgumentMatchers.any())).thenReturn(null);

        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(nullDeletePlugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));

        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.getDeleteExecutionEngine(mock(Committer.class)));
        assertTrue(e.getMessage().contains("No DataFormatPlugin provides a DeleteExecutionEngine"));
    }

    // --- computeCapabilityMap tests ---

    private DataFormatRegistry createRegistryWith(MockDataFormat... formats) {
        List<DataFormatPlugin> plugins = new java.util.ArrayList<>();
        List<String> formatNames = new java.util.ArrayList<>();
        for (MockDataFormat f : formats) {
            plugins.add(MockDataFormatPlugin.of(f));
            formatNames.add(f.name());
        }
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(plugins);
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(new MockSearchBackEndPlugin(formatNames)));
        return new DataFormatRegistry(pluginsService);
    }

    public void testComputeCapabilityMapSingleFormat() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(
                new FieldTypeCapabilities(
                    "keyword",
                    Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER)
                )
            )
        );
        DataFormatRegistry registry = createRegistryWith(parquet);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = registry.computeCapabilityMap("keyword");

        assertEquals(1, capMap.size());
        assertTrue(capMap.containsKey(parquet));
        assertEquals(
            Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER),
            capMap.get(parquet)
        );
    }

    public void testComputeCapabilityMapHighestPriorityWins() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormat lucene = new MockDataFormat(
            "lucene",
            50L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        DataFormatRegistry registry = createRegistryWith(parquet, lucene);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = registry.computeCapabilityMap("keyword");

        assertTrue(capMap.containsKey(parquet));
        assertTrue(capMap.get(parquet).contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        assertFalse(capMap.containsKey(lucene));
    }

    public void testComputeCapabilityMapSplitAcrossFormats() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormat lucene = new MockDataFormat(
            "lucene",
            50L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)))
        );
        DataFormatRegistry registry = createRegistryWith(parquet, lucene);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = registry.computeCapabilityMap("keyword");

        assertEquals(2, capMap.size());
        assertEquals(Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE), capMap.get(parquet));
        assertEquals(Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH), capMap.get(lucene));
    }

    public void testComputeCapabilityMapUnknownFieldTypeReturnsEmpty() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        DataFormatRegistry registry = createRegistryWith(parquet);

        assertTrue(registry.computeCapabilityMap("unknown_type").isEmpty());
    }

    public void testComputeCapabilityMapEmptyRegistryReturnsEmpty() {
        DataFormatRegistry registry = createRegistryWith();
        assertTrue(registry.computeCapabilityMap("keyword").isEmpty());
    }

    public void testComputeCapabilityMapDefaultCapabilitiesFallback() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormat lucene = new MockDataFormat(
            "lucene",
            50L,
            Set.of(new FieldTypeCapabilities("text", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)))
        );
        DataFormatRegistry registry = createRegistryWith(parquet, lucene);

        // No format declares support for "_id", but defaults include FULL_TEXT_SEARCH + STORED_FIELDS
        Set<FieldTypeCapabilities.Capability> defaults = Set.of(
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH,
            FieldTypeCapabilities.Capability.STORED_FIELDS
        );
        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = registry.computeCapabilityMap("_id", defaults);

        // Both default capabilities assigned to highest-priority format (parquet, priority 0)
        assertEquals(1, capMap.size());
        assertTrue(capMap.containsKey(parquet));
        assertEquals(defaults, capMap.get(parquet));
    }

    public void testComputeCapabilityMapFormatDeclarationTakesPrecedenceOverDefaults() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        MockDataFormat lucene = new MockDataFormat(
            "lucene",
            50L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS)))
        );
        DataFormatRegistry registry = createRegistryWith(parquet, lucene);

        Set<FieldTypeCapabilities.Capability> defaults = Set.of(
            FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
            FieldTypeCapabilities.Capability.STORED_FIELDS
        );
        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = registry.computeCapabilityMap("keyword", defaults);

        // Format declarations win: COLUMNAR_STORAGE → parquet, STORED_FIELDS → lucene
        assertEquals(2, capMap.size());
        assertEquals(Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE), capMap.get(parquet));
        assertEquals(Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS), capMap.get(lucene));
    }

    public void testComputeCapabilityMapPartialDefaultFallback() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        DataFormatRegistry registry = createRegistryWith(parquet);

        // STORED_FIELDS not declared by any format, but is in defaults
        Set<FieldTypeCapabilities.Capability> defaults = Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS);
        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = registry.computeCapabilityMap("keyword", defaults);

        // COLUMNAR_STORAGE from format + STORED_FIELDS from default, both on parquet
        assertEquals(1, capMap.size());
        assertTrue(capMap.get(parquet).contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        assertTrue(capMap.get(parquet).contains(FieldTypeCapabilities.Capability.STORED_FIELDS));
    }

    public void testComputeCapabilityMapEmptyDefaultsNoFallback() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        DataFormatRegistry registry = createRegistryWith(parquet);

        assertTrue(registry.computeCapabilityMap("_id", Set.of()).isEmpty());
    }

    public void testComputeCapabilityMapResultIsImmutable() {
        MockDataFormat parquet = new MockDataFormat(
            "parquet",
            0L,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
        );
        DataFormatRegistry registry = createRegistryWith(parquet);

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = registry.computeCapabilityMap("keyword");
        expectThrows(UnsupportedOperationException.class, () -> capMap.put(parquet, Set.of()));
    }

    // --- MappedFieldType capability map tests ---

    public void testMappedFieldTypeCapabilityMapDefaultsToEmpty() {
        org.opensearch.index.mapper.MappedFieldType ft = new org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType("test");
        assertTrue(ft.getCapabilityMap().isEmpty());
    }

    public void testMappedFieldTypeSetAndGetCapabilityMap() {
        org.opensearch.index.mapper.MappedFieldType ft = new org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType("test");
        MockDataFormat parquet = new MockDataFormat("parquet", 0L, Set.of());

        ft.setCapabilityMap(
            Map.of(parquet, Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER))
        );

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> result = ft.getCapabilityMap();
        assertEquals(1, result.size());
        assertEquals(
            Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER),
            result.get(parquet)
        );
    }

    public void testMappedFieldTypeCapabilityMapIsUnmodifiable() {
        org.opensearch.index.mapper.MappedFieldType ft = new org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType("test");
        MockDataFormat parquet = new MockDataFormat("parquet", 0L, Set.of());

        ft.setCapabilityMap(Map.of(parquet, Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)));

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> result = ft.getCapabilityMap();
        expectThrows(UnsupportedOperationException.class, () -> result.put(parquet, Set.of()));
    }

    public void testMappedFieldTypeDefaultCapabilitiesIsEmpty() {
        org.opensearch.index.mapper.MappedFieldType ft = new org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType("test");
        assertTrue(ft.defaultCapabilities().isEmpty());
    }

    // --- requestedCapabilities tests ---

    public void testKeywordFieldTypeRequestedCapabilities() {
        org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType ft =
            new org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType("test");
        Set<FieldTypeCapabilities.Capability> caps = ft.requestedCapabilities();
        // keyword defaults: indexed=true, docValues=true, stored=false
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH));
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        assertFalse(caps.contains(FieldTypeCapabilities.Capability.STORED_FIELDS));
    }

    public void testNumberFieldTypeRequestedCapabilities() {
        org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType ft =
            new org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType(
                "test",
                org.opensearch.index.mapper.NumberFieldMapper.NumberType.LONG
            );
        Set<FieldTypeCapabilities.Capability> caps = ft.requestedCapabilities();
        // numeric fields use POINT_RANGE instead of FULL_TEXT_SEARCH
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.POINT_RANGE));
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        assertFalse(caps.contains(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH));
    }

    public void testDateFieldTypeRequestedCapabilities() {
        org.opensearch.index.mapper.DateFieldMapper.DateFieldType ft = new org.opensearch.index.mapper.DateFieldMapper.DateFieldType(
            "test"
        );
        Set<FieldTypeCapabilities.Capability> caps = ft.requestedCapabilities();
        // date fields use POINT_RANGE instead of FULL_TEXT_SEARCH
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.POINT_RANGE));
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        assertFalse(caps.contains(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH));
    }

    // --- metadata field defaultCapabilities tests ---

    public void testSeqNoFieldTypeDefaultCapabilities() {
        org.opensearch.index.mapper.MappedFieldType ft = new org.opensearch.index.mapper.SeqNoFieldMapper().fieldType();
        Set<FieldTypeCapabilities.Capability> caps = ft.defaultCapabilities();
        assertEquals(2, caps.size());
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.POINT_RANGE));
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
    }

    public void testVersionFieldTypeDefaultCapabilitiesBase() {
        // VersionFieldType is package-private; the override is tested in
        // VersionFieldMapperTests in the mapper package. Here we verify the
        // base MappedFieldType returns empty defaults.
        org.opensearch.index.mapper.MappedFieldType ft = new org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType("test");
        assertTrue(ft.defaultCapabilities().isEmpty());
    }
}
