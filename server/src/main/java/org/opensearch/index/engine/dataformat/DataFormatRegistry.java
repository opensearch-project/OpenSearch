/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Registry that holds the mapping from {@link DataFormat} to the {@link DataFormatPlugin} that provides it.
 * Provides methods to create indexing engines and query field-level capability support.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatRegistry {

    private final Map<DataFormat, DataFormatPlugin> dataFormatPluginRegistry;
    private final Map<DataFormat, SearchBackEndPlugin<?>> readerManagerPlugins;
    private final Map<String, DataFormat> dataFormats;

    /**
     * Creates a registry by discovering all {@link DataFormatPlugin} and {@link SearchBackEndPlugin} implementations
     * from the given {@link PluginsService}.
     *
     * @param pluginsService the plugins service used to discover data format plugins and search back-end plugins
     */
    public DataFormatRegistry(PluginsService pluginsService) {
        Map<DataFormat, DataFormatPlugin> pluginRegistry = new HashMap<>();
        Map<DataFormat, SearchBackEndPlugin<?>> readerPlugins = new HashMap<>();
        Map<String, DataFormat> formats = new HashMap<>();

        for (DataFormatPlugin plugin : pluginsService.filterPlugins(DataFormatPlugin.class)) {
            DataFormat format = plugin.getDataFormat();
            DataFormatPlugin existing = pluginRegistry.putIfAbsent(format, plugin);
            if (existing != null) {
                throw new IllegalArgumentException("DataFormat [" + format.name() + "] is already registered by plugin [" + existing + "]");
            }
            formats.put(format.name(), format);
        }

        for (SearchBackEndPlugin<?> plugin : pluginsService.filterPlugins(SearchBackEndPlugin.class)) {
            for (DataFormat format : plugin.getSupportedFormats()) {
                readerPlugins.put(format, plugin);
            }
        }

        if (readerPlugins.keySet().equals(pluginRegistry.keySet()) == false) {
            throw new IllegalStateException(
                "Cannot build registry as data formats have missing indexing engine/reader managers"
                    + " - formats with reader managers: "
                    + readerPlugins.keySet()
                    + ", formats with plugins: "
                    + pluginRegistry.keySet()
            );
        }

        this.dataFormatPluginRegistry = Map.copyOf(pluginRegistry);
        this.dataFormats = Map.copyOf(formats);
        this.readerManagerPlugins = Map.copyOf(readerPlugins);
    }

    /**
     * Creates an {@link IndexingExecutionEngine} for the given data format.
     *
     * @param committer the committer holding the backing store, or null if not available
     * @param format the data format
     * @param mapperService the mapper service for field mapping resolution
     * @param shardPath the shard path for file storage
     * @param indexSettings the index settings
     * @return the indexing execution engine
     * @throws IllegalArgumentException if the data format is not registered
     */
    public IndexingExecutionEngine<?, ?> getIndexingEngine(
        Committer committer,
        DataFormat format,
        MapperService mapperService,
        ShardPath shardPath,
        IndexSettings indexSettings
    ) {
        DataFormatPlugin plugin = dataFormatPluginRegistry.get(format);
        if (plugin == null) {
            throw new IllegalArgumentException("No plugin registered for DataFormat [" + format.name() + "]");
        }
        return plugin.indexingEngine(committer, mapperService, shardPath, indexSettings);
    }

    public DataFormat format(String name) {
        DataFormat format = dataFormats.get(name);
        if (format == null) {
            throw new IllegalArgumentException("No data format registered with name [" + name + "]");
        }
        return format;
    }

    /**
     * Returns all registered data formats that support a specific capability for a field type.
     */
    public List<DataFormat> supportsCapability(String fieldType, FieldTypeCapabilities.Capability capability) {
        return dataFormatPluginRegistry.keySet()
            .stream()
            .filter(
                format -> format.supportedFields()
                    .stream()
                    .anyMatch(ftc -> ftc.fieldType().equals(fieldType) && ftc.capabilities().contains(capability))
            )
            .sorted(Comparator.comparingLong(DataFormat::priority))
            .collect(Collectors.toList());
    }

    /**
     * Returns an unmodifiable view of all registered data formats.
     */
    public Set<DataFormat> getRegisteredFormats() {
        return Set.copyOf(dataFormatPluginRegistry.keySet());
    }

    /**
     * Creates {@link EngineReaderManager} instances for all applicable data formats.
     *
     * @param committer the committer holding the backing store, or null if not available
     * @param mapperService the mapper service (reserved for future filtering)
     * @param indexSettings the index settings (reserved for future filtering)
     * @param shardPath the shard path used to create reader managers
     * @return a map from data format to its reader manager
     * @throws IOException if reader manager creation fails
     */
    public Map<DataFormat, EngineReaderManager<?>> getReaderManagers(
        Committer committer,
        MapperService mapperService,
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException {
        Map<DataFormat, EngineReaderManager<?>> readerManagers = new HashMap<>();
        for (Map.Entry<DataFormat, SearchBackEndPlugin<?>> entry : readerManagerPlugins.entrySet()) {
            readerManagers.put(entry.getKey(), entry.getValue().createReaderManager(committer, entry.getKey(), shardPath));
        }
        return readerManagers;
    }
}
