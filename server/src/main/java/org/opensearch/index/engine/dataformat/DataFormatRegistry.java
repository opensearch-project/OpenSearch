/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    /** Index setting name that specifies the active pluggable data format. */
    public static final String PLUGGABLE_DATAFORMAT_SETTING = "pluggable_dataformat";

    /** Map from data format to the plugin that provides its indexing engine. */
    private final Map<DataFormat, DataFormatPlugin> dataFormatPluginRegistry;

    /** Map from data format to a factory that creates an {@link EngineReaderManager} for the given settings. */
    private final Map<DataFormat, CheckedFunction<ReaderManagerConfig, EngineReaderManager<?>, IOException>> readerManagerBuilders;

    private final Map<String, DataFormat> dataFormats;

    /**
     * Creates a registry by discovering all {@link DataFormatPlugin} and {@link SearchBackEndPlugin} implementations
     * from the given {@link PluginsService}. Registers each data format with its indexing plugin and reader manager factory.
     *
     * @param pluginsService the plugins service used to discover data format plugins and search back-end plugins
     * @throws IllegalArgumentException if a data format is registered by more than one plugin
     * @throws IllegalStateException if the set of formats with indexing plugins does not match the set with reader managers
     */
    public DataFormatRegistry(PluginsService pluginsService) {
        Map<DataFormat, DataFormatPlugin> dataFormatPlugiRegistry = new HashMap<>();
        Map<DataFormat, CheckedFunction<ReaderManagerConfig, EngineReaderManager<?>, IOException>> readerManagerBuilders = new HashMap<>();
        Map<String, DataFormat> dataFormats = new HashMap<>();

        for (DataFormatPlugin plugin : pluginsService.filterPlugins(DataFormatPlugin.class)) {
            DataFormat format = plugin.getDataFormat();
            DataFormatPlugin existing = dataFormatPlugiRegistry.putIfAbsent(format, plugin);
            if (existing != null) {
                throw new IllegalArgumentException("DataFormat [" + format.name() + "] is already registered by plugin [" + existing + "]");
            }
            dataFormats.put(format.name(), format);
        }

        for (SearchBackEndPlugin<?> plugin : pluginsService.filterPlugins(SearchBackEndPlugin.class)) {
            for (DataFormat format : plugin.getSupportedFormats()) {
                readerManagerBuilders.put(format, settings -> plugin.createReaderManager(settings));
            }
        }

        this.dataFormatPluginRegistry = Map.copyOf(dataFormatPlugiRegistry);
        this.dataFormats = Map.copyOf(dataFormats);
        this.readerManagerBuilders = Map.copyOf(readerManagerBuilders);
    }

    /**
     * Creates an {@link IndexingExecutionEngine} for the given data format.
     *
     * @param settings the engine initialization settings
     * @param format the data format
     * @return the indexing execution engine
     * @throws IllegalArgumentException if the data format is not registered
     */
    public IndexingExecutionEngine<?, ?> getIndexingEngine(IndexingEngineConfig settings, DataFormat format) {
        DataFormatPlugin plugin = dataFormatPluginRegistry.get(format);
        if (plugin == null) {
            throw new IllegalArgumentException("No plugin registered for DataFormat [" + format.name() + "]");
        }
        return plugin.indexingEngine(settings);
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
     *
     * @param fieldType the field type name
     * @param capability the capability to check
     * @return list of data formats supporting the capability for the field type
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
     * Returns an unmodifiable view of all registered data formats and their plugins.
     *
     * @return unmodifiable set of data formats
     */
    public Set<DataFormat> getRegisteredFormats() {
        return Set.copyOf(dataFormatPluginRegistry.keySet());
    }

    /**
     * Returns format descriptors for the active data format of the given index.
     * Resolves the data format from index settings via the {@code pluggable_dataformat} setting,
     * then delegates to {@link DataFormatPlugin#getFormatDescriptors(IndexSettings, DataFormatRegistry)}.
     *
     * @param indexSettings the index settings used to determine the active data format
     * @return unmodifiable map of format name to descriptor, or empty map if no pluggable data format is configured
     */
    public Map<String, DataFormatDescriptor> getFormatDescriptors(IndexSettings indexSettings) {
        String dataformatName = indexSettings.getSettings().get(PLUGGABLE_DATAFORMAT_SETTING);
        if (dataformatName != null) {
            DataFormat format = dataFormats.get(dataformatName);
            if (format != null) {
                DataFormatPlugin plugin = dataFormatPluginRegistry.get(format);
                if (plugin != null) {
                    return plugin.getFormatDescriptors(indexSettings, this);
                }
            }
        }
        return Map.of();
    }

    /**
     * Creates checksum strategies for all formats of the given index, intended to be called
     * once per shard during initialization. The returned map should be shared between the
     * directory and the engine so that pre-computed checksums registered during write are
     * visible to the upload path.
     *
     * @param indexSettings the index settings used to determine the active data format
     * @return unmodifiable map of format name to checksum strategy
     */
    public Map<String, FormatChecksumStrategy> createChecksumStrategies(IndexSettings indexSettings) {
        Map<String, DataFormatDescriptor> descriptors = getFormatDescriptors(indexSettings);
        Map<String, FormatChecksumStrategy> strategies = new HashMap<>();
        for (Map.Entry<String, DataFormatDescriptor> entry : descriptors.entrySet()) {
            strategies.put(entry.getKey(), entry.getValue().getChecksumStrategy());
        }
        return Collections.unmodifiableMap(strategies);
    }

    /**
     * Creates {@link EngineReaderManager} instances for all applicable data formats based on index settings/mappings.
     * Each reader manager is instantiated by applying the store provider and shard path to the factory registered
     * by the corresponding {@link SearchBackEndPlugin}.
     *
     * @param indexStoreProvider the store provider, or empty if not available
     * @param mapperService the mapper service for field mapping resolution (reserved for future filtering)
     * @param indexSettings the index settings (reserved for future filtering)
     * @param shardPath the shard path used to create reader managers
     * @return a map from data format to its reader manager
     * @throws IOException if reader manager creation fails
     */
    public Map<DataFormat, EngineReaderManager<?>> getReaderManagers(
        Optional<IndexStoreProvider> indexStoreProvider,
        MapperService mapperService,
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException {
        // TODO: Filter based on index settings
        Map<DataFormat, EngineReaderManager<?>> readerManagers = new HashMap<>();
        for (Map.Entry<DataFormat, CheckedFunction<ReaderManagerConfig, EngineReaderManager<?>, IOException>> entry : readerManagerBuilders
            .entrySet()) {
            ReaderManagerConfig settings = new ReaderManagerConfig(indexStoreProvider, entry.getKey(), shardPath);
            readerManagers.put(entry.getKey(), entry.getValue().apply(settings));
        }
        return readerManagers;
    }
}
