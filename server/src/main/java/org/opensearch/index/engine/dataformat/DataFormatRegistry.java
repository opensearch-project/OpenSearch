/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;

/**
 * Registry that holds the mapping from {@link DataFormat} to the {@link DataFormatPlugin} that provides it.
 * Provides methods to create indexing engines and query field-level capability support.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatRegistry {

    /** Map from data format to the plugin that provides its indexing engine. */
    private final Map<DataFormat, DataFormatPlugin> dataFormatPluginRegistry;

    /** Map from data format to a factory that creates an {@link EngineReaderManager} for the given settings. */
    private final Map<DataFormat, CheckedFunction<ReaderManagerConfig, EngineReaderManager<?>, IOException>> readerManagerBuilders;

    private final Map<String, DataFormat> dataFormats;

    private static final Logger logger = LogManager.getLogger(DataFormatRegistry.class);

    /** All capability values, cached to avoid repeated array allocation from {@code Capability.values()}. */
    private static final Set<FieldTypeCapabilities.Capability> ALL_CAPABILITIES = unmodifiableSet(
        EnumSet.allOf(FieldTypeCapabilities.Capability.class)
    );

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
            for (String formatName : plugin.getSupportedFormats()) {
                DataFormat format = dataFormats.get(formatName);
                if (format != null) {
                    readerManagerBuilders.put(format, settings -> plugin.createReaderManager(settings));
                }
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
     * Returns the plugin registered for the given format name, or {@code null} if not found.
     * Used by composite plugins to look up sub-format plugins directly without going through
     * the registry's top-level methods (which would cause infinite recursion).
     *
     * @param formatName the data format name (e.g., "parquet", "lucene")
     * @return the plugin, or null if no plugin is registered for the format
     */
    public DataFormatPlugin getPlugin(String formatName) {
        if (formatName == null) {
            return null;
        }
        DataFormat format = dataFormats.get(formatName);
        return format != null ? dataFormatPluginRegistry.get(format) : null;
    }

    /**
     * Returns all registered data formats that support a specific capability for a field type,
     * sorted by priority ascending (lowest priority value = highest preference).
     *
     * @param fieldType  the field type name
     * @param capability the capability to check
     * @return list of data formats supporting the capability, sorted by priority
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
     * Returns all registered data formats sorted by {@link DataFormat#priority()} ascending
     * (lowest priority value = highest preference). Used by capability coverage validation
     * to walk formats in primary-first order.
     *
     * @return list of data formats sorted by priority ascending
     */
    public List<DataFormat> getRegisteredFormatsByPriority() {
        return dataFormatPluginRegistry.keySet()
            .stream()
            .sorted(Comparator.comparingLong(DataFormat::priority))
            .collect(Collectors.toList());
    }

    /**
     * Computes the capability map for a given field type name across all registered data formats.
     * Convenience overload that passes no default or requested capabilities.
     *
     * @param fieldTypeName the field type name (e.g., "keyword", "long")
     * @return an immutable capability map for this field type
     * @see #computeCapabilityMap(String, Set, Set)
     */
    public Map<DataFormat, Set<FieldTypeCapabilities.Capability>> computeCapabilityMap(String fieldTypeName) {
        return computeCapabilityMap(fieldTypeName, Set.of(), ALL_CAPABILITIES);
    }

    /**
     * Computes the capability map for a given field type name, using the provided default
     * capabilities as a fallback when no registered data format declares support for a capability.
     * Considers all capabilities (no filtering by user request).
     *
     * @param fieldTypeName       the field type name (e.g., "keyword", "long", "_id")
     * @param defaultCapabilities fallback capabilities for metadata fields not declared by any format
     * @return an immutable capability map for this field type
     * @see #computeCapabilityMap(String, Set, Set)
     */
    public Map<DataFormat, Set<FieldTypeCapabilities.Capability>> computeCapabilityMap(
        String fieldTypeName,
        Set<FieldTypeCapabilities.Capability> defaultCapabilities
    ) {
        // Pass all capabilities as requested — the 2-arg overload means "consider everything, with defaults as fallback"
        return computeCapabilityMap(fieldTypeName, defaultCapabilities, ALL_CAPABILITIES);
    }

    /**
     * Computes the capability map for a given field type name, filtered to only the capabilities
     * that the user's mapping configuration actually requests.
     * <p>
     * The algorithm determines which capabilities to consider:
     * <ul>
     *   <li>If {@code requestedCapabilities} is non-empty, only those capabilities are considered</li>
     *   <li>If {@code requestedCapabilities} is empty (metadata fields), {@code defaultCapabilities} are used</li>
     *   <li>If both are empty, no capabilities are assigned</li>
     * </ul>
     * For each considered capability:
     * <ol>
     *   <li>Queries all registered formats for support via {@link #supportsCapability}</li>
     *   <li>If at least one format supports it, the highest-priority format (lowest priority value) wins</li>
     *   <li>If no format declares support but the capability is in {@code defaultCapabilities},
     *       it is assigned to the highest-priority registered format (primary)</li>
     * </ol>
     *
     * @param fieldTypeName         the field type name (e.g., "keyword", "long", "_id")
     * @param defaultCapabilities   fallback capabilities for metadata fields not declared by any format
     * @param requestedCapabilities capabilities the user's mapping requests; empty means use defaultCapabilities
     * @return an immutable capability map for this field type
     */
    public Map<DataFormat, Set<FieldTypeCapabilities.Capability>> computeCapabilityMap(
        String fieldTypeName,
        Set<FieldTypeCapabilities.Capability> defaultCapabilities,
        Set<FieldTypeCapabilities.Capability> requestedCapabilities
    ) {
        // Determine which capabilities to consider:
        // - Callers pass the specific set of capabilities to evaluate
        // - The 1-arg and 2-arg overloads pass all Capability.values()
        // - The 3-arg call from assignCapabilities passes requestedCapabilities from the field type
        // - If requestedCapabilities is empty, no capabilities are assigned (field doesn't need any)
        if (requestedCapabilities.isEmpty()) {
            return Map.of();
        }
        Set<FieldTypeCapabilities.Capability> capabilitiesToConsider = requestedCapabilities;

        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> result = new HashMap<>();

        for (FieldTypeCapabilities.Capability capability : capabilitiesToConsider) {
            List<DataFormat> supporters = supportsCapability(fieldTypeName, capability);
            if (supporters.isEmpty() == false) {
                // First match wins — list is sorted by priority ascending (lowest = highest priority)
                result.computeIfAbsent(supporters.get(0), k -> new HashSet<>()).add(capability);
            } else if (defaultCapabilities.contains(capability)) {
                // No format declares support, but the field type itself declares this as a default
                // capability. Assign to the highest-priority registered format.
                DataFormat primary = dataFormatPluginRegistry.keySet()
                    .stream()
                    .min(Comparator.comparingLong(DataFormat::priority))
                    .orElse(null);
                if (primary != null) {
                    result.computeIfAbsent(primary, k -> new HashSet<>()).add(capability);
                }
            }
        }

        return result.isEmpty() ? Map.of() : Map.copyOf(result);
    }

    /**
     * Returns all {@link StoreStrategy} instances that apply to the active
     * data format of the given index, keyed by the format name the strategy
     * applies to.
     *
     * <p>Called once per shard at open time. The store layer uses the returned
     * strategies to construct per-shard native file registries, seed them from
     * remote metadata, and route directory events.
     *
     * @param indexSettings the index settings for this shard
     * @return the map of applicable strategies, or an empty map when no
     *         pluggable data format is configured or the configured format
     *         does not participate in the tiered store
     */
    public Map<DataFormat, StoreStrategy> getStoreStrategies(IndexSettings indexSettings) {
        String dataformatName = indexSettings.pluggableDataFormat();
        if (dataformatName != null && dataformatName.isEmpty() == false) {
            DataFormat format = dataFormats.get(dataformatName);
            if (format != null) {
                DataFormatPlugin plugin = dataFormatPluginRegistry.get(format);
                if (plugin != null) {
                    Map<DataFormat, StoreStrategy> strategies = plugin.getStoreStrategies(indexSettings, this);
                    return strategies == null ? Map.of() : Map.copyOf(strategies);
                }
            }
        }
        return Map.of();
    }

    /**
     * Returns the data formats configured for use by the given index, in priority-walk order
     * (primary first, then secondaries by priority ascending). Resolves the active data format
     * plugin from index settings via the {@code pluggable_dataformat} setting and delegates to
     * {@link DataFormatPlugin#getConfiguredFormats(IndexSettings, DataFormatRegistry)}.
     *
     * <p>Used by capability coverage validation in
     * {@link org.opensearch.index.mapper.Mapper.BuilderContext#assignCapabilities} to scope
     * checks to formats that actually participate in this index, not every format registered
     * on the node.
     *
     * @param indexSettings the index settings used to determine the active data format plugin
     * @return ordered list of configured formats, or an empty list if no pluggable data format
     *         is configured
     */
    public List<DataFormat> getConfiguredFormats(IndexSettings indexSettings) {
        String dataformatName = indexSettings.pluggableDataFormat();
        if (dataformatName == null || dataformatName.isEmpty()) {
            return List.of();
        }
        DataFormat format = dataFormats.get(dataformatName);
        if (format == null) {
            return List.of();
        }
        DataFormatPlugin plugin = dataFormatPluginRegistry.get(format);
        if (plugin == null) {
            return List.of();
        }
        List<DataFormat> configured = plugin.getConfiguredFormats(indexSettings, this);
        return configured == null ? List.of() : List.copyOf(configured);
    }

    /**
     * Returns store strategies for a specific data format, bypassing the
     * {@code pluggable_dataformat} index setting lookup. Used by composite
     * plugins to resolve child strategies without recursion.
     *
     * @param indexSettings the index settings
     * @param dataFormat    the specific data format to get strategies for
     * @return map of data format to strategy, or empty map if the format is not registered
     */
    public Map<DataFormat, StoreStrategy> getStoreStrategies(IndexSettings indexSettings, DataFormat dataFormat) {
        DataFormatPlugin plugin = dataFormatPluginRegistry.get(dataFormat);
        if (plugin == null) {
            return Map.of();
        }
        Map<DataFormat, StoreStrategy> strategies = plugin.getStoreStrategies(indexSettings, this);
        return strategies == null ? Map.of() : strategies;
    }

    /**
     * Returns format descriptor suppliers for the active data format of the given index.
     * Resolves the data format from index settings via the {@code pluggable_dataformat} setting,
     * then delegates to {@link DataFormatPlugin#getFormatDescriptors(IndexSettings, DataFormatRegistry)}.
     * Callers that only need format names can use {@code keySet()} without triggering descriptor creation.
     *
     * @param indexSettings the index settings used to determine the active data format
     * @return map of format name to descriptor supplier, or empty map if no pluggable data format is configured
     */
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings) {
        String dataformatName = indexSettings.pluggableDataFormat();
        if (dataformatName != null && dataformatName.isEmpty() == false) {
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
     * Returns format descriptor suppliers for a specific data format, bypassing the
     * {@code pluggable_dataformat} index setting lookup. This is used by composite
     * plugins to resolve child format descriptors without recursion.
     *
     * @param indexSettings the index settings
     * @param dataFormat the specific data format to get descriptors for
     * @return map of format name to descriptor supplier, or empty map if the format is not registered
     */
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormat dataFormat) {
        DataFormatPlugin plugin = dataFormatPluginRegistry.get(dataFormat);
        if (plugin == null) {
            return Map.of();
        }
        return plugin.getFormatDescriptors(indexSettings, this);
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
        Map<String, Supplier<DataFormatDescriptor>> descriptors = getFormatDescriptors(indexSettings);
        Map<String, FormatChecksumStrategy> strategies = new HashMap<>();
        for (Map.Entry<String, Supplier<DataFormatDescriptor>> entry : descriptors.entrySet()) {
            FormatChecksumStrategy strategy = entry.getValue().get().getChecksumStrategy();
            if (strategy != null) {
                strategies.put(entry.getKey(), strategy);
            }
        }
        return Collections.unmodifiableMap(strategies);
    }

    /**
     * Creates {@link EngineReaderManager} instances for all applicable data formats based on index settings/mappings.
     * Each reader manager is instantiated by applying the store provider and shard path to the factory registered
     * by the corresponding {@link SearchBackEndPlugin}.
     *
     * @param readerManagerConfig config containing details about how to construct reader manager
     * @return a map from data format to its reader manager
     * @throws IOException if reader manager creation fails
     */
    public Map<DataFormat, EngineReaderManager<?>> getReaderManager(ReaderManagerConfig readerManagerConfig) throws IOException {
        if (!readerManagerBuilders.containsKey(readerManagerConfig.format())) {
            throw new IllegalArgumentException(
                "Unsupported format: ["
                    + readerManagerConfig.format()
                    + "]. Reader Manager can be built only for: "
                    + readerManagerBuilders.keySet()
            );
        }
        return Map.of(readerManagerConfig.format(), readerManagerBuilders.get(readerManagerConfig.format()).apply(readerManagerConfig));
    }

    /**
     * Returns the {@link DeleteExecutionEngine} by finding the single registered plugin that provides one.
     * Iterates over all registered data format plugins and validates that exactly one returns a non-null
     * result from {@link DataFormatPlugin#getDeleteExecutionEngine(Committer)}.
     *
     * @param committer the committer for durable delete tracking
     * @return the delete execution engine
     * @throws IllegalStateException if no plugin or multiple plugins provide a delete execution engine
     */
    public DeleteExecutionEngine<?> getDeleteExecutionEngine(Committer committer) {
        List<DeleteExecutionEngine<?>> engines = new ArrayList<>();
        for (DataFormatPlugin plugin : dataFormatPluginRegistry.values()) {
            DeleteExecutionEngine<?> engine = plugin.getDeleteExecutionEngine(committer);
            if (engine != null) {
                engines.add(engine);
            }
        }
        if (engines.size() > 1) {
            throw new IllegalStateException(
                "Multiple DataFormatPlugins provide a DeleteExecutionEngine, expected exactly one but found [" + engines.size() + "]"
            );
        }
        if (engines.isEmpty()) {
            throw new IllegalStateException("No DataFormatPlugin provides a DeleteExecutionEngine");
        }
        return engines.getFirst();
    }
}
