/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataformatAwareLockableWriterPool;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Sandbox plugin that provides a {@link CompositeIndexingExecutionEngine} for
 * orchestrating multi-format indexing. Discovers {@link DataFormatPlugin} instances
 * during node bootstrap via the {@link ExtensiblePlugin} SPI and creates a composite
 * engine when composite indexing is enabled for an index.
 * <p>
 * Registers two index settings:
 * <ul>
 *   <li>{@code index.composite.enabled} — activates composite indexing (default {@code false})</li>
 *   <li>{@code index.composite.primary_data_format} — designates the primary format (default {@code "lucene"})</li>
 * </ul>
 * <p>
 * Format plugins (e.g., Parquet) extend this plugin by declaring
 * {@code extendedPlugins = ['composite-engine']} in their {@code build.gradle}
 * and implementing {@link DataFormatPlugin}. The {@link ExtensiblePlugin} SPI
 * discovers them automatically during node bootstrap.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeEnginePlugin extends Plugin implements ExtensiblePlugin, DataFormatPlugin {

    private static final Logger logger = LogManager.getLogger(CompositeEnginePlugin.class);

    /**
     * Index setting to enable composite indexing for an index.
     * When {@code true}, the composite engine orchestrates writes across all registered data formats.
     * Validates that the primary data format is non-empty when enabled.
     */
    public static final Setting<Boolean> COMPOSITE_ENABLED = Setting.boolSetting(
        "index.composite.enabled",
        false,
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {}

            @Override
            public void validate(Boolean enabled, Map<Setting<?>, Object> settings) {
                if (enabled) {
                    String primary = (String) settings.get(PRIMARY_DATA_FORMAT);
                    if (primary == null || primary.isEmpty()) {
                        throw new IllegalArgumentException(
                            "[index.composite.enabled] requires [index.composite.primary_data_format] to be set"
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(PRIMARY_DATA_FORMAT).iterator();
            }
        },
        Setting.Property.IndexScope
    );

    /**
     * Index setting that designates the primary data format for an index.
     * The primary format is the authoritative format used for merge operations.
     */
    public static final Setting<String> PRIMARY_DATA_FORMAT = Setting.simpleString(
        "index.composite.primary_data_format",
        "lucene",
        Setting.Property.IndexScope
    );

    /**
     * Index setting that lists the secondary data formats for an index.
     * Secondary formats receive writes alongside the primary but are not used
     * as the merge authority.
     */
    public static final Setting<List<String>> SECONDARY_DATA_FORMATS = Setting.listSetting(
        "index.composite.secondary_data_formats",
        Collections.emptyList(),
        s -> s,
        Setting.Property.IndexScope
    );

    /**
     * Discovered {@link DataFormatPlugin} instances keyed by format name.
     * When multiple plugins declare the same format name, the one with the highest
     * {@link DataFormat#priority()} is retained.
     */
    private volatile Map<String, DataFormatPlugin> dataFormatPlugins = Map.of();

    /** Creates a new composite engine plugin. */
    public CompositeEnginePlugin() {}

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        List<DataFormatPlugin> formatPlugins = loader.loadExtensions(DataFormatPlugin.class);
        Map<String, DataFormatPlugin> registry = new HashMap<>();
        for (DataFormatPlugin plugin : formatPlugins) {
            DataFormat format = plugin.getDataFormat();
            if (format == null) {
                logger.warn("DataFormatPlugin [{}] returned null DataFormat, skipping", plugin.getClass().getName());
                continue;
            }
            String name = format.name();
            if (name == null || name.isBlank()) {
                logger.warn("DataFormatPlugin [{}] returned a DataFormat with null/blank name, skipping", plugin.getClass().getName());
                continue;
            }
            DataFormatPlugin existing = registry.get(name);
            if (existing != null) {
                long existingPriority = existing.getDataFormat().priority();
                if (format.priority() <= existingPriority) {
                    logger.debug(
                        "Skipping DataFormatPlugin [{}] for format [{}] (priority {} <= existing {})",
                        plugin.getClass().getName(),
                        name,
                        format.priority(),
                        existingPriority
                    );
                    continue;
                }
                logger.info(
                    "Replacing DataFormatPlugin for format [{}] (priority {} > existing {})",
                    name,
                    format.priority(),
                    existingPriority
                );
            }
            registry.put(name, plugin);
            logger.info("Registered DataFormatPlugin [{}] for format [{}]", plugin.getClass().getName(), name);
        }
        this.dataFormatPlugins = Collections.unmodifiableMap(registry);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(COMPOSITE_ENABLED, PRIMARY_DATA_FORMAT, SECONDARY_DATA_FORMATS);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        Settings settings = indexModule.getSettings();
        boolean compositeEnabled = COMPOSITE_ENABLED.get(settings);
        if (compositeEnabled == false) {
            return;
        }

        String primaryFormatName = PRIMARY_DATA_FORMAT.get(settings);
        List<String> secondaryFormatNames = SECONDARY_DATA_FORMATS.get(settings);
        CompositeIndexingExecutionEngine.validateFormatsRegistered(dataFormatPlugins, primaryFormatName, secondaryFormatNames);
    }

    @Override
    public DataFormat getDataFormat() {
        // TODO: Dataformat for Composite is per index, while this one talks about cluster level. Switching it off for now
        return null;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(
        MapperService mapperService,
        ShardPath shardPath,
        IndexSettings indexSettings,
        @Nullable DataformatAwareLockableWriterPool<?> writerPool
    ) {
        Objects.requireNonNull(writerPool, "DataformatAwareLockableWriterPool is required for CompositeIndexingExecutionEngine");
        @SuppressWarnings("unchecked")
        DataformatAwareLockableWriterPool<CompositeWriter> compositeWriterPool = (DataformatAwareLockableWriterPool<
            CompositeWriter>) writerPool;
        return new CompositeIndexingExecutionEngine(dataFormatPlugins, indexSettings, mapperService, shardPath, compositeWriterPool);
    }

    /**
     * Returns the discovered data format plugins keyed by format name.
     *
     * @return unmodifiable map of format name to plugin
     */
    public Map<String, DataFormatPlugin> getDataFormatPlugins() {
        return dataFormatPlugins;
    }
}
