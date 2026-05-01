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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Sandbox plugin that provides a {@link CompositeIndexingExecutionEngine} for
 * orchestrating multi-format indexing. Discovers {@link DataFormatPlugin}
 * instances during node bootstrap via the {@link ExtensiblePlugin} SPI and
 * creates a composite engine when composite indexing is enabled for an index.
 *
 * <p>Registers two index settings:
 * <ul>
 *   <li>{@code index.composite.primary_data_format} — designates the primary
 *       format (default {@code "lucene"})</li>
 *   <li>{@code index.composite.secondary_data_formats} — lists the secondary
 *       formats (default empty)</li>
 * </ul>
 *
 * <p>Format plugins (e.g., Parquet) extend this plugin by declaring
 * {@code extendedPlugins = ['composite-engine']} in their {@code build.gradle}
 * and implementing {@link DataFormatPlugin}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDataFormatPlugin extends Plugin implements DataFormatPlugin {

    private static final Logger logger = LogManager.getLogger(CompositeDataFormatPlugin.class);

    /**
     * Index setting that designates the primary data format for an index.
     * The primary format is the authoritative format used for merge operations.
     */
    public static final Setting<String> PRIMARY_DATA_FORMAT = Setting.simpleString(
        "index.composite.primary_data_format",
        "lucene",
        Setting.Property.IndexScope,
        Setting.Property.Final
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
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public CompositeDataFormatPlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(PRIMARY_DATA_FORMAT, SECONDARY_DATA_FORMATS);
    }

    @Override
    public DataFormat getDataFormat() {
        return new CompositeDataFormat();
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
        return new CompositeIndexingExecutionEngine(
            settings.indexSettings(),
            settings.mapperService(),
            settings.committer(),
            settings.registry(),
            settings.store(),
            settings.checksumStrategies()
        );
    }

    @Override
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(
        IndexSettings indexSettings,
        DataFormatRegistry dataFormatRegistry
    ) {
        Settings settings = indexSettings.getSettings();
        String primaryFormatName = PRIMARY_DATA_FORMAT.get(settings);
        List<String> secondaryFormatNames = SECONDARY_DATA_FORMATS.get(settings);

        Map<String, Supplier<DataFormatDescriptor>> descriptors = new HashMap<>();
        if (primaryFormatName != null) {
            descriptors.putAll(dataFormatRegistry.getFormatDescriptors(indexSettings, dataFormatRegistry.format(primaryFormatName)));
        }
        for (String secondaryName : secondaryFormatNames) {
            if (secondaryName != null) {
                descriptors.putAll(dataFormatRegistry.getFormatDescriptors(indexSettings, dataFormatRegistry.format(secondaryName)));
            }
        }
        return Map.copyOf(descriptors);
    }

    /**
     * Returns the store strategies from every participating sub-format plugin
     * (primary + secondary), keyed by format name. Mirrors {@link #getFormatDescriptors}:
     * each participating format is resolved through the registry, which delegates
     * to the sub-plugin without re-entering this composite.
     */
    @Override
    public Map<String, StoreStrategy> getStoreStrategies(IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        Settings settings = indexSettings.getSettings();
        String primaryFormatName = PRIMARY_DATA_FORMAT.get(settings);
        List<String> secondaryFormatNames = SECONDARY_DATA_FORMATS.get(settings);

        Map<String, StoreStrategy> strategies = new HashMap<>();
        if (primaryFormatName != null && primaryFormatName.isEmpty() == false) {
            strategies.putAll(dataFormatRegistry.getStoreStrategies(indexSettings, dataFormatRegistry.format(primaryFormatName)));
        }
        for (String secondaryName : secondaryFormatNames) {
            if (secondaryName != null && secondaryName.isEmpty() == false) {
                strategies.putAll(dataFormatRegistry.getStoreStrategies(indexSettings, dataFormatRegistry.format(secondaryName)));
            }
        }
        return Map.copyOf(strategies);
    }
}
