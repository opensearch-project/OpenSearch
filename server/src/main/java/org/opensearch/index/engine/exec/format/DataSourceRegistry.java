/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.format;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.engine.IndexingConfiguration;
import org.opensearch.index.engine.exec.engine.IndexingExecutionEngine;
import org.opensearch.plugins.PluginsService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Registry to maintain all data formats available for use on a given node.
 */
@ExperimentalApi
public class DataSourceRegistry {

    private final Map<String, DataFormat> dataFormats = new ConcurrentHashMap<>();
    private final Map<DataFormat, Function<IndexingConfiguration, IndexingExecutionEngine<?>>> engines = new ConcurrentHashMap<>();

    public DataSourceRegistry(PluginsService pluginsService) {
        for (DataSourcePlugin plugin: pluginsService.filterPlugins(DataSourcePlugin.class)) {
            dataFormats.compute(plugin.getDataFormat().name(), (k, v) -> {
               if (v == null) {
                   return plugin.getDataFormat();
               }
               throw new IllegalStateException("data format [" + plugin.getDataFormat().name() + "] already registered");
            });
            engines.put(plugin.getDataFormat(), plugin::indexingEngine);
        }
    }

    /**
     * Returns the DataFormat object for a given identifier.
     */
    public DataFormat getDataFormat(String name) {
        if (dataFormats.containsKey(name) == false) {
            throw new IllegalStateException("data format [" + name + "] does not exist");
        }
        return dataFormats.get(name);
    }

    /**
     * Creates a new Engine instance for the data format.
     */
    public IndexingExecutionEngine<?> newEngine(DataFormat dataFormat, IndexingConfiguration indexingConfiguration) {
        if (engines.get(dataFormat) == null) {
            throw new IllegalStateException("data format [" + dataFormat.name() + "] not registered");
        }
        return engines.get(dataFormat).apply(indexingConfiguration);
    }
}
