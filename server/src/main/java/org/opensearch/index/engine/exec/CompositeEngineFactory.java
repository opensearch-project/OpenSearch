/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.CheckedFunction;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CompositeEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory that discovers {@link SearchAnalyticsBackEndPlugin}s via
 * {@link PluginsService} and builds the per-format reader managers and
 * memoizing suppliers consumed by {@link CompositeEngine}.
 * <p>
 * This keeps CompositeEngine decoupled from the plugin layer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeEngineFactory {

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers = new HashMap<>();
    private final Map<DataFormat, CheckedSupplier<SearchExecEngine<?, ?>, IOException>> engineSuppliers = new HashMap<>();
    private final Map<DataFormat, CheckedSupplier<IndexFilterProvider<?, ?>, IOException>> indexFilterProviderSuppliers = new HashMap<>();
    private final Map<DataFormat, CheckedSupplier<SourceProvider<?, ?>, IOException>> sourceProviderSuppliers = new HashMap<>();

    public CompositeEngineFactory(
        PluginsService pluginsService,
        ShardPath shardPath,
        MapperService mapperService,
        IndexSettings indexSettings
    ) throws IOException {
        for (SearchAnalyticsBackEndPlugin plugin : pluginsService.filterPlugins(SearchAnalyticsBackEndPlugin.class)) {
            for (DataFormat format : plugin.getSupportedFormats()) {
                // TODO: use mapperService and indexSettings to filter formats relevant to this index
                readerManagers.put(format, plugin.createReaderManager(format, shardPath));
                engineSuppliers.put(format, memoize(format, f -> plugin.createSearchExecEngine(f, shardPath)));
                indexFilterProviderSuppliers.put(format, memoize(format, f -> plugin.createIndexFilterProvider(f, shardPath)));
                sourceProviderSuppliers.put(format, memoize(format, f -> plugin.createSourceProvider(f, shardPath)));
            }
        }
    }

    /**
     * Wraps a {@link CheckedFunction} factory into a thread-safe memoizing supplier
     * using double-checked locking. The factory is invoked at most once.
     */
    private static <T> CheckedSupplier<T, IOException> memoize(DataFormat format, CheckedFunction<DataFormat, T, IOException> factory) {
        return new CheckedSupplier<>() {
            private volatile T instance;

            @Override
            public T get() throws IOException {
                T result = instance;
                if (result != null) {
                    return result;
                }
                synchronized (this) {
                    result = instance;
                    if (result != null) {
                        return result;
                    }
                    result = factory.apply(format);
                    instance = result;
                    return result;
                }
            }
        };
    }

    /**
     * Creates a new {@link CompositeEngine} populated with the discovered
     * reader managers and memoizing suppliers.
     */
    public CompositeEngine create() {
        return new CompositeEngine(readerManagers, engineSuppliers, indexFilterProviderSuppliers, sourceProviderSuppliers);
    }
}
