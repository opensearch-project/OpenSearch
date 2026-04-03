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
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;
import org.opensearch.plugins.spi.vectorized.DataFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory that discovers {@link SearchAnalyticsBackEndPlugin}s via
 * {@link PluginsService} and builds per-format reader managers and
 * memoizing suppliers for search-side components.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SearchBackendFactory {

    private final Map<DataFormat, CatalogSnapshotAwareReaderManager<?>> readerManagers = new HashMap<>();
    private final Map<DataFormat, CheckedSupplier<SearchExecEngine<?, ?>, IOException>> engineSuppliers = new HashMap<>();
    private final Map<DataFormat, CheckedSupplier<IndexFilterProvider<?, ?>, IOException>> indexFilterProviderSuppliers = new HashMap<>();
    private final Map<DataFormat, CheckedSupplier<IndexFilterTreeProvider<?, ?, ?>, IOException>> treeProviderSuppliers = new HashMap<>();
    private final Map<DataFormat, CheckedSupplier<SourceProvider<?, ?>, IOException>> sourceProviderSuppliers = new HashMap<>();

    public SearchBackendFactory(
        PluginsService pluginsService,
        ShardPath shardPath,
        MapperService mapperService,
        IndexSettings indexSettings
    ) throws IOException {
        for (SearchAnalyticsBackEndPlugin plugin : pluginsService.filterPlugins(SearchAnalyticsBackEndPlugin.class)) {
            for (DataFormat format : plugin.getSupportedFormats()) {
                readerManagers.put(format, plugin.createReaderManager(format, shardPath));
                engineSuppliers.put(format, memoize(format, f -> plugin.createSearchExecEngine(f, shardPath)));
                indexFilterProviderSuppliers.put(format, memoize(format, f -> plugin.createIndexFilterProvider(f, shardPath)));
                treeProviderSuppliers.put(format, memoize(format, f -> plugin.createIndexFilterTreeProvider(f, shardPath)));
                sourceProviderSuppliers.put(format, memoize(format, f -> plugin.createSourceProvider(f, shardPath)));
            }
        }
    }

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

    public Map<DataFormat, CatalogSnapshotAwareReaderManager<?>> getReaderManagers() {
        return readerManagers;
    }

    public Map<DataFormat, CheckedSupplier<SearchExecEngine<?, ?>, IOException>> getEngineSuppliers() {
        return engineSuppliers;
    }

    public Map<DataFormat, CheckedSupplier<IndexFilterProvider<?, ?>, IOException>> getIndexFilterProviderSuppliers() {
        return indexFilterProviderSuppliers;
    }

    public Map<DataFormat, CheckedSupplier<SourceProvider<?, ?>, IOException>> getSourceProviderSuppliers() {
        return sourceProviderSuppliers;
    }

    public Map<DataFormat, CheckedSupplier<IndexFilterTreeProvider<?, ?, ?>, IOException>> getTreeProviderSuppliers() {
        return treeProviderSuppliers;
    }
}
