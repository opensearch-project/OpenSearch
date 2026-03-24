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
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory that discovers {@link SearchBackEndPlugin}s via
 * {@link PluginsService} and builds the per-format reader managers and
 * memoizing suppliers consumed by {@link DataFormatAwareEngine}.
 * <p>
 * This keeps DataformatAwareEngine decoupled from the plugin layer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatAwareEngineFactory {

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers = new HashMap<>();
    private final IndexFileDeleter indexFileDeleter;

    public DataFormatAwareEngineFactory(
        PluginsService pluginsService,
        ShardPath shardPath,
        MapperService mapperService,
        IndexSettings indexSettings
    ) throws IOException {
        for (SearchBackEndPlugin plugin : pluginsService.filterPlugins(SearchBackEndPlugin.class)) {
            for (DataFormat format : plugin.getSupportedFormats()) {
                // TODO: use mapperService and indexSettings to filter formats relevant to this index
                readerManagers.put(format, plugin.createReaderManager(format, shardPath));
            }
        }
        this.indexFileDeleter = new IndexFileDeleter(null, shardPath);
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
     * Creates a new {@link DataFormatAwareEngine} populated with the discovered
     * reader managers and memoizing suppliers.
     */
    public DataFormatAwareEngine create() {
        return new DataFormatAwareEngine(readerManagers);
    }

    /**
     * Creates a {@link CatalogSnapshotLifecycleListener} that routes events
     * through the {@link IndexFileDeleter} and fans out to the given reader managers.
     *
     * @param readerManagers the per-format reader managers that receive notifications
     */
    public CatalogSnapshotLifecycleListener createCatalogSnapshotListener(Map<DataFormat, EngineReaderManager<?>> readerManagers) {
        return new DataFormatEngineCatalogSnapshotListener(readerManagers, indexFileDeleter);
    }
}
