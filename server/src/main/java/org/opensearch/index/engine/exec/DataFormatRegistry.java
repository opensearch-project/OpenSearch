/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.engine.exec.SourceProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry of reader managers, search engine factories, and index filter provider factories per data format.
 * <p>
 * Accepts {@link MapperService} and {@link IndexSettings} to determine which
 * formats are relevant for the index.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatRegistry {

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers = new HashMap<>();
    private final Map<DataFormat, CheckedFunction<DataFormat, SearchExecEngine<?, ?>, IOException>> engineFactories = new HashMap<>();
    private final Map<DataFormat, CheckedFunction<DataFormat, IndexFilterProvider<?, ?>, IOException>> indexFilterProviderFactories =
        new HashMap<>();
    private final Map<DataFormat, CheckedFunction<DataFormat, SourceProvider<?, ?>, IOException>> sourceProviderFactories =
        new HashMap<>();

    public DataFormatRegistry(
        List<SearchAnalyticsBackEndPlugin> searchPlugins,
        ShardPath shardPath,
        MapperService mapperService,
        IndexSettings indexSettings
    ) throws IOException {
        for (SearchAnalyticsBackEndPlugin plugin : searchPlugins) {
            for (DataFormat format : plugin.getSupportedFormats()) {
                // TODO: use mapperService and indexSettings to filter formats relevant to this index
                readerManagers.put(format, plugin.createReaderManager(format, shardPath));
                engineFactories.put(format, f -> plugin.createSearchExecEngine(f, shardPath));
                indexFilterProviderFactories.put(format, f -> plugin.createIndexFilterProvider(f, shardPath));
                sourceProviderFactories.put(format, f -> plugin.createSourceProvider(f, shardPath));
            }
        }
    }

    public Map<DataFormat, EngineReaderManager<?>> getReaderManagers() {
        return readerManagers;
    }

    public SearchExecEngine<?, ?> createSearchExecEngine(DataFormat format) throws IOException {
        CheckedFunction<DataFormat, SearchExecEngine<?, ?>, IOException> factory = engineFactories.get(format);
        if (factory == null) {
            throw new IllegalArgumentException("No plugin registered for format: " + format.name());
        }
        return factory.apply(format);
    }

    public IndexFilterProvider<?, ?> createIndexFilterProvider(DataFormat format) throws IOException {
        CheckedFunction<DataFormat, IndexFilterProvider<?, ?>, IOException> factory = indexFilterProviderFactories.get(format);
        if (factory == null) {
            throw new IllegalArgumentException("No index filter provider for format: " + format.name());
        }
        return factory.apply(format);
    }

    public SourceProvider<?, ?> createSourceProvider(DataFormat format) throws IOException {
        CheckedFunction<DataFormat, SourceProvider<?, ?>, IOException> factory = sourceProviderFactories.get(format);
        if (factory == null) {
            throw new IllegalArgumentException("No source provider for format: " + format.name());
        }
        return factory.apply(format);
    }
}
