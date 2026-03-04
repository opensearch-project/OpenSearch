/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry of data format SPIs from associated plugins
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatRegistry {
    private final List<CatalogSnapshotAwareRefreshListener> catalogSnapshotAwareRefreshListeners = new ArrayList<>();
    private final Map<DataFormat, FilesListener> filesListenerMap = new HashMap<>();
    private final List<CatalogSnapshotDeleteListener> catalogSnapshotDeleteListeners = new ArrayList<>();
    private final Map<DataFormat, SearchExecEngine<?, ?>> searchExecEngineMap = new HashMap<>();

    public DataFormatRegistry(List<SearchAnalyticsBackEndPlugin> searchPlugins, ShardPath shardPath) throws IOException {
        for (SearchAnalyticsBackEndPlugin plugin : searchPlugins) {
            for (DataFormat dataFormat : plugin.getSupportedFormats()) {
                SearchExecEngine<?, ?> engine = plugin.create(shardPath, dataFormat);
                EngineReaderManager<?> readerManager = engine.getReaderManager();
                catalogSnapshotAwareRefreshListeners.add(readerManager);
                filesListenerMap.put(dataFormat, readerManager);
                catalogSnapshotDeleteListeners.add(readerManager);
                searchExecEngineMap.put(dataFormat, engine);
            }
        }
    }

    public List<CatalogSnapshotAwareRefreshListener> getCatalogSnapshotAwareRefreshListeners() {
        return catalogSnapshotAwareRefreshListeners;
    }

    public List<CatalogSnapshotDeleteListener> getCatalogSnapshotDeleteListeners() {
        return catalogSnapshotDeleteListeners;
    }

    public Map<DataFormat, FilesListener> getFilesListenerMap() {
        return filesListenerMap;
    }

    public SearchExecEngine<?, ?> getSearchExecEngine(DataFormat dataFormat) {
        return searchExecEngineMap.get(dataFormat);
    }
}
