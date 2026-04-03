/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory that discovers {@link SearchBackEndPlugin}s via
 * {@link PluginsService} and builds the per-format reader managers consumed by {@link DataFormatAwareEngine}.
 * <p>
 * This keeps DataformatAwareEngine decoupled from the plugin layer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatAwareEngineFactory {

    private final List<SearchBackEndPlugin<?>> searchBackEndPlugins;
    private final ShardPath shardPath;
    private final IndexFileDeleter indexFileDeleter;

    @SuppressWarnings("unchecked")
    public DataFormatAwareEngineFactory(PluginsService pluginsService, ShardPath shardPath) throws IOException {
        this.searchBackEndPlugins = (List<SearchBackEndPlugin<?>>) (List<?>) pluginsService.filterPlugins(SearchBackEndPlugin.class);
        this.shardPath = shardPath;
        this.indexFileDeleter = new IndexFileDeleter(null, shardPath);
    }

    /**
     * Creates reader managers for all discovered search back-end plugins.
     * The {@link Committer} is passed through so plugins can access the backing store.
     *
     * @param committer the committer holding the backing store, or null if not available
     * @return a map of data format to reader manager
     * @throws IOException if reader manager creation fails
     */
    @SuppressWarnings("unchecked")
    public Map<DataFormat, EngineReaderManager<?>> createReaderManagers(Committer committer) throws IOException {
        Map<DataFormat, EngineReaderManager<?>> readerManagers = new HashMap<>();
        for (SearchBackEndPlugin<?> plugin : searchBackEndPlugins) {
            for (DataFormat format : plugin.getSupportedFormats()) {
                readerManagers.put(format, plugin.createReaderManager(committer, format, shardPath));
            }
        }
        return readerManagers;
    }

    /**
     * Creates a new {@link DataFormatAwareEngine} with empty reader managers.
     * Reader managers should be populated later via {@link #createReaderManagers}.
     */
    public DataFormatAwareEngine create() {
        return new DataFormatAwareEngine(new HashMap<>());
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
