/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages {@link DatafusionReader} instances (native memory).
 * <p>
 * Acquire returns a DatafusionReader with incremented ref count;
 * release decrements it. On refresh, a new reader is swapped in
 * atomically from the updated catalog snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader> {

    Map<CatalogSnapshot, DatafusionReader> readers = new HashMap<>();
    private final DataFormat dataFormat;
    private final String directoryPath;

    /**
     * Creates a reader manager
     * @param dataFormat the data format for this reader
     * @param shardPath the shard path to read data from
     */
    public DatafusionReaderManager(DataFormat dataFormat, ShardPath shardPath) {
        this.dataFormat = dataFormat;
        directoryPath = shardPath.getDataPath().resolve(dataFormat.name()).toString();
    }

    @Override
    public DatafusionReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        if (readers.containsKey(catalogSnapshot)) {
            return readers.get(catalogSnapshot);
        }
        throw new IOException("No DataFusion reader available");
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        readers.remove(catalogSnapshot).close();
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        // TODO: evict deleted files from cache manager
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        // TODO: Add new files to cache manager
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (!didRefresh) return;
        // This catalog snapshot is already present in the reader manager
        if (readers.containsKey(catalogSnapshot)) {
            return;
        }
        DatafusionReader reader = new DatafusionReader(directoryPath, catalogSnapshot.getSearchableFiles(dataFormat.name()));
        readers.put(catalogSnapshot, reader);
    }
}
