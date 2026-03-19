/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.CatalogSnapshotAwareReaderManager;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.spi.vectorized.DataFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Catalog-snapshot-aware reader manager.
 * afterRefresh → creates native reader via JNI, onDeleted → closes it.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements CatalogSnapshotAwareReaderManager<DatafusionReader> {

    private final Map<CatalogSnapshot, DatafusionReader> readers = new HashMap<>();
    private final String dataFormatName;
    private final String directoryPath;

    public DatafusionReaderManager(DataFormat dataFormat, ShardPath shardPath) {
        this.dataFormatName = dataFormat.getName();
        this.directoryPath = shardPath.getDataPath().resolve(dataFormatName).toString();
    }

    @Override
    public DatafusionReader getReader(CatalogSnapshot snapshot) throws IOException {
        DatafusionReader reader = readers.get(snapshot);
        if (reader == null) throw new IOException("No reader for snapshot " + snapshot);
        return reader;
    }

    @Override
    public void beforeRefresh() {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot snapshot) throws IOException {
        if (didRefresh == false || readers.containsKey(snapshot)) return;
        Collection<WriterFileSet> files = snapshot.getSearchableFiles(dataFormatName);
        readers.put(snapshot, new DatafusionReader(directoryPath, files));
    }

    @Override
    public void onDeleted(CatalogSnapshot snapshot) {
        DatafusionReader reader = readers.remove(snapshot);
        if (reader != null) reader.close();
    }

    @Override
    public void onFilesDeleted(Collection<String> files) {}

    @Override
    public void onFilesAdded(Collection<String> files) {}
}
