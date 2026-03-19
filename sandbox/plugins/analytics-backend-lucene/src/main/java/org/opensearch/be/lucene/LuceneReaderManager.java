/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.CatalogSnapshotAwareReaderManager;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.plugins.spi.vectorized.DataFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneReaderManager implements CatalogSnapshotAwareReaderManager<DirectoryReader> {

    private final Map<CatalogSnapshot, DirectoryReader> readers = new HashMap<>();
    private final DataFormat dataFormat;

    public LuceneReaderManager(DataFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        // no-op
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        // no-op
    }

    @Override
    public DirectoryReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        return readers.get(catalogSnapshot);
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (readers.containsKey(catalogSnapshot)) {
            return;
        }
        readers.put(catalogSnapshot, (DirectoryReader) catalogSnapshot.getReader(dataFormat));
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DirectoryReader reader = readers.remove(catalogSnapshot);
        if (reader != null) {
            reader.close();
        }
    }
}
