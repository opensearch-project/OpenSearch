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
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.EngineReaderManager;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Lucene implementation of {@link EngineReaderManager}.
 * <p>
 * Wraps Lucene's {@link ReferenceManager} for {@link DirectoryReader}.
 * Acquire increments the ref count on the current reader;
 * release decrements it — same pattern as {@code DatafusionReaderManager}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneReaderManager implements EngineReaderManager<DirectoryReader> {

    Map<CatalogSnapshot, DirectoryReader> readers = new HashMap<>();
    DataFormat dataFormat;

    /**
     * Creates a new LuceneReaderManager for the given data format.
     *
     * @param dataFormat the data format for this reader manager
     */
    @SuppressWarnings("unchecked")
    public LuceneReaderManager(DataFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    /**
     * Called when files are deleted after merges.
     *
     * @param files the collection of deleted file paths
     */
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
    public void beforeRefresh() throws IOException {

    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (readers.containsKey(catalogSnapshot)) {
            return;
        }
        readers.put(catalogSnapshot, (DirectoryReader) catalogSnapshot.getReader(dataFormat));
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        readers.remove(catalogSnapshot).close();
    }
}
