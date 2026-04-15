/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Lucene implementation of {@link EngineReaderManager}.
 * <p>
 * Constructed with a {@link DataFormat} and an initial {@link DirectoryReader}
 * (typically opened from an IndexWriter). Maintains a map of {@link CatalogSnapshot}
 * to {@link DirectoryReader} so each snapshot gets the reader that was current
 * at the time of its refresh. On each {@link #afterRefresh}, the current reader is
 * refreshed via {@link DirectoryReader#openIfChanged}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneReaderManager implements EngineReaderManager<DirectoryReader> {

    private final DataFormat dataFormat;
    private final Map<CatalogSnapshot, DirectoryReader> readers = new HashMap<>();
    private volatile DirectoryReader currentReader;

    /**
     * Creates a new LuceneReaderManager.
     *
     * @param dataFormat the data format this reader manager serves
     * @param initialReader the initial DirectoryReader, must not be null
     * @throws NullPointerException if initialReader is null
     */
    public LuceneReaderManager(DataFormat dataFormat, DirectoryReader initialReader) {
        this.dataFormat = dataFormat;
        Objects.requireNonNull(initialReader, "initialReader must not be null");
        this.currentReader = initialReader;
    }

    @Override
    public DirectoryReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        DirectoryReader reader = readers.get(catalogSnapshot);
        if (reader == null) {
            throw new IllegalStateException("No reader available for catalog snapshot [gen=" + catalogSnapshot.getGeneration() + "]");
        }
        return reader;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // no-op
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false || readers.containsKey(catalogSnapshot)) {
            return;
        }
        DirectoryReader refreshed = DirectoryReader.openIfChanged(currentReader);
        if (refreshed != null) {
            currentReader = refreshed;
        }
        readers.put(catalogSnapshot, currentReader);
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DirectoryReader reader = readers.remove(catalogSnapshot);
        if (reader != null) {
            reader.close();
        }
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
    public void close() throws IOException {
        for (DirectoryReader reader : readers.values()) {
            reader.close();
        }
        readers.clear();
    }
}
