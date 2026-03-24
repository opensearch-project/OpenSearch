/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.DataFormatAwareEngineFactory;
import org.opensearch.index.engine.exec.EngineReaderManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Owns all reader managers, lazily creates search engines, index filter providers
 * and source providers per data format.
 * <p>
 * Instances are created by {@link DataFormatAwareEngineFactory}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatAwareEngine implements Closeable {

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;
    private volatile CatalogSnapshot latestSnapshot;

    /**
     * Constructs a new CompositeEngine with pre-built maps.
     * Prefer using {@link DataFormatAwareEngineFactory#create()}.
     */
    public DataFormatAwareEngine(Map<DataFormat, EngineReaderManager<?>> readerManagers) {
        this.readerManagers = readerManagers;
    }

    public EngineReaderManager<?> getReaderManager(DataFormat format) {
        return readerManagers.get(format);
    }

    /**
     * Called by the catalog snapshot lifecycle listener after a refresh
     * to update the latest searchable snapshot.
     */
    public void setLatestSnapshot(CatalogSnapshot snapshot) {
        CatalogSnapshot prev = this.latestSnapshot;
        this.latestSnapshot = snapshot;
        if (prev != null) {
            prev.decRef();
        }
    }

    /**
     * Acquires a DataFormatAwareReader on the latest catalog snapshot.
     * The snapshot is incRef'd; the caller MUST close the returned
     * {@link DataFormatAwareReader} when done, which decRef's the snapshot.
     */
    public DataFormatAwareReader acquireReader() throws IOException {
        CatalogSnapshot snapshot = latestSnapshot;
        if (snapshot == null) {
            throw new IllegalStateException("No catalog snapshot available");
        }
        return acquireReader(snapshot);
    }

    /**
     * Acquires a composite reader on a specific catalog snapshot.
     */
    public DataFormatAwareReader acquireReader(CatalogSnapshot catalogSnapshot) throws IOException {
        catalogSnapshot.incRef();
        try {
            Map<DataFormat, Object> readers = new HashMap<>();
            for (Map.Entry<DataFormat, EngineReaderManager<?>> entry : readerManagers.entrySet()) {
                Object reader = entry.getValue().getReader(catalogSnapshot);
                if (reader != null) {
                    readers.put(entry.getKey(), reader);
                }
            }
            return new DataFormatAwareReader(catalogSnapshot, readers);
        } catch (Exception e) {
            catalogSnapshot.decRef();
            throw e;
        }
    }

    /**
     * A catalog-snapshot-backed data-format aware reader providing per-format reader access.
     * Closing this reader releases the catalog snapshot reference.
     */
    @ExperimentalApi
    public static class DataFormatAwareReader implements Closeable {
        private final CatalogSnapshot catalogSnapshot;
        private final Map<DataFormat, Object> readers;

        DataFormatAwareReader(CatalogSnapshot catalogSnapshot, Map<DataFormat, Object> readers) {
            this.catalogSnapshot = catalogSnapshot;
            this.readers = readers;
        }

        public Object getReader(DataFormat format) {
            return readers.get(format);
        }

        public CatalogSnapshot getCatalogSnapshot() {
            return catalogSnapshot;
        }

        @Override
        public void close() {
            catalogSnapshot.decRef();
        }
    }

    @Override
    public void close() throws IOException {
        List<Exception> exceptions = new ArrayList<>();
        for (EngineReaderManager<?> rm : readerManagers.values()) {
            if (rm instanceof Closeable) {
                try {
                    ((Closeable) rm).close();
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        }
        if (exceptions.isEmpty() == false) {
            IOException ioException = new IOException("Failed to close CompositeEngine resources");
            for (Exception e : exceptions) {
                ioException.addSuppressed(e);
            }
            throw ioException;
        }
    }
}
