/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.DataFormatAwareEngineFactory;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;

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
public class DataFormatAwareEngine implements IndexReaderProvider, Closeable {

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;
    private volatile CatalogSnapshotManager catalogSnapshotManager;
    private volatile Committer committer;

    /**
     * Constructs a new DataFormatAwareEngine.
     * Prefer using {@link DataFormatAwareEngineFactory#create()}.
     */
    public DataFormatAwareEngine(Map<DataFormat, EngineReaderManager<?>> readerManagers, CatalogSnapshotManager catalogSnapshotManager) {
        this.readerManagers = readerManagers;
        this.catalogSnapshotManager = catalogSnapshotManager;
    }

    /**
     * Constructs a new DataFormatAwareEngine without a snapshot manager.
     * The manager must be set via {@link #setCatalogSnapshotManager} before acquiring readers.
     */
    public DataFormatAwareEngine(Map<DataFormat, EngineReaderManager<?>> readerManagers) {
        this.readerManagers = readerManagers;
    }

    public void setCatalogSnapshotManager(CatalogSnapshotManager catalogSnapshotManager) {
        this.catalogSnapshotManager = catalogSnapshotManager;
    }

    /**
     * Sets the committer for durable catalog snapshot persistence.
     * May be null if no committer is configured.
     *
     * @param committer the committer instance, or null
     */
    public void setCommitter(Committer committer) {
        this.committer = committer;
    }

    /**
     * Returns the committer, or null if none has been set.
     *
     * @return the committer instance, or null
     */
    public Committer getCommitter() {
        return committer;
    }

    public EngineReaderManager<?> getReaderManager(DataFormat format) {
        return readerManagers.get(format);
    }

    /**
     * Acquires a DataFormatAwareReader on the latest catalog snapshot.
     * The caller MUST close the returned {@link DataFormatAwareReader} when done,
     * which releases the snapshot reference.
     */
    public GatedCloseable<Reader> acquireReader() throws IOException {
        if (catalogSnapshotManager == null) {
            throw new IllegalStateException("CatalogSnapshotManager not set");
        }
        GatedCloseable<CatalogSnapshot> snapshotRef = catalogSnapshotManager.acquireSnapshot();
        try {
            CatalogSnapshot catalogSnapshot = snapshotRef.get();
            Map<DataFormat, Object> readers = new HashMap<>();
            for (Map.Entry<DataFormat, EngineReaderManager<?>> entry : readerManagers.entrySet()) {
                Object reader = entry.getValue().getReader(catalogSnapshot);
                if (reader != null) {
                    readers.put(entry.getKey(), reader);
                }
            }
            DataFormatAwareReader reader = new DataFormatAwareReader(catalogSnapshot, snapshotRef, readers);
            return new GatedCloseable<>(reader, reader::close);
        } catch (Exception e) {
            snapshotRef.close();
            throw e;
        }
    }

    /**
     * A catalog-snapshot-backed data-format aware reader providing per-format reader access.
     * Closing this reader releases the catalog snapshot reference.
     */
    @ExperimentalApi
    public static class DataFormatAwareReader implements IndexReaderProvider.Reader {
        private final CatalogSnapshot catalogSnapshot;
        private final GatedCloseable<CatalogSnapshot> snapshotRef;
        private final Map<DataFormat, Object> readers;

        DataFormatAwareReader(
            CatalogSnapshot catalogSnapshot,
            GatedCloseable<CatalogSnapshot> snapshotRef,
            Map<DataFormat, Object> readers
        ) {
            this.catalogSnapshot = catalogSnapshot;
            this.snapshotRef = snapshotRef;
            this.readers = readers;
        }

        @Override
        public Object reader(DataFormat format) {
            return readers.get(format);
        }

        /**
         * Returns the reader for the given format, validated against the expected type.
         *
         * @param format the data format
         * @param readerType the expected reader class
         * @param <R> the reader type
         * @return the typed reader, or {@code null} if no reader exists for the format
         * @throws IllegalArgumentException if the reader exists but is not of the expected type
         */
        @SuppressWarnings("unchecked")
        public <R> R getReader(DataFormat format, Class<R> readerType) {
            Object reader = readers.get(format);
            if (reader == null) {
                return null;
            }
            if (readerType.isInstance(reader) == false) {
                throw new IllegalArgumentException(
                    "Reader for format [" + format.name() + "] is " + reader.getClass().getName() + ", expected " + readerType.getName()
                );
            }
            return (R) reader;
        }

        @Override
        public CatalogSnapshot catalogSnapshot() {
            return catalogSnapshot;
        }

        @Override
        public void close() {
            try {
                snapshotRef.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to release catalog snapshot reference", e);
            }
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
            IOException ioException = new IOException("Failed to close DataFormatAwareEngine resources");
            for (Exception e : exceptions) {
                ioException.addSuppressed(e);
            }
            throw ioException;
        }
    }
}
