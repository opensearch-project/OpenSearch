/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.CatalogSnapshotLifecycleListener;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.engine.exec.SourceProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Owns all reader managers, lazily creates search engines, index filter providers
 * and source providers per data format.
 * <p>
 * Instances are created by {@link org.opensearch.index.engine.exec.CompositeEngineFactory}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeEngine implements Closeable {

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;
    private final Map<DataFormat, CheckedSupplier<SearchExecEngine<?, ?>, IOException>> engineSuppliers;
    private final Map<DataFormat, CheckedSupplier<IndexFilterProvider<?, ?>, IOException>> indexFilterProviderSuppliers;
    private final Map<DataFormat, CheckedSupplier<SourceProvider<?, ?>, IOException>> sourceProviderSuppliers;

    /**
     * Constructs a new CompositeEngine with pre-built maps.
     * Prefer using {@link org.opensearch.index.engine.exec.CompositeEngineFactory#create()}.
     */
    public CompositeEngine(
        Map<DataFormat, EngineReaderManager<?>> readerManagers,
        Map<DataFormat, CheckedSupplier<SearchExecEngine<?, ?>, IOException>> engineSuppliers,
        Map<DataFormat, CheckedSupplier<IndexFilterProvider<?, ?>, IOException>> indexFilterProviderSuppliers,
        Map<DataFormat, CheckedSupplier<SourceProvider<?, ?>, IOException>> sourceProviderSuppliers
    ) {
        this.readerManagers = readerManagers;
        this.engineSuppliers = engineSuppliers;
        this.indexFilterProviderSuppliers = indexFilterProviderSuppliers;
        this.sourceProviderSuppliers = sourceProviderSuppliers;
    }

    // ---- Public getters ----

    public EngineReaderManager<?> getReaderManager(DataFormat format) {
        return readerManagers.get(format);
    }

    public SearchExecEngine<?, ?> getSearchExecEngine(DataFormat format) throws IOException {
        return getFromSupplier(engineSuppliers, format, "search exec engine");
    }

    public IndexFilterProvider<?, ?> getIndexFilterProvider(DataFormat format) throws IOException {
        return getFromSupplier(indexFilterProviderSuppliers, format, "index filter provider");
    }

    public SourceProvider<?, ?> getSourceProvider(DataFormat format) throws IOException {
        return getFromSupplier(sourceProviderSuppliers, format, "source provider");
    }

    private <T> T getFromSupplier(
        Map<DataFormat, CheckedSupplier<T, IOException>> suppliers,
        DataFormat format,
        String label
    ) throws IOException {
        CheckedSupplier<T, IOException> supplier = suppliers.get(format);
        if (supplier == null) {
            throw new IllegalArgumentException("No " + label + " registered for format: " + format.name());
        }
        return supplier.get();
    }

    // ---- Lifecycle listener helpers ----

    public List<CatalogSnapshotLifecycleListener> getCatalogSnapshotLifecycleListeners() {
        return new ArrayList<>(readerManagers.values());
    }

    public void notifyFilesAdded(Map<DataFormat, Collection<String>> filesByFormat) throws IOException {
        for (Map.Entry<DataFormat, Collection<String>> entry : filesByFormat.entrySet()) {
            EngineReaderManager<?> rm = readerManagers.get(entry.getKey());
            if (rm != null) {
                rm.onFilesAdded(entry.getValue());
            }
        }
    }

    public void notifyDelete(Map<DataFormat, Collection<String>> filesByFormat) throws IOException {
        for (Map.Entry<DataFormat, Collection<String>> entry : filesByFormat.entrySet()) {
            EngineReaderManager<?> rm = readerManagers.get(entry.getKey());
            if (rm != null) {
                rm.onFilesDeleted(entry.getValue());
            }
        }
    }

    public void notifyCatalogSnapshotDelete(CatalogSnapshot catalogSnapshot) throws IOException {
        for (CatalogSnapshotLifecycleListener listener : getCatalogSnapshotLifecycleListeners()) {
            listener.onDeleted(catalogSnapshot);
        }
    }

    // ---- Snapshot acquisition ----

    /**
     * Acquires a snapshot across all reader managers, returning a releasable reference.
     */
    public ReleasableRef acquireSnapshot(CatalogSnapshot catalogSnapshot) throws IOException {
        List<Object> readers = new ArrayList<>();
        for (EngineReaderManager<?> rm : readerManagers.values()) {
            readers.add(rm.getReader(catalogSnapshot));
        }
        return new ReleasableRef(readers);
    }

    /**
     * A releasable reference to a set of readers acquired from reader managers.
     */
    @ExperimentalApi
    public static class ReleasableRef implements Closeable {
        private final List<Object> readers;

        ReleasableRef(List<Object> readers) {
            this.readers = readers;
        }

        public List<Object> getReaders() {
            return readers;
        }

        @Override
        public void close() throws IOException {
            // Reader managers handle their own reference counting;
            // this is a placeholder for future release logic.
        }
    }

    // ---- Closeable ----

    @Override
    public void close() throws IOException {
        List<Exception> exceptions = new ArrayList<>();
        closeSupplierInstances(engineSuppliers.values(), exceptions);
        closeSupplierInstances(indexFilterProviderSuppliers.values(), exceptions);
        closeSupplierInstances(sourceProviderSuppliers.values(), exceptions);
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

    /**
     * Attempts to retrieve each memoized instance and close it if it implements {@link Closeable}.
     * Suppliers that were never invoked will return quickly from the memoize wrapper.
     */
    private static <T> void closeSupplierInstances(
        Collection<CheckedSupplier<T, IOException>> suppliers,
        List<Exception> exceptions
    ) {
        for (CheckedSupplier<T, IOException> supplier : suppliers) {
            try {
                T instance = supplier.get();
                if (instance instanceof Closeable) {
                    ((Closeable) instance).close();
                }
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
    }
}
