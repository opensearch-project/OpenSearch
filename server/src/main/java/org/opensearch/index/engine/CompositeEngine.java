/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.exec.CatalogSnapshotDeleteListener;
import org.opensearch.index.engine.exec.DataFormatRegistry;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.engine.exec.SourceProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns all reader managers, lazily creates search engines per each shard and index filter providers.
 * This stands as a bridge for reads/writes. Initializes engines and providers only relevant to the
 * index settings and mappings.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeEngine implements Closeable {

    private static final Logger logger = LogManager.getLogger(CompositeEngine.class);

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;
    private final DataFormatRegistry dataFormatRegistry;
    private final Map<DataFormat, SearchExecEngine<?, ?>> searchEngines = new ConcurrentHashMap<>();
    private final Map<DataFormat, IndexFilterProvider<?, ?>> indexFilterProviders = new ConcurrentHashMap<>();
    private final Map<DataFormat, SourceProvider<?, ?>> sourceProviders = new ConcurrentHashMap<>();

    public CompositeEngine(DataFormatRegistry dataFormatRegistry) {
        this.dataFormatRegistry = dataFormatRegistry;
        this.readerManagers = dataFormatRegistry.getReaderManagers();
    }

    public EngineReaderManager<?> getReaderManager(DataFormat format) {
        return readerManagers.get(format);
    }

    public SearchExecEngine<?, ?> getSearchExecEngine(DataFormat format) throws IOException {
        SearchExecEngine<?, ?> engine = searchEngines.get(format);
        if (engine != null) {
            return engine;
        }
        synchronized (searchEngines) {
            engine = searchEngines.get(format);
            if (engine == null) {
                engine = dataFormatRegistry.createSearchExecEngine(format);
                searchEngines.put(format, engine);
            }
            return engine;
        }
    }

    public IndexFilterProvider<?, ?> getIndexFilterProvider(DataFormat format) throws IOException {
        IndexFilterProvider<?, ?> provider = indexFilterProviders.get(format);
        if (provider != null) {
            return provider;
        }
        synchronized (indexFilterProviders) {
            provider = indexFilterProviders.get(format);
            if (provider == null) {
                provider = dataFormatRegistry.createIndexFilterProvider(format);
                indexFilterProviders.put(format, provider);
            }
            return provider;
        }
    }

    public SourceProvider<?, ?> getSourceProvider(DataFormat format) throws IOException {
        SourceProvider<?, ?> sp = sourceProviders.get(format);
        if (sp != null) {
            return sp;
        }
        synchronized (sourceProviders) {
            sp = sourceProviders.get(format);
            if (sp == null) {
                sp = dataFormatRegistry.createSourceProvider(format);
                sourceProviders.put(format, sp);
            }
            return sp;
        }
    }

    public List<CatalogSnapshotAwareRefreshListener> getCatalogSnapshotAwareRefreshListeners() {
        return new ArrayList<>(readerManagers.values());
    }

    public List<CatalogSnapshotDeleteListener> getCatalogSnapshotDeleteListeners() {
        return new ArrayList<>(readerManagers.values());
    }

    public void notifyFilesAdded(Map<DataFormat, Collection<String>> dfNewFiles) throws IOException {
        for (Map.Entry<DataFormat, Collection<String>> entry : dfNewFiles.entrySet()) {
            FilesListener listener = readerManagers.get(entry.getKey());
            if (listener != null) {
                listener.onFilesAdded(entry.getValue());
            }
        }
    }

    public void notifyDelete(Map<DataFormat, Collection<String>> dfFilesToDelete) throws IOException {
        for (Map.Entry<DataFormat, Collection<String>> entry : dfFilesToDelete.entrySet()) {
            FilesListener listener = readerManagers.get(entry.getKey());
            if (listener != null) {
                listener.onFilesDeleted(entry.getValue());
            }
        }
    }

    public void notifyCatalogSnapshotDelete(CatalogSnapshot catalogSnapshot) throws IOException {
        for (EngineReaderManager<?> rm : readerManagers.values()) {
            rm.onDeleted(catalogSnapshot);
        }
    }

    public ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        return null; // TODO : return this.catalogSnapshotManager.acquireSnapshot();
    }

    @Override
    public void close() throws IOException {
        for (SearchExecEngine<?, ?> engine : searchEngines.values()) {
            engine.close();
        }
        for (IndexFilterProvider<?, ?> provider : indexFilterProviders.values()) {
            provider.close();
        }
        for (SourceProvider<?, ?> sp : sourceProviders.values()) {
            sp.close();
        }
    }

    @ExperimentalApi
    public static abstract class ReleasableRef<T> implements AutoCloseable {

        private final T t;

        public ReleasableRef(T t) {
            this.t = t;
        }

        public T getRef() {
            return t;
        }
    }
}
