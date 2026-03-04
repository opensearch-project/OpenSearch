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
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.TriConsumer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.exec.CatalogSnapshotDeleteListener;
import org.opensearch.index.engine.exec.DataFormatRegistry;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.shard.ShardPath;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

@ExperimentalApi
public class CompositeEngine implements Closeable {

    private static final Logger logger = LogManager.getLogger(CompositeEngine.class);
    private final Map<DataFormat, FilesListener> fileListeners;
    private final List<CatalogSnapshotAwareRefreshListener> catalogSnapshotAwareRefreshListeners;
    private final List<CatalogSnapshotDeleteListener> deleteSnapshotListeners;
    private static final TriConsumer<
        Supplier<ReleasableRef<CatalogSnapshot>>,
        CatalogSnapshotAwareRefreshListener,
        Boolean> POST_REFRESH_CATALOG_SNAPSHOT_AWARE_LISTENER_CONSUMER = (
            catalogSnapshot,
            catalogSnapshotAwareRefreshListener,
            didRefresh) -> {
            try {
                // Wrap in Supplier as required by CatalogSnapshotAwareRefreshListener interface
                catalogSnapshotAwareRefreshListener.afterRefresh(didRefresh, catalogSnapshot.get().getRef());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    private static final Consumer<ReferenceManager.RefreshListener> POST_REFRESH_LISTENER_CONSUMER = refreshListener -> {
        try {
            refreshListener.afterRefresh(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    public CompositeEngine(DataFormatRegistry dataFormatRegistry, ShardPath shardPath) throws IOException {
        fileListeners = dataFormatRegistry.getFilesListenerMap();
        deleteSnapshotListeners = dataFormatRegistry.getCatalogSnapshotDeleteListeners();
        catalogSnapshotAwareRefreshListeners = dataFormatRegistry.getCatalogSnapshotAwareRefreshListeners();
    }

    @Override
    public void close() throws IOException {

    }

    public void notifyDelete(Map<DataFormat, Collection<String>> dfFilesToDelete) throws IOException {
        for (DataFormat format : fileListeners.keySet()) {
            fileListeners.get(format).onFilesDeleted(dfFilesToDelete.get(format));
        }
    }

    public void notifyFilesAdded(Map<DataFormat, Collection<String>> dfNewFiles) throws IOException {
        for (DataFormat format : fileListeners.keySet()) {
            fileListeners.get(format).onFilesAdded(dfNewFiles.get(format));
        }
    }

    public void notifyCatalogSnapshotDelete(CatalogSnapshot catalogSnapshot) throws IOException {
        for (CatalogSnapshotDeleteListener listener : deleteSnapshotListeners) {
            listener.onDeleted(catalogSnapshot);
        }
    }

    private void invokeRefreshListeners(boolean didRefresh) {
        catalogSnapshotAwareRefreshListeners.forEach(
            refreshListener -> POST_REFRESH_CATALOG_SNAPSHOT_AWARE_LISTENER_CONSUMER.apply(
                this::acquireSnapshot,
                refreshListener,
                didRefresh
            )
        );

    }

    public ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        return null;// TODO : return this.catalogSnapshotManager.acquireSnapshot();
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
