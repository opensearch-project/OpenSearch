/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Routes {@link CatalogSnapshotLifecycleListener} events through the
 * {@link IndexFileDeleter} and then fans out to the per-format
 * {@link EngineReaderManager}s.
 * <p>
 * Keeps lifecycle orchestration separate from the engine's component
 * registry responsibilities.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatEngineCatalogSnapshotListener implements CatalogSnapshotLifecycleListener {

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;
    private final IndexFileDeleter indexFileDeleter;

    public DataFormatEngineCatalogSnapshotListener(
        Map<DataFormat, EngineReaderManager<?>> readerManagers,
        IndexFileDeleter indexFileDeleter
    ) {
        this.readerManagers = readerManagers;
        this.indexFileDeleter = indexFileDeleter;
    }

    @Override
    public void beforeRefresh() throws IOException {
        for (CatalogSnapshotLifecycleListener listener : readerManagers.values()) {
            listener.beforeRefresh();
        }
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        Map<DataFormat, Collection<String>> newFiles = indexFileDeleter.addFileReferences(catalogSnapshot);
        if (newFiles.isEmpty() == false) {
            notifyFilesAdded(newFiles);
        }
        for (CatalogSnapshotLifecycleListener listener : readerManagers.values()) {
            listener.afterRefresh(didRefresh, catalogSnapshot);
        }
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        Map<DataFormat, Collection<String>> deletedFiles = indexFileDeleter.removeFileReferences(catalogSnapshot);
        if (deletedFiles.isEmpty() == false) {
            notifyFilesDeleted(deletedFiles);
        }
        for (CatalogSnapshotLifecycleListener listener : readerManagers.values()) {
            listener.onDeleted(catalogSnapshot);
        }
    }

    private void notifyFilesAdded(Map<DataFormat, Collection<String>> filesByFormat) throws IOException {
        for (Map.Entry<DataFormat, Collection<String>> entry : filesByFormat.entrySet()) {
            EngineReaderManager<?> rm = readerManagers.get(entry.getKey());
            if (rm != null) {
                rm.onFilesAdded(entry.getValue());
            }
        }
    }

    private void notifyFilesDeleted(Map<DataFormat, Collection<String>> filesByFormat) throws IOException {
        for (Map.Entry<DataFormat, Collection<String>> entry : filesByFormat.entrySet()) {
            EngineReaderManager<?> rm = readerManagers.get(entry.getKey());
            if (rm != null) {
                rm.onFilesDeleted(entry.getValue());
            }
        }
    }
}
