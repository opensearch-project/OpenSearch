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
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.Map;

/**
 * Routes {@link CatalogSnapshotLifecycleListener} events to the per-format
 * {@link EngineReaderManager}s.
 * <p>
 * File lifecycle management (ref counting, deletion) is now handled by
 * {@link org.opensearch.index.engine.exec.coord.IndexFileDeleter} inside {@link org.opensearch.index.engine.exec.coord.CatalogSnapshotManager}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatEngineCatalogSnapshotListener implements CatalogSnapshotLifecycleListener {

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;

    public DataFormatEngineCatalogSnapshotListener(Map<DataFormat, EngineReaderManager<?>> readerManagers) {
        this.readerManagers = readerManagers;
    }

    @Override
    public void beforeRefresh() throws IOException {
        for (CatalogSnapshotLifecycleListener listener : readerManagers.values()) {
            listener.beforeRefresh();
        }
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        for (CatalogSnapshotLifecycleListener listener : readerManagers.values()) {
            listener.afterRefresh(didRefresh, catalogSnapshot);
        }
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        for (CatalogSnapshotLifecycleListener listener : readerManagers.values()) {
            listener.onDeleted(catalogSnapshot);
        }
    }
}
