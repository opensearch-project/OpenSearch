/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.index.engine.exec.engine.FileMetadata;
import org.opensearch.index.engine.exec.manage.CatalogSnapshot;
import org.opensearch.index.engine.exec.read.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.exec.read.EngineReaderManager;

import java.io.IOException;
import java.util.Collection;

/**
 * Datafusion reader manager to manage datafusion readers
 */
public class DatafusionReaderManager
    implements
        EngineReaderManager<org.opensearch.datafusion.search.DatafusionReader>,
        CatalogSnapshotAwareRefreshListener {
    private org.opensearch.datafusion.search.DatafusionReader current;
    private String path;
    private String dataFormat;

    public DatafusionReaderManager(String path, Collection<FileMetadata> files, String dataFormat) throws IOException {
        this.current = null;
        this.path = path;
        this.dataFormat = dataFormat;
    }

    @Override
    public org.opensearch.datafusion.search.DatafusionReader acquire() throws IOException {
        if (current == null) {
            throw new RuntimeException("Invalid state for datafusion reader");
        }
        current.incRef();
        return current;
    }

    @Override
    public void release(org.opensearch.datafusion.search.DatafusionReader reference) throws IOException {
        assert reference != null : "Shard view can't be null";
        reference.decRef();
    }

    @Override
    public void beforeRefresh() throws IOException {
        // no op
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh && catalogSnapshot != null) {
            org.opensearch.datafusion.search.DatafusionReader old = this.current;
            if (old != null) {
                release(old);
            }
            this.current = new org.opensearch.datafusion.search.DatafusionReader(this.path, catalogSnapshot.getSearchableFiles(dataFormat));
            this.current.incRef();
        }
    }
}
