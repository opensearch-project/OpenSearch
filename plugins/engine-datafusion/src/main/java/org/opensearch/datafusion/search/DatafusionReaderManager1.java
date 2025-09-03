/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.EngineReaderManager;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.Collection;

public class DatafusionReaderManager1 extends ReferenceManager<DatafusionReader> implements CatalogSnapshotAwareRefreshListener {
    private DatafusionReader current;
    private String path;

    public DatafusionReaderManager1(String path, Collection<FileMetadata> files) throws IOException {
        this.current = new DatafusionReader(path, files);
        this.path = path;
    }



    @Override
    public void beforeRefresh() throws IOException {
        // no op
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh && catalogSnapshot != null) {
            DatafusionReader old = this.current;
            release(old);
            this.current = new DatafusionReader(this.path, catalogSnapshot.getSearchableFiles(dataFormat));
            this.current.incRef();
        }
    }

    @Override
    protected void decRef(DatafusionReader datafusionReader) throws IOException {

    }

    @Override
    protected DatafusionReader refreshIfNeeded(DatafusionReader datafusionReader) throws IOException {
        return null;
    }

    @Override
    protected boolean tryIncRef(DatafusionReader datafusionReader) throws IOException {
        return false;
    }

    @Override
    protected int getRefCount(DatafusionReader datafusionReader) {
        return 0;
    }
}
