/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.EngineReaderManager;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader>, CatalogSnapshotAwareRefreshListener {
    private DatafusionReader current;
    private String path;
    private String dataFormat;
//    private final Lock refreshLock = new ReentrantLock();
//    private final List<ReferenceManager.RefreshListener> refreshListeners = new CopyOnWriteArrayList();

    public DatafusionReaderManager(String path, Collection<FileMetadata> files, String dataFormat) throws IOException {
        WriterFileSet writerFileSet = new WriterFileSet(Path.of(URI.create("file:///" + path)), 1);
        files.forEach(fileMetadata -> writerFileSet.add(fileMetadata.file()));
        this.current = new DatafusionReader(path, List.of(writerFileSet));;
        this.path = path;
        this.dataFormat = dataFormat;
    }

    @Override
    public DatafusionReader acquire() throws IOException {
        if (current == null) {
            throw new RuntimeException("Invalid state for datafusion reader");
        }
        current.incRef();
        return current;
    }

    @Override
    public void release(DatafusionReader reference) throws IOException {
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
            DatafusionReader old = this.current;
            if(old !=null) {
                release(old);
            }
            this.current = new DatafusionReader(this.path, catalogSnapshot.getSearchableFiles(dataFormat));
            this.current.incRef();
        }
    }
}
