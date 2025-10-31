/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.EngineReaderManager;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader>, CatalogSnapshotAwareRefreshListener {
    private DatafusionReader current;
    private String path;
    private String dataFormat;
    private Consumer<List<String>> onFilesAdded;
//    private final Lock refreshLock = new ReentrantLock();
//    private final List<ReferenceManager.RefreshListener> refreshListeners = new CopyOnWriteArrayList();

    public DatafusionReaderManager(String path, Collection<FileMetadata> files, String dataFormat) throws IOException {
        this.current = null;
        this.path = path;
        this.dataFormat = dataFormat;
    }

    /**
     * Set callback for when files are added during refresh
     */
    public void setOnFilesAdded(Consumer<List<String>> onFilesAdded) {
        this.onFilesAdded = onFilesAdded;
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
            Collection<WriterFileSet> newFiles = catalogSnapshot.getSearchableFiles(dataFormat);
            if(old !=null) {
                release(old);
                processFileChanges(old.files, newFiles);
            } else {
                processFileChanges(List.of(), newFiles);
            }
            this.current = new DatafusionReader(this.path, newFiles);
            this.current.incRef();
        }
    }

    private void processFileChanges(Collection<WriterFileSet> oldFiles, Collection<WriterFileSet> newFiles) {
        Set<String> oldFilePaths = extractFilePaths(oldFiles);
        Set<String> newFilePaths = extractFilePaths(newFiles);

        Set<String> filesToAdd = new HashSet<>(newFilePaths);
        filesToAdd.removeAll(oldFilePaths);

        // TODO: Either remove files periodically or let eviction handle stale files
        Set<String> filesToRemove = new HashSet<>(oldFilePaths);
        filesToRemove.removeAll(newFilePaths);

        if (!filesToAdd.isEmpty() && onFilesAdded != null) {
            onFilesAdded.accept(List.copyOf(filesToAdd));
        }
    }

    private Set<String> extractFilePaths(Collection<WriterFileSet> files) {
        String[] fileNames = files.stream()
            .flatMap(writerFileSet -> writerFileSet.getFiles().stream())
            .toArray(String[]::new);
        Set<String> paths = new HashSet<>();
        paths.addAll(Arrays.asList(fileNames));
        return paths;
    }
}
