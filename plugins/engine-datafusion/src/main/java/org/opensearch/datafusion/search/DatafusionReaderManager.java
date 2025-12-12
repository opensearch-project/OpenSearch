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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.EngineReaderManager;
import org.opensearch.index.engine.FileDeletionListener;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader>, CatalogSnapshotAwareRefreshListener, FileDeletionListener {
    private static final Logger logger = LogManager.getLogger(DatafusionReaderManager.class);
    
    private DatafusionReader current;
    private String path;
    private String dataFormat;
    private Consumer<List<String>> onFilesAdded;
    private String shardId;
//    private final Lock refreshLock = new ReentrantLock();
//    private final List<ReferenceManager.RefreshListener> refreshListeners = new CopyOnWriteArrayList();

    public DatafusionReaderManager(String path, Collection<FileMetadata> files, String dataFormat) throws IOException {
        this(path, files, dataFormat, null);
    }

    public DatafusionReaderManager(String path, Collection<FileMetadata> files, String dataFormat, String shardId) throws IOException {
        WriterFileSet writerFileSet = new WriterFileSet(Path.of(URI.create("file:///" + path)), 1);
        files.forEach(fileMetadata -> writerFileSet.add(fileMetadata.file()));
        this.current = new DatafusionReader(path, null, List.of(writerFileSet));
        this.path = path;
        this.dataFormat = dataFormat;
        this.shardId = shardId != null ? shardId : path; // Use path as fallback if shardId not provided
        
        // Register the initial reader with the global registry
        registerReader(this.current);
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
    public void afterRefresh(boolean didRefresh, CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshot) throws IOException {
        if (didRefresh && catalogSnapshot != null) {
            DatafusionReader old = this.current;
            Collection<WriterFileSet> newFiles = catalogSnapshot.getRef().getSearchableFiles(dataFormat);
            if(old !=null) {
                release(old);
                processFileChanges(old.files, newFiles);
            } else {
                processFileChanges(List.of(), newFiles);
            }
            this.current = new DatafusionReader(this.path, catalogSnapshot, catalogSnapshot.getRef().getSearchableFiles(dataFormat));
            // Register the new reader with the global registry
            registerReader(this.current);
        }
    }
    
    /**
     * Register a reader with the global registry
     */
    private void registerReader(DatafusionReader reader) {
        if (reader != null) {
            reader.setShardId(this.shardId);
            long registryId = DatafusionReaderRegistry.getInstance().registerReader(this.shardId, reader);
            reader.setRegistryId(registryId);
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
            .map(fileName -> String.format("%s/%s", this.path, fileName))
            .toArray(String[]::new);
        Set<String> paths = new HashSet<>();
        paths.addAll(Arrays.asList(fileNames));
        return paths;
    }

    @Override
    public void onFileDeleted(Collection<String> files) throws IOException {
        // TODO - Hook cache eviction with deletion here
        System.out.println("onFileDeleted call from DatafusionReader Manager: " + files);
    }

    public void getRefcount(DatafusionReader reader){
        logger.info("READER id {}, shardID, reader fecount",reader.getRefCount());
    }
}
