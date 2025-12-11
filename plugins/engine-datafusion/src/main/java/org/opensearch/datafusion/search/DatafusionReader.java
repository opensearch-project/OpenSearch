/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.datafusion.jni.handle.ReaderHandle;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
/**
 * DataFusion reader for JNI operations.
 */
public class DatafusionReader implements Closeable {
    /**
     * The directory path.
     */
    public String directoryPath;
    /**
     * The file metadata collection.
     */
    public Collection<WriterFileSet> files;
    /**
     * The reader handle.
     */
    public ReaderHandle readerHandle;
    /**
     * The catalog snapshot reference.
     */
    private CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotRef;
    /**
     * The unique reader ID in the global registry
     */
    private Long registryId;
    /**
     * The shard ID this reader belongs to
     */
    private String shardId;



    /**
     * Constructor
     * @param directoryPath The directory path
     * @param files The file metadata collection
     */
    public DatafusionReader(String directoryPath, CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotRef, Collection<WriterFileSet> files) {
        this.directoryPath = directoryPath;
        this.catalogSnapshotRef = catalogSnapshotRef;
        this.files = files;
        String[] fileNames = new String[0];
        if(files != null) {
            System.out.println("Got the files!!!!!");
            fileNames = files.stream()
                .flatMap(writerFileSet -> writerFileSet.getFiles().stream())
                .toArray(String[]::new);
        }
        System.out.println("File names: " + Arrays.toString(fileNames));
        System.out.println("Directory path: " + directoryPath);
        this.readerHandle = new ReaderHandle(directoryPath, fileNames, this::releaseCatalogSnapshot);
    }

    /**
     * Gets the cache pointer.
     * @return the cache pointer
     */
    public long getReaderPtr() {
        return readerHandle.getPointer();
    }

    /**
     * Increments the reference count.
     */
    public void incRef() {
        readerHandle.retain();
    }

    /**
     * Decrements the reference count.
     */
    public void decRef() {
        readerHandle.close();
        // Check if this was the last reference and unregister if so
        if (readerHandle.getRefCount() == 0 && registryId != null) {
            DatafusionReaderRegistry.getInstance().unregisterReader(registryId);
            registryId = null; // Prevent double unregistration
        }
    }

    /**
     * Gets the reference count.
     * @return the reference count
     */
    public int getRefCount() {
        return readerHandle.getRefCount();
    }

    /**
     * Set the registry ID for this reader
     * @param registryId The registry ID
     */
    public void setRegistryId(Long registryId) {
        this.registryId = registryId;
    }

    /**
     * Get the registry ID for this reader
     * @return The registry ID
     */
    public Long getRegistryId() {
        return registryId;
    }

    /**
     * Set the shard ID for this reader
     * @param shardId The shard ID
     */
    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    /**
     * Get the shard ID for this reader
     * @return The shard ID
     */
    public String getShardId() {
        return shardId;
    }

    @Override
    public void close() {
        // Unregister from the global registry if registered
        if (registryId != null) {
            DatafusionReaderRegistry.getInstance().unregisterReader(registryId);
            registryId = null; // Prevent double unregistration
        }
        readerHandle.close();
    }

    private void releaseCatalogSnapshot() {
        try {
            if (catalogSnapshotRef != null)
                catalogSnapshotRef.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
