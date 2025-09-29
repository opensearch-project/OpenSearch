/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.datafusion.DataFusionQueryJNI;
import org.opensearch.index.engine.exec.FileMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.datafusion.DataFusionQueryJNI.closeDatafusionReader;

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
    public Collection<FileMetadata> files;
    /**
     * The cache pointer.
     */
    public long cachePtr;
    private AtomicInteger refCount = new AtomicInteger(0);

    /**
     * Constructor
     * @param directoryPath The directory path
     * @param files The file metadata collection
     */
    public DatafusionReader(String directoryPath, Collection<FileMetadata> files) {
        this.directoryPath = directoryPath;
        this.files = files;
        String[] fileNames = Objects.isNull(files) ? new String[]{"hits_data.parquet"} : files.stream().map(FileMetadata::fileName).toArray(String[]::new);
        this.cachePtr = DataFusionQueryJNI.createDatafusionReader(directoryPath, fileNames);
        incRef();
    }

    /**
     * Gets the cache pointer.
     * @return the cache pointer
     */
    public long getCachePtr() {
        return cachePtr;
    }

    /**
     * Increments the reference count.
     */
    public void incRef() {
        refCount.getAndIncrement();
    }

    /**
     * Decrements the reference count.
     * @throws IOException if an I/O error occurs
     */
    public void decRef() throws IOException {
        if(refCount.get() == 0) {
            throw new IllegalStateException("Listing table has been already closed");
        }

        int currRefCount = refCount.decrementAndGet();
        if(currRefCount == 0) {
            this.close();
        }

    }

    @Override
    public void close() throws IOException {
        if(cachePtr == -1L) {
            throw new IllegalStateException("Listing table has been already closed");
        }

        closeDatafusionReader(this.cachePtr);
        this.cachePtr = -1;
    }
}
