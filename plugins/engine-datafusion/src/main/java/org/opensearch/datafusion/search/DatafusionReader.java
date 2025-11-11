/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.datafusion.DataFusionQueryJNI;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
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
    public Collection<WriterFileSet> files;
    /**
     * The cache pointer.
     */
    public long cachePtr;
    private AtomicInteger refCount = new AtomicInteger(1);

    /**
     * Constructor
     * @param directoryPath The directory path
     * @param files The file metadata collection
     */
    public DatafusionReader(String directoryPath, Collection<WriterFileSet> files) {
        this.directoryPath = directoryPath;
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

        this.cachePtr = DataFusionQueryJNI.createDatafusionReader(directoryPath, fileNames);
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
        closeDatafusionReader(cachePtr);
        this.cachePtr = -1;
    }
}
