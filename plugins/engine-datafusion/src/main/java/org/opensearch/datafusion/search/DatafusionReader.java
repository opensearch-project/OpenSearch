/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.index.engine.exec.engine.FileMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DataFusion reader for JNI operations backed by listing table
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
     * The native reader pointer.
     */
    public long nativeReaderPtr;
    private AtomicInteger refCount = new AtomicInteger(0);

    /**
     * Constructor
     * @param directoryPath The directory path
     * @param files The file metadata collection
     */
    public DatafusionReader(String directoryPath, Collection<FileMetadata> files) {
        this.directoryPath = directoryPath;
        this.files = files;
        // TODO : initialize the native reader pointer
        this.nativeReaderPtr = 0;
        incRef();
    }

    /**
     * Gets the reader pointer.
     * @return the reader pointer
     */
    public long getReaderPtr() {
        return nativeReaderPtr;
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
        if (refCount.get() == 0) {
            throw new IllegalStateException("Listing table has been already closed");
        }

        int currRefCount = refCount.decrementAndGet();
        if (currRefCount == 0) {
            this.close();
        }

    }

    @Override
    public void close() throws IOException {
        if (nativeReaderPtr == -1L) {
            throw new IllegalStateException("Listing table has been already closed");
        }
        // TODO : close the native reader
        this.nativeReaderPtr = -1;
    }
}
