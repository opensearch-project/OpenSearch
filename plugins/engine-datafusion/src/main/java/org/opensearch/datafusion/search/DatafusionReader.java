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

// JNI from java to rust
// substrait
// Harcode --> file --> register as the table with the same name
public class DatafusionReader implements Closeable {
    public String directoryPath;
    public Collection<FileMetadata> files;
    public long cachePtr;
    private AtomicInteger refCount = new AtomicInteger(0);

    public DatafusionReader(String directoryPath, Collection<FileMetadata> files) {
        this.directoryPath = directoryPath;
        this.files = files;
        String[] fileNames = Objects.isNull(files) ? new String[]{} : files.stream().map(FileMetadata::fileName).toArray(String[]::new);
        this.cachePtr = DataFusionQueryJNI.createDatafusionReader(directoryPath, fileNames);
        incRef();
    }

    public long getCachePtr() {
        return cachePtr;
    }

    public void incRef() {
        refCount.getAndIncrement();
    }

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
