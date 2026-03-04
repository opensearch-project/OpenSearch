/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DataFusion reader for JNI operations.
 * <p>
 * Wraps a native reader pointer with reference counting. Each reader
 * represents a point-in-time snapshot of parquet/arrow files for a shard.
 * Created from a catalog snapshot during refresh; closed when ref count
 * reaches zero.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReader implements Closeable {

    private static final Logger logger = LogManager.getLogger(DatafusionReader.class);

    private final String directoryPath;
    // TODO: replace with Collection<WriterFileSet> when WriterFileSet is on classpath
    private final Collection<String> fileNames;
    private final AtomicInteger refCount = new AtomicInteger(1);

    // TODO: replace with ReaderHandle (JNI native handle with ref counting)
    // private final ReaderHandle readerHandle;
    private final long readerPtr;

    // TODO: add CatalogSnapshot reference for lifecycle management
    // private final CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotRef;

    // TODO: cached segment stats (doc count, file size per segment)
    // private volatile Map<String, FileStats> segmentStats;

    /**
     * @param directoryPath shard data directory
     * @param fileNames     file names from the catalog snapshot
     */
    public DatafusionReader(String directoryPath, Collection<String> fileNames) {
        this.directoryPath = directoryPath;
        this.fileNames = fileNames;

        // TODO: create native reader via ReaderHandle
        // String[] fileNames = files.stream()
        // .flatMap(wfs -> wfs.getFiles().stream())
        // .toArray(String[]::new);
        // this.readerHandle = new ReaderHandle(directoryPath, fileNames, this::releaseCatalogSnapshot);
        this.readerPtr = 0L; // placeholder
    }

    public long getReaderPtr() {
        // TODO: return readerHandle.getPointer();
        return readerPtr;
    }

    public String getDirectoryPath() {
        return directoryPath;
    }

    public Collection<String> getFiles() {
        return fileNames;
    }

    public void incRef() {
        // TODO: readerHandle.retain();
        refCount.incrementAndGet();
    }

    public boolean decRef() {
        // TODO: readerHandle.close(); (ref-counted close)
        return refCount.decrementAndGet() == 0;
    }

    public int getRefCount() {
        // TODO: return readerHandle.getRefCount();
        return refCount.get();
    }

    // TODO: fetch segment stats (doc count, file size) from native reader
    // public Map<String, FileStats> fetchSegmentStats() {
    // if (segmentStats != null && !segmentStats.isEmpty()) return segmentStats;
    // NativeBridge.fetchSegmentStats(getReaderPtr(), listener);
    // return segmentStats;
    // }

    @Override
    public void close() throws IOException {
        if (decRef()) {
            // TODO: native cleanup via readerHandle.close()
            // TODO: releaseCatalogSnapshot()
            logger.debug("DatafusionReader closed for [{}]", directoryPath);
        }
    }

    // TODO: release catalog snapshot when reader is fully closed
    // private void releaseCatalogSnapshot() {
    // if (catalogSnapshotRef != null) catalogSnapshotRef.close();
    // }
}
