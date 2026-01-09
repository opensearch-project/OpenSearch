/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.jni.handle.ReaderHandle;
import org.opensearch.index.engine.exec.FileStats;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * DataFusion reader for JNI operations.
 */
public class DatafusionReader implements Closeable {

    private static final Logger logger = LogManager.getLogger(DatafusionReader.class);

    private static final TimeValue FETCH_TIMEOUT = TimeValue.timeValueMillis(500);

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
     * The segment stats with doc count and file size.
     */
    private volatile Map<String, FileStats> segmentStats;

    private final CompletableFuture<Map<String, FileStats>> segmentStatsFuture;

    /**
     * Constructor
     * @param directoryPath The directory path
     * @param files The file metadata collection
     */
    public DatafusionReader(
        String directoryPath,
        CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotRef,
        Collection<WriterFileSet> files
    ) {
        this.directoryPath = directoryPath;
        this.catalogSnapshotRef = catalogSnapshotRef;
        this.files = files;
        String[] fileNames = new String[0];
        if (files != null) {
            System.out.println("Got the files!!!!!");
            fileNames = files.stream().flatMap(writerFileSet -> writerFileSet.getFiles().stream()).toArray(String[]::new);
        }
        System.out.println("File names: " + Arrays.toString(fileNames));
        System.out.println("Directory path: " + directoryPath);
        this.readerHandle = new ReaderHandle(directoryPath, fileNames, this::releaseCatalogSnapshot);
        this.segmentStatsFuture = new CompletableFuture<>();
        setupSegmentStatsCompletableFuture(segmentStatsFuture);
    }

    private CompletableFuture<Map<String, FileStats>> setupSegmentStatsCompletableFuture(CompletableFuture<Map<String, FileStats>> segmentStatsFuture) {
        NativeBridge.fetchSegmentStats(getReaderPtr(), new ActionListener<>() {
            @Override
            public void onResponse(Map<String, FileStats> map) {
                segmentStatsFuture.complete(map);
            }

            @Override
            public void onFailure(Exception e) {
                segmentStatsFuture.completeExceptionally(e);
            }
        });
        return segmentStatsFuture;
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
    }

    /**
     * Gets the reference count.
     * @return the reference count
     */
    public int getRefCount() {
        return readerHandle.getRefCount();
    }

    /**
     * Get count of docs ingested in files referenced by this reader.
     * @return Doc count
     */
    public Map<String, FileStats> fetchSegmentStats() {
        if (segmentStats != null && !segmentStats.isEmpty()) {
            return segmentStats;
        }
        CountDownLatch statsLatch = new CountDownLatch(1);
        ActionListener<Map<String, FileStats>> listener = new ActionListener<>() {
            @Override
            public void onResponse(Map<String, FileStats> statsMap) {
                segmentStats = statsMap;
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failure while fetching segment stats from datafusion reader", e);
                segmentStats = Map.of();
            }
        };
        NativeBridge.fetchSegmentStats(getReaderPtr(), new LatchedActionListener<>(listener, statsLatch));
        try {
            if (statsLatch.await(FETCH_TIMEOUT.getMillis(), TimeUnit.MILLISECONDS) == false) {
                logger.warn("Failed to fetch segment stats from datafusion reader within {} timeout", FETCH_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt status
        }
        return segmentStats;
    }

    @Override
    public void close() {
        readerHandle.close();
    }

    public void releaseCatalogSnapshot() {
        try {
            if (catalogSnapshotRef != null) {
                catalogSnapshotRef.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
