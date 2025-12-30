/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.store.CompositeRemoteSegmentStoreDirectory;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * CompositeStoreDirectory wrapper that records copy process for replication statistics.
 * Similar to ReplicationStatsDirectoryWrapper but designed for CompositeStoreDirectory's format-aware operations.
 *
 * This wrapper intercepts copyFrom operations and tracks progress for replication statistics,
 * while delegating all actual directory operations to the underlying CompositeStoreDirectory.
 *
 * @opensearch.internal
 */
public final class CompositeStoreDirectoryStatsWrapper extends SegmentReplicationSource.ReplicationStatsDirectoryWrapper {
    private final CompositeStoreDirectory delegate;
    private final BiConsumer<String, Long> fileProgressTracker;

    public CompositeStoreDirectoryStatsWrapper(CompositeStoreDirectory delegate, BiConsumer<String, Long> fileProgressTracker) {
        super(delegate, fileProgressTracker);
        this.delegate = delegate;
        this.fileProgressTracker = fileProgressTracker;
    }

    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        copyFrom(new FileMetadata(src), (RemoteSegmentStoreDirectory) from, context);
    }

    /**
     * Copies a file from source directory with format-agnostic progress tracking.
     * This method is format-aware and uses callback-based progress tracking instead of FilterDirectory.
     */
    public void copyFrom(FileMetadata fileMetadata, RemoteSegmentStoreDirectory from, IOContext context) throws IOException {
        String fileName = fileMetadata.file();

        try {
            // Get file size for progress tracking
            long fileSize = from.fileLength(fileMetadata.serialize());

            // Report start of copy operation
            fileProgressTracker.accept(fileName, 0L);

            // Delegate the actual format-aware copy to CompositeStoreDirectory
            // CompositeStoreDirectory will route based on fileMetadata.dataFormat()
            delegate.copyFrom(fileMetadata, from, context);

            // Report completion of copy operation
            fileProgressTracker.accept(fileName, fileSize);

        } catch (IOException e) {
            // Report failure - no bytes were successfully transferred
            fileProgressTracker.accept(fileName, 0L);
            throw e;
        }
    }

    /**
     * Legacy copyFrom method for backward compatibility with existing download APIs.
     * Converts String filenames to FileMetadata with default "lucene" format.
     */
    public void copyFrom(CompositeRemoteSegmentStoreDirectory from, String src, String dest, IOContext context) throws IOException {
        // Convert to FileMetadata with default format for backward compatibility
        FileMetadata destFileMetadata = new FileMetadata("lucene", dest);
        copyFrom(destFileMetadata, from, context);
    }

    /**
     * Gets the underlying CompositeStoreDirectory for direct access when needed.
     */
    public CompositeStoreDirectory getCompositeStoreDirectory() {
        return delegate;
    }

    @Override
    public String toString() {
        return "CompositeStoreDirectoryStatsWrapper(" + delegate.toString() + ")";
    }
}
