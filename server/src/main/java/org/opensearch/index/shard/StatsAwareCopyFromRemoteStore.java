/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;

/**
 * Copies over files from a remote store and populates remote store stats accordingly
 *
 * @opensearch.internal
 */
public class StatsAwareCopyFromRemoteStore {
    private final RemoteStoreDownloadStatsListener downloadStatsListener;

    public StatsAwareCopyFromRemoteStore(RemoteSegmentTransferTracker downloadStatsTracker) {
        this.downloadStatsListener = new RemoteStoreDownloadStatsListener(downloadStatsTracker);
    }

    /**
     * Encapsulates the copyFrom method and populates remote store segment download stats
     * @param storeDirectory: Destination directory implementation to copy segments to
     * @param remoteDirectory: Source {@link RemoteSegmentStoreDirectory} to copy segments from
     * @param srcFile: Source file name
     * @param destFile: Source file name
     * @param fileSize: Size of the file being downloaded
     * @param ioContext: {@link IOContext} object to be used in {@link Directory#copyFrom} method
     * @throws IOException if an error occurs in copying files
     */
    public void performCopy(
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        String srcFile,
        String destFile,
        long fileSize,
        IOContext ioContext
    ) throws IOException {
        downloadStatsListener.updateFileSize(fileSize);
        downloadStatsListener.beforeDownload();
        boolean copySuccessful = false;
        try {
            storeDirectory.copyFrom(remoteDirectory, srcFile, destFile, ioContext);
            copySuccessful = true;
            downloadStatsListener.afterDownload();
        } finally {
            if (!copySuccessful) {
                downloadStatsListener.downloadFailed();
            }
        }
    }

    /**
     * Implementation of {@link SegmentDownloadListener} to populate remote store state related to segment downloads
     *
     * @opensearch.internal
     */
    public static class RemoteStoreDownloadStatsListener implements SegmentDownloadListener {
        private final RemoteSegmentTransferTracker downloadStatsTracker;
        private long startTimeInMs;
        private long fileSize;

        public RemoteStoreDownloadStatsListener(RemoteSegmentTransferTracker downloadStatsTracker) {
            this.downloadStatsTracker = downloadStatsTracker;
        }

        /**
         * Updates the current segment file's size.
         * This would be used to update the tracker values
         * @param fileSize: Size of the incoming segment file
         */
        public void updateFileSize(long fileSize) {
            this.fileSize = fileSize;
        }

        /**
         * Updates the amount of bytes attempted for download
         */
        @Override
        public void beforeDownload() {
            startTimeInMs = System.currentTimeMillis();
            downloadStatsTracker.addDownloadBytesStarted(fileSize);
        }

        /**
         * Updates
         * - The amount of bytes that has been successfully downloaded from the remote store
         * - The last successful download completion timestamp
         * - The last successfully downloaded segment size
         * - The speed of segment download (in bytes/sec)
         */
        @Override
        public void afterDownload() {
            downloadStatsTracker.addDownloadBytesSucceeded(fileSize);
            downloadStatsTracker.updateLastDownloadedSegmentSize(fileSize);
            long currentTimeInMs = System.currentTimeMillis();
            downloadStatsTracker.updateLastDownloadTimestampMs(currentTimeInMs);
            long timeTakenInMS = Math.max(1, currentTimeInMs - startTimeInMs);
            downloadStatsTracker.addDownloadBytesPerSec((fileSize * 1_000L) / timeTakenInMS);
        }

        /**
         * Updates the amount of bytes failed in download
         */
        @Override
        public void downloadFailed() {
            downloadStatsTracker.addDownloadBytesFailed(fileSize);
        }
    }
}
