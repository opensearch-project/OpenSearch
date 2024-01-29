/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote.transfer;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A delegating wrapper class of {@link DownloadManager} for keeping track of download stats and metric reporting.
 */
public class StatsTrackingDownloadManager extends DownloadManager {

    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;

    public StatsTrackingDownloadManager(
        ThreadPool threadPool,
        RecoverySettings recoverySettings,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker
    ) {
        super(threadPool, recoverySettings);
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
    }

    public StatsTrackingDownloadManager(DownloadManager downloadManager, RemoteTranslogTransferTracker remoteTranslogTransferTracker) {
        super(downloadManager);
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
    }

    /**
     * Tracks the download time and size when fetching an object from the container to memory
     * @param blobContainer location of the blob
     * @param fileName name of the blob to be downloaded
     * @return in-memory byte representation of the file
     * @throws IOException exception on reading blob from container
     */
    @Override
    public byte[] downloadFileFromRemoteStoreToMemory(BlobContainer blobContainer, String fileName) throws IOException {
        // TODO: Rewrite stats logic to remove hard-wiring to translog transfer tracker
        // (maybe make RemoteTranslogTransferTracker methods interface dependent?)
        long downloadStartTime = System.nanoTime();
        byte[] data = null;
        try {
            data = super.downloadFileFromRemoteStoreToMemory(blobContainer, fileName);
        } finally {
            remoteTranslogTransferTracker.addDownloadTimeInMillis((System.nanoTime() - downloadStartTime) / 1_000_000L);
            if (data != null) {
                remoteTranslogTransferTracker.addDownloadBytesSucceeded(data.length);
            }
        }
        return data;
    }

    /**
     * Tracks the download time and size when fetching an object from the container to a local file
     * @param blobContainer location of the blob
     * @param fileName name of the blob to be downloaded
     * @param location location on disk where the blob will be downloaded
     * @throws IOException exception on reading blob from container or writing to disk
     */
    @Override
    public void downloadFileFromRemoteStore(BlobContainer blobContainer, String fileName, Path location) throws IOException {
        // TODO: Rewrite stats logic to remove hard-wiring to translog transfer tracker
        boolean downloadStatus = false;
        long bytesToRead = 0, downloadStartTime = System.nanoTime();
        try {
            super.downloadFileFromRemoteStore(blobContainer, fileName, location);
            bytesToRead = Files.size(location.resolve(fileName));
            downloadStatus = true;
        } finally {
            remoteTranslogTransferTracker.addDownloadTimeInMillis((System.nanoTime() - downloadStartTime) / 1_000_000L);
            if (downloadStatus) {
                remoteTranslogTransferTracker.addDownloadBytesSucceeded(bytesToRead);
            }
        }

    }
}
