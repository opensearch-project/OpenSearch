/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote.transfer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

/**
 * Manages file downloads from the remote store and from a {@link RemoteSegmentStoreDirectory}
 * instance to a local {@link Directory} instance.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.13.0")
public class DownloadManager {

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;
    private static final Logger logger = LogManager.getLogger(DownloadManager.class);

    public DownloadManager(ThreadPool threadPool, RecoverySettings recoverySettings) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
    }

    public DownloadManager(DownloadManager other) {
        this.threadPool = other.threadPool;
        this.recoverySettings = other.recoverySettings;
    }

    /**
     * Downloads a file stored in the blob container into memory
     * @param blobContainer location of the blob
     * @param fileName name of the blob to be downloaded
     * @return in-memory byte representation of the blob
     * @throws IOException exception when reading from the repository file
     */
    public byte[] downloadFileFromRemoteStoreToMemory(BlobContainer blobContainer, String fileName) throws IOException {
        // TODO: Add a circuit breaker check before putting all the data in heap
        try (InputStream inputStream = blobContainer.readBlob(fileName)) {
            return inputStream.readAllBytes();
        }
    }

    /**
     * Downloads a file stored within the blob container onto local disk specified using {@link Path} based location.
     * @param blobContainer location of the blob
     * @param fileName name of the blob to be downloaded
     * @param location location on disk where the blob will be downloaded
     * @throws IOException exception when reading from the repository file or writing to disk
     */
    public void downloadFileFromRemoteStore(BlobContainer blobContainer, String fileName, Path location) throws IOException {
        Path filePath = location.resolve(fileName);
        // Here, we always override the existing file if present.
        // We need to change this logic when we introduce incremental download
        Files.deleteIfExists(filePath);

        try (InputStream inputStream = blobContainer.readBlob(fileName)) {
            Files.copy(inputStream, filePath);
        }
    }

    /**
     * Copies the given segments from the remote segment store to the given
     * local directory.
     * @param source The remote directory to copy segment files from
     * @param destination The local directory to copy segment files to
     * @param toDownloadSegments The list of segment files to download
     * @param listener Callback listener to be notified upon completion
     */
    public void copySegmentsFromRemoteStoreAsync(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        Collection<String> toDownloadSegments,
        ActionListener<Void> listener
    ) {
        copySegmentsInternal(cancellableThreads, source, destination, null, toDownloadSegments, () -> {}, listener);
    }

    /**
     * Copies the given segments from the remote segment store to the given
     * local directory, while also copying the segments _to_ another remote directory.
     * @param source The remote directory to copy segment files from
     * @param destination The local directory to copy segment files to
     * @param secondDestination The second remote directory that segment files are
     *                          copied to after being copied to the local directory
     * @param toDownloadSegments The list of segment files to download
     * @param onFileCompletion A generic runnable that is invoked after each file download.
     *                         Must be thread safe as this may be invoked concurrently from
     *                         different threads.
     */
    public void copySegmentsFromRemoteStore(
        Directory source,
        Directory destination,
        Directory secondDestination,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion
    ) throws InterruptedException, IOException {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
        copySegmentsInternal(cancellableThreads, source, destination, secondDestination, toDownloadSegments, onFileCompletion, listener);
        try {
            listener.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            // If the blocking call on the PlainActionFuture itself is interrupted, then we must
            // cancel the asynchronous work we were waiting on
            cancellableThreads.cancel(e.getMessage());
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private void copySegmentsInternal(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        @Nullable Directory secondDestination,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion,
        ActionListener<Void> listener
    ) {
        final Queue<String> queue = new ConcurrentLinkedQueue<>(toDownloadSegments);
        // Choose the minimum of:
        // - number of files to download
        // - max thread pool size
        // - "indices.recovery.max_concurrent_remote_store_streams" setting
        final int threads = Math.min(
            toDownloadSegments.size(),
            Math.min(threadPool.info(ThreadPool.Names.REMOTE_RECOVERY).getMax(), recoverySettings.getMaxConcurrentRemoteStoreStreams())
        );
        logger.trace("Starting download of {} files with {} threads", queue.size(), threads);
        final ActionListener<Void> allFilesListener = new GroupedActionListener<>(ActionListener.map(listener, r -> null), threads);
        for (int i = 0; i < threads; i++) {
            copyFileFromRemoteStoreDirectory(
                cancellableThreads,
                source,
                destination,
                secondDestination,
                queue,
                onFileCompletion,
                allFilesListener
            );
        }
    }

    private void copyFileFromRemoteStoreDirectory(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        @Nullable Directory secondDestination,
        Queue<String> queue,
        Runnable onFileCompletion,
        ActionListener<Void> listener
    ) {
        final String file = queue.poll();
        if (file == null) {
            // Queue is empty, so notify listener we are done
            listener.onResponse(null);
        } else {
            threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY).submit(() -> {
                logger.trace("Downloading file {}", file);
                try {
                    cancellableThreads.executeIO(() -> {
                        destination.copyFrom(source, file, file, IOContext.DEFAULT);
                        onFileCompletion.run();
                        if (secondDestination != null) {
                            secondDestination.copyFrom(destination, file, file, IOContext.DEFAULT);
                        }
                    });
                } catch (Exception e) {
                    // Clear the queue to stop any future processing, report the failure, then return
                    queue.clear();
                    listener.onFailure(e);
                    return;
                }
                copyFileFromRemoteStoreDirectory(
                    cancellableThreads,
                    source,
                    destination,
                    secondDestination,
                    queue,
                    onFileCompletion,
                    listener
                );
            });
        }
    }
}
