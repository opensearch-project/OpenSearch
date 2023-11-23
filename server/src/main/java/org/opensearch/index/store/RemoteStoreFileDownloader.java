/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

/**
 * Helper class to downloads files from a {@link RemoteSegmentStoreDirectory}
 * instance to a local {@link Directory} instance in parallel depending on thread
 * pool size and recovery settings.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.11.0")
public final class RemoteStoreFileDownloader {
    private final Logger logger;
    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;

    public RemoteStoreFileDownloader(ShardId shardId, ThreadPool threadPool, RecoverySettings recoverySettings) {
        this.logger = Loggers.getLogger(RemoteStoreFileDownloader.class, shardId);
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
    }

    /**
     * Copies the given segments from the remote segment store to the given
     * local directory.
     * @param source The remote directory to copy segment files from
     * @param destination The local directory to copy segment files to
     * @param toDownloadSegments The list of segment files to download
     * @param listener Callback listener to be notified upon completion
     */
    public void downloadAsync(
        CancellableThreads cancellableThreads,
        Directory source,
        Directory destination,
        Collection<String> toDownloadSegments,
        ActionListener<Void> listener
    ) {
        downloadInternal(cancellableThreads, source, destination, null, toDownloadSegments, () -> {}, listener);
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
    public void download(
        Directory source,
        Directory destination,
        Directory secondDestination,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion
    ) throws InterruptedException, IOException {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
        downloadInternal(cancellableThreads, source, destination, secondDestination, toDownloadSegments, onFileCompletion, listener);
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

    private void downloadInternal(
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
            copyOneFile(cancellableThreads, source, destination, secondDestination, queue, onFileCompletion, allFilesListener);
        }
    }

    private void copyOneFile(
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
                copyOneFile(cancellableThreads, source, destination, secondDestination, queue, onFileCompletion, listener);
            });
        }
    }
}
