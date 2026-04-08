/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.DiskIoBufferPool;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.indexinput.BlockFetchRequest;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * This class is responsible for managing download of blocks from remote storage.
 * It uses a ThreadPool to download blocks in parallel.
 * The methods in this class are thread safe.
 */
public class BlockTransferManager {
    private static final Logger logger = LogManager.getLogger(BlockTransferManager.class);
    private static final String REMOTE_DOWNLOAD = "remote_download";
    private static final int TIMEOUT_ONE_HOUR = 1;
    private static final String TMP_BLOCK_FILE_EXTENSION = ".part";
    private final TransferManager.StreamReader streamReader;
    protected final Supplier<ThreadPool> threadPoolSupplier;
    private final ConcurrentHashMap<Path, ActionListener<Void>> downloadsInProgress = new ConcurrentHashMap<>();
    private final IndexSettings indexSettings;

    public BlockTransferManager(TransferManager.StreamReader streamReader, IndexSettings indexSettings, Supplier<ThreadPool> threadPoolSupplier) {
        this.streamReader = streamReader;
        this.indexSettings = indexSettings;
        this.threadPoolSupplier = threadPoolSupplier;
    }

    /**
     * This method fetches blocks from remote storage in parallel using the threadPool.
     * It uses a PlainActionFuture to wait for all the downloads to complete.
     * @param blockFetchRequests
     * @throws IOException
     * @throws InterruptedException
     */
    public void fetchBlocksAsync(List<BlockFetchRequest> blockFetchRequests) throws IOException, InterruptedException {
        final PlainActionFuture<Collection<Void>> listener = PlainActionFuture.newFuture();
        final ActionListener<Void> allFilesListener = new GroupedActionListener<>(listener, blockFetchRequests.size());
        for (BlockFetchRequest blockFetchRequest : blockFetchRequests) {
            downloadBlob(allFilesListener, blockFetchRequest);
        }

        try {
            listener.get(TIMEOUT_ONE_HOUR, TimeUnit.HOURS);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            // If the blocking call on the PlainActionFuture itself is interrupted, then we must
            // cancel the asynchronous work we were waiting on
            Thread.currentThread().interrupt();
            throw e;
        }

    }

    private void downloadBlob(ActionListener<Void> listener, BlockFetchRequest blockFetchRequest) {
        logger.debug("Fetching block request: {}", blockFetchRequest);
        Path filePath = blockFetchRequest.getFilePath();

        ActionListener<Void> effectiveListener = registerDownloadRequest(filePath, listener);
        if (effectiveListener != listener) {
            /**
             * Download already in progress. This edge case is not something that we do not expect based on current design
             * but still handling.
             */
            return;
        }

        submitDownloadTask(blockFetchRequest, filePath);
    }

    private ActionListener<Void> registerDownloadRequest(Path filePath, ActionListener<Void> listener) {
        Path tempFilePath = getTempFilePath(filePath);

        return downloadsInProgress.compute(filePath, (key, existingListener) -> {
            if (existingListener == null) {
                if (Files.exists(filePath)) {
                    listener.onResponse(null);
                    return null;
                }
                cleanupTempFile(tempFilePath); // One scenario where this can help, is if the OS process crashes which file download is happening.
                return listener;
            } else {
                logger.debug("Download already in progress for file: {}. Waiting for ongoing download to complete.",
                        filePath.toString());
                return chainListeners(existingListener, listener);
            }
        });
    }

    private void submitDownloadTask(BlockFetchRequest blockFetchRequest, Path filePath) {
        try {
            logger.debug("Submitting BlockFetchRequest to threadpool [{}]", blockFetchRequest.toString());
            threadPoolSupplier.get().executor(REMOTE_DOWNLOAD).submit(() -> performDownload(blockFetchRequest, filePath));
        } catch (Exception e) {
            notifyDownloadFailure(filePath, e);
        }
    }

    @SuppressForbidden(reason = "Channel#write")
    private void performDownload(BlockFetchRequest blockFetchRequest, Path filePath) {
        Path tempFilePath = getTempFilePath(filePath);

        try (InputStream remoteFileInputStream = streamReader.read(
                blockFetchRequest.getFileName(),
                blockFetchRequest.getBlockStart(),
                blockFetchRequest.getBlockSize());
             ReadableByteChannel inputChannel = Channels.newChannel(remoteFileInputStream);
             FileChannel outputChannel = FileChannel.open(tempFilePath,
                     StandardOpenOption.CREATE_NEW,
                     StandardOpenOption.WRITE)) {

            ByteBuffer directBuffer = DiskIoBufferPool.getIoBuffer();
            while (inputChannel.read(directBuffer) != -1) {
                directBuffer.flip();
                while (directBuffer.hasRemaining()) {
                    outputChannel.write(directBuffer);
                }
                directBuffer.clear();
            }
            outputChannel.force(true); // Ensure data is written to disk
            Files.move(tempFilePath, filePath, StandardCopyOption.ATOMIC_MOVE);
            notifyDownloadSuccess(filePath);
        } catch (Exception e) {
            cleanupTempFile(tempFilePath);
            notifyDownloadFailure(filePath, e);
        }
    }

    private Path getTempFilePath(Path filePath) {
        return filePath.getParent().resolve(filePath.getFileName() + TMP_BLOCK_FILE_EXTENSION);
    }

    private void cleanupTempFile(Path tempFilePath) {
        if (Files.exists(tempFilePath)) {
            try {
                logger.debug("Cleaning temp file: {}", tempFilePath.toString());
                Files.delete(tempFilePath);
            } catch (IOException e) {
                logger.error("Failed to delete existing temp file: {}", tempFilePath, e);
            }
        }
    }

    private ActionListener<Void> chainListeners(ActionListener<Void> existing, ActionListener<Void> newListener) {
        return ActionListener.wrap(
            v -> {
                existing.onResponse(v);
                newListener.onResponse(v);
                },
            e -> {
                existing.onFailure(e);
                newListener.onFailure(e);
            }
        );
    }

    private void notifyDownloadSuccess(Path filePath) {
        downloadsInProgress.compute(filePath, (key, listener) -> {
            assert listener != null : "Listener should not be null";
            listener.onResponse(null);
            return null;
        });
    }

    private void notifyDownloadFailure(Path filePath, Exception e) {
        downloadsInProgress.compute(filePath, (key, listener) -> {
            assert listener != null : "Listener should not be null";
            listener.onFailure(e);
            return null;
        });
    }
}
