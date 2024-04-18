/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.FetchBlobResult;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeFileInputStream;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.ChannelFactory;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.common.blobstore.BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC;

/**
 * Service that handles remote transfer of translog and checkpoint files
 *
 * @opensearch.internal
 */
public class BlobStoreTransferService implements TransferService {

    private final BlobStore blobStore;
    private final ThreadPool threadPool;
    private ConcurrentHashMap<String, FileTransferTracker.TransferState> fileTransferTrackerCache;

    private static final Logger logger = LogManager.getLogger(BlobStoreTransferService.class);

    public BlobStoreTransferService(BlobStore blobStore, ThreadPool threadPool) {
        this.blobStore = blobStore;
        this.threadPool = threadPool;
        fileTransferTrackerCache = new ConcurrentHashMap<>();
    }

    @Override
    public ConcurrentHashMap<String, FileTransferTracker.TransferState> getFileTransferTrackerCache() {
        return fileTransferTrackerCache;
    }

    void add(String file, boolean success) {
        FileTransferTracker.TransferState targetState = success
            ? FileTransferTracker.TransferState.SUCCESS
            : FileTransferTracker.TransferState.FAILED;
        add(file, targetState);
    }

    private void add(String file, FileTransferTracker.TransferState targetState) {
        fileTransferTrackerCache.compute(file, (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + "while setting target to" + targetState);
        });
    }

    @Override
    public boolean isObjectMetadataUploadSupported() {
        return blobStore.isObjectMetadataUploadSupported();
    }

    @Override
    public void uploadBlob(
        String threadPoolName,
        final TransferFileSnapshot fileSnapshot,
        Iterable<String> remoteTransferPath,
        ActionListener<TransferFileSnapshot> listener,
        WritePriority writePriority
    ) {
        assert remoteTransferPath instanceof BlobPath;
        BlobPath blobPath = (BlobPath) remoteTransferPath;
        threadPool.executor(threadPoolName).execute(ActionRunnable.wrap(listener, l -> {
            try {
                uploadBlob(fileSnapshot, blobPath, writePriority);
                l.onResponse(fileSnapshot);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), e);
                l.onFailure(new FileTransferException(fileSnapshot, e));
            }
        }));
    }

    @Override
    public void uploadBlob(final TransferFileSnapshot fileSnapshot, Iterable<String> remoteTransferPath, WritePriority writePriority)
        throws IOException {
        BlobPath blobPath = (BlobPath) remoteTransferPath;
        try (InputStream inputStream = fileSnapshot.inputStream()) {
            blobStore.blobContainer(blobPath).writeBlobAtomic(fileSnapshot.getName(), inputStream, fileSnapshot.getContentLength(), true);
        }
    }

    @Override
    public void uploadBlobs(
        Set<TransferFileSnapshot> fileSnapshots,
        final Map<Long, BlobPath> blobPaths,
        ActionListener<TransferFileSnapshot> listener,
        WritePriority writePriority
    ) {
        boolean isObjectMetadataUploadSupported = isObjectMetadataUploadSupported();

        fileSnapshots.forEach(fileSnapshot -> {
            BlobPath blobPath = blobPaths.get(fileSnapshot.getPrimaryTerm());

            if (!isObjectMetadataUploadSupported) {

                try {
                    List<Exception> exceptionList = new ArrayList<>(2);
                    Set<TransferFileSnapshot> filesToUpload = new HashSet<>(2);

                    // translog.tlog file snapshot
                    if (fileTransferTrackerCache.get(fileSnapshot.getName()) != null
                        && !fileTransferTrackerCache.get(fileSnapshot.getName()).equals(FileTransferTracker.TransferState.SUCCESS)) {
                        filesToUpload.add(fileSnapshot);
                    }

                    // translog.ckp file snapshot
                    if (fileTransferTrackerCache.get(fileSnapshot.getMetadataFileName()) != null
                        && !fileTransferTrackerCache.get(fileSnapshot.getMetadataFileName())
                            .equals(FileTransferTracker.TransferState.FAILED)) {
                        filesToUpload.add(
                            new TransferFileSnapshot(
                                fileSnapshot.getMetadataFilePath(),
                                fileSnapshot.getPrimaryTerm(),
                                fileSnapshot.getMetadataFileChecksum()
                            )
                        );
                    }

                    if (filesToUpload.isEmpty()) {
                        listener.onResponse(fileSnapshot);
                        return;
                    }

                    final CountDownLatch latch = new CountDownLatch(filesToUpload.size());
                    LatchedActionListener<TransferFileSnapshot> latchedActionListener = new LatchedActionListener<>(
                        ActionListener.wrap(fileSnapshotResp -> add(fileSnapshotResp.getName(), true), ex -> {
                            assert ex instanceof FileTransferException;
                            logger.error(
                                () -> new ParameterizedMessage(
                                    "Exception during transfer for file {}",
                                    ((FileTransferException) ex).getFileSnapshot().getName()
                                ),
                                ex
                            );
                            FileTransferException e = (FileTransferException) ex;
                            TransferFileSnapshot file = e.getFileSnapshot();
                            add(file.getName(), false);
                            exceptionList.add(ex);
                        }),
                        latch
                    );

                    filesToUpload.forEach(separateFileSnapshot -> {
                        if (!(blobStore.blobContainer(blobPath) instanceof AsyncMultiStreamBlobContainer)) {
                            uploadBlob(
                                ThreadPool.Names.TRANSLOG_TRANSFER,
                                separateFileSnapshot,
                                blobPath,
                                latchedActionListener,
                                writePriority
                            );
                        } else {
                            logger.info("uploading file = {}", fileSnapshot.getName());
                            uploadBlob(separateFileSnapshot, latchedActionListener, blobPath, writePriority);
                        }
                    });

                    try {
                        if (latch.await(300, TimeUnit.MILLISECONDS) == false) {
                            Exception ex = new TranslogUploadFailedException(
                                "Timed out waiting for transfer of snapshot " + fileSnapshot + " to complete"
                            );
                            throw new FileTransferException(fileSnapshot, ex);
                        }
                    } catch (InterruptedException ex) {
                        Exception exception = new TranslogUploadFailedException("Failed to upload " + fileSnapshot, ex);
                        Thread.currentThread().interrupt();
                        throw new FileTransferException(fileSnapshot, exception);
                    }

                    if (fileTransferTrackerCache.get(fileSnapshot.getName()).equals(FileTransferTracker.TransferState.SUCCESS)
                        && fileTransferTrackerCache.get(fileSnapshot.getMetadataFileName())
                            .equals(FileTransferTracker.TransferState.SUCCESS)) {
                        listener.onResponse(fileSnapshot);
                    } else {
                        assert exceptionList.isEmpty() == false;
                        listener.onFailure(exceptionList.get(0));
                    }

                } catch (Exception e) {
                    throw new FileTransferException(fileSnapshot, e);
                } finally {
                    try {
                        fileSnapshot.close();
                    } catch (IOException e) {
                        logger.warn("Error while closing TransferFileSnapshot", e);
                    }
                }

            } else {
                if (!(blobStore.blobContainer(blobPath) instanceof AsyncMultiStreamBlobContainer)) {
                    uploadBlob(ThreadPool.Names.TRANSLOG_TRANSFER, fileSnapshot, blobPath, listener, writePriority);
                } else {
                    logger.info("uploading file = {}", fileSnapshot.getName());
                    uploadBlob(fileSnapshot, listener, blobPath, writePriority);
                }
            }
        });

    }

    private void uploadBlob(
        TransferFileSnapshot fileSnapshot,
        ActionListener<TransferFileSnapshot> listener,
        BlobPath blobPath,
        WritePriority writePriority
    ) {

        try {
            Map<String, String> metadata = null;
            if (isObjectMetadataUploadSupported()) {
                metadata = fileSnapshot.prepareFileMetadata();
            }

            ChannelFactory channelFactory = FileChannel::open;
            long contentLength;
            try (FileChannel channel = channelFactory.open(fileSnapshot.getPath(), StandardOpenOption.READ)) {
                contentLength = channel.size();
            }
            boolean remoteIntegrityEnabled = false;
            BlobContainer blobContainer = blobStore.blobContainer(blobPath);
            if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
                remoteIntegrityEnabled = ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported();
            }
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                fileSnapshot.getName(),
                fileSnapshot.getName(),
                contentLength,
                true,
                writePriority,
                (size, position) -> new OffsetRangeFileInputStream(fileSnapshot.getPath(), size, position),
                Objects.requireNonNull(fileSnapshot.getChecksum()),
                remoteIntegrityEnabled,
                metadata
            );
            ActionListener<Void> completionListener = ActionListener.wrap(resp -> listener.onResponse(fileSnapshot), ex -> {
                logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), ex);
                listener.onFailure(new FileTransferException(fileSnapshot, ex));
            });

            completionListener = ActionListener.runBefore(completionListener, () -> {
                try {
                    remoteTransferContainer.close();
                } catch (Exception e) {
                    logger.warn("Error occurred while closing streams", e);
                }
            });

            WriteContext writeContext = remoteTransferContainer.createWriteContext();
            ((AsyncMultiStreamBlobContainer) blobStore.blobContainer(blobPath)).asyncBlobUpload(writeContext, completionListener);

        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), e);
            listener.onFailure(new FileTransferException(fileSnapshot, e));
        } finally {
            try {
                fileSnapshot.close();
            } catch (IOException e) {
                logger.warn("Error while closing TransferFileSnapshot", e);
            }
        }

    }

    @Override
    public InputStream downloadBlob(Iterable<String> path, String fileName) throws IOException {
        return blobStore.blobContainer((BlobPath) path).readBlob(fileName);
    }

    @Override
    @ExperimentalApi
    public FetchBlobResult downloadBlobWithMetadata(Iterable<String> path, String fileName) throws IOException {
        return blobStore.blobContainer((BlobPath) path).readBlobWithMetadata(fileName);
    }

    @Override
    public void deleteBlobs(Iterable<String> path, List<String> fileNames) throws IOException {
        blobStore.blobContainer((BlobPath) path).deleteBlobsIgnoringIfNotExists(fileNames);
    }

    @Override
    public void deleteBlobsAsync(String threadpoolName, Iterable<String> path, List<String> fileNames, ActionListener<Void> listener) {
        threadPool.executor(threadpoolName).execute(() -> {
            try {
                deleteBlobs(path, fileNames);
                listener.onResponse(null);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void delete(Iterable<String> path) throws IOException {
        blobStore.blobContainer((BlobPath) path).delete();
    }

    @Override
    public void deleteAsync(String threadpoolName, Iterable<String> path, ActionListener<Void> listener) {
        threadPool.executor(threadpoolName).execute(() -> {
            try {
                delete(path);
                listener.onResponse(null);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public Set<String> listAll(Iterable<String> path) throws IOException {
        return blobStore.blobContainer((BlobPath) path).listBlobs().keySet();
    }

    @Override
    public Set<String> listFolders(Iterable<String> path) throws IOException {
        return blobStore.blobContainer((BlobPath) path).children().keySet();
    }

    @Override
    public void listFoldersAsync(String threadpoolName, Iterable<String> path, ActionListener<Set<String>> listener) {
        threadPool.executor(threadpoolName).execute(() -> {
            try {
                listener.onResponse(listFolders(path));
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

    public void listAllInSortedOrder(Iterable<String> path, String filenamePrefix, int limit, ActionListener<List<BlobMetadata>> listener) {
        blobStore.blobContainer((BlobPath) path).listBlobsByPrefixInSortedOrder(filenamePrefix, limit, LEXICOGRAPHIC, listener);
    }

    public void listAllInSortedOrderAsync(
        String threadpoolName,
        Iterable<String> path,
        String filenamePrefix,
        int limit,
        ActionListener<List<BlobMetadata>> listener
    ) {
        threadPool.executor(threadpoolName).execute(() -> { listAllInSortedOrder(path, filenamePrefix, limit, listener); });
    }

}
