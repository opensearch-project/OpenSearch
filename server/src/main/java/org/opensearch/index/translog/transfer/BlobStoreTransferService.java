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
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRunnable;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.VerifyingMultiStreamBlobContainer;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeFileInputStream;
import org.opensearch.index.translog.ChannelFactory;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.common.blobstore.BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC;

/**
 * Service that handles remote transfer of translog and checkpoint files
 *
 * @opensearch.internal
 */
public class BlobStoreTransferService implements TransferService {

    private final BlobStore blobStore;
    private final ThreadPool threadPool;

    private static final Logger logger = LogManager.getLogger(BlobStoreTransferService.class);

    public BlobStoreTransferService(BlobStore blobStore, ThreadPool threadPool) {
        this.blobStore = blobStore;
        this.threadPool = threadPool;
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
        fileSnapshots.forEach(fileSnapshot -> {
            BlobPath blobPath = blobPaths.get(fileSnapshot.getPrimaryTerm());
            if (!(blobStore.blobContainer(blobPath) instanceof VerifyingMultiStreamBlobContainer)) {
                uploadBlob(ThreadPool.Names.TRANSLOG_TRANSFER, fileSnapshot, blobPath, listener, writePriority);
            } else {
                uploadBlob(fileSnapshot, listener, blobPath, writePriority);
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
            ChannelFactory channelFactory = FileChannel::open;
            long contentLength;
            try (FileChannel channel = channelFactory.open(fileSnapshot.getPath(), StandardOpenOption.READ)) {
                contentLength = channel.size();
            }
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                fileSnapshot.getName(),
                fileSnapshot.getName(),
                contentLength,
                true,
                writePriority,
                (size, position) -> new OffsetRangeFileInputStream(fileSnapshot.getPath(), size, position),
                Objects.requireNonNull(fileSnapshot.getChecksum()),
                blobStore.blobContainer(blobPath) instanceof VerifyingMultiStreamBlobContainer
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
            ((VerifyingMultiStreamBlobContainer) blobStore.blobContainer(blobPath)).asyncBlobUpload(writeContext, completionListener);

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
