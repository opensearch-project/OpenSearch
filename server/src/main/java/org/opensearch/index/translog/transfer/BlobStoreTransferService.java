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
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.ActionRunnable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeFileInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.exception.ChecksumCombinationException;
import org.opensearch.index.translog.ChannelFactory;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.common.blobstore.BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC;
import static org.opensearch.common.blobstore.transfer.RemoteTransferContainer.checksumOfChecksum;
import static org.opensearch.index.translog.transfer.TranslogTransferManager.CHECKPOINT_FILE_DATA_KEY;

/**
 * Service that handles remote transfer of translog and checkpoint files
 *
 * @opensearch.internal
 */
public class BlobStoreTransferService implements TransferService {

    private final BlobStore blobStore;
    private final ThreadPool threadPool;

    private static final int CHECKSUM_BYTES_LENGTH = 8;
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
            if (!(blobStore.blobContainer(blobPath) instanceof AsyncMultiStreamBlobContainer)) {
                uploadBlob(ThreadPool.Names.TRANSLOG_TRANSFER, fileSnapshot, blobPath, listener, writePriority);
            } else {
                uploadBlob(fileSnapshot, listener, blobPath, writePriority);
            }
        });

    }

    @Override
    public void uploadBlob(
        InputStream inputStream,
        Iterable<String> remotePath,
        String fileName,
        WritePriority writePriority,
        ActionListener<Void> listener
    ) throws IOException {
        assert remotePath instanceof BlobPath;
        BlobPath blobPath = (BlobPath) remotePath;
        final BlobContainer blobContainer = blobStore.blobContainer(blobPath);
        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            blobContainer.writeBlob(fileName, inputStream, inputStream.available(), false);
            listener.onResponse(null);
            return;
        }
        final String resourceDescription = "BlobStoreTransferService.uploadBlob(blob=\"" + fileName + "\")";
        byte[] bytes = inputStream.readAllBytes();
        long expectedChecksum = computeChecksum(bytes, resourceDescription);
        uploadBlobAsyncInternal(
            fileName,
            fileName,
            bytes.length,
            blobPath,
            writePriority,
            (size, position) -> new OffsetRangeIndexInputStream(new ByteArrayIndexInput(resourceDescription, bytes), size, position),
            expectedChecksum,
            listener,
            null
        );
    }

    // Builds a metadata map containing the Base64-encoded checkpoint file data associated with a translog file.
    static Map<String, String> buildTransferFileMetadata(InputStream metadataInputStream) throws IOException {
        Map<String, String> metadata = new HashMap<>();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[128];
            int bytesRead;
            int totalBytesRead = 0;

            while ((bytesRead = metadataInputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
                if (totalBytesRead > 1024) {
                    // We enforce a limit of 1KB on the size of the checkpoint file.
                    throw new IOException("Input stream exceeds 1KB limit");
                }
            }

            byte[] bytes = byteArrayOutputStream.toByteArray();
            String metadataString = Base64.getEncoder().encodeToString(bytes);
            metadata.put(CHECKPOINT_FILE_DATA_KEY, metadataString);
        }
        return metadata;
    }

    private void uploadBlob(
        TransferFileSnapshot fileSnapshot,
        ActionListener<TransferFileSnapshot> listener,
        BlobPath blobPath,
        WritePriority writePriority
    ) {

        try {
            ChannelFactory channelFactory = FileChannel::open;
            Map<String, String> metadata = null;
            if (fileSnapshot.getMetadataFileInputStream() != null) {
                metadata = buildTransferFileMetadata(fileSnapshot.getMetadataFileInputStream());
            }

            long contentLength;
            try (FileChannel channel = channelFactory.open(fileSnapshot.getPath(), StandardOpenOption.READ)) {
                contentLength = channel.size();
            }
            ActionListener<Void> completionListener = ActionListener.wrap(resp -> listener.onResponse(fileSnapshot), ex -> {
                logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), ex);
                listener.onFailure(new FileTransferException(fileSnapshot, ex));
            });

            Objects.requireNonNull(fileSnapshot.getChecksum());
            uploadBlobAsyncInternal(
                fileSnapshot.getName(),
                fileSnapshot.getName(),
                contentLength,
                blobPath,
                writePriority,
                (size, position) -> new OffsetRangeFileInputStream(fileSnapshot.getPath(), size, position),
                fileSnapshot.getChecksum(),
                completionListener,
                metadata
            );

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

    // package private for testing
    void uploadBlobAsyncInternal(
        String fileName,
        String remoteFileName,
        long contentLength,
        BlobPath blobPath,
        WritePriority writePriority,
        RemoteTransferContainer.OffsetRangeInputStreamSupplier inputStreamSupplier,
        long expectedChecksum,
        ActionListener<Void> completionListener,
        Map<String, String> metadata
    ) throws IOException {
        BlobContainer blobContainer = blobStore.blobContainer(blobPath);
        assert blobContainer instanceof AsyncMultiStreamBlobContainer;
        boolean remoteIntegrityEnabled = ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported();
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                fileName,
                remoteFileName,
                contentLength,
                true,
                writePriority,
                inputStreamSupplier,
                expectedChecksum,
                remoteIntegrityEnabled,
                metadata
            )
        ) {
            ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(
                remoteTransferContainer.createWriteContext(),
                completionListener
            );
        }
    }

    @Override
    public InputStream downloadBlob(Iterable<String> path, String fileName) throws IOException {
        return blobStore.blobContainer((BlobPath) path).readBlob(fileName);
    }

    @Override
    @ExperimentalApi
    public InputStreamWithMetadata downloadBlobWithMetadata(Iterable<String> path, String fileName) throws IOException {
        assert blobStore.isBlobMetadataEnabled();
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

    private static long computeChecksum(byte[] bytes, String resourceDescription) throws ChecksumCombinationException {
        long expectedChecksum;
        try (IndexInput indexInput = new ByteArrayIndexInput(resourceDescription, bytes)) {
            expectedChecksum = checksumOfChecksum(indexInput, CHECKSUM_BYTES_LENGTH);
        } catch (Exception e) {
            throw new ChecksumCombinationException(
                "Potentially corrupted file: Checksum combination failed while combining stored checksum "
                    + "and calculated checksum of stored checksum",
                resourceDescription,
                e
            );
        }
        return expectedChecksum;
    }

}
