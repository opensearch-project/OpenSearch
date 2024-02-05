/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.exception.ChecksumCombinationException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.opensearch.common.blobstore.transfer.RemoteTransferContainer.checksumOfChecksum;

/**
 * A {@code RemoteDirectory} provides an abstraction layer for storing a list of files to a remote store.
 * A remoteDirectory contains only files (no sub-folder hierarchy). This class does not support all the methods in
 * the Directory interface. Currently, it contains implementation of methods which are used to copy files to/from
 * the remote store. Implementation of remaining methods will be added as remote store is integrated with
 * replication, peer recovery etc.
 *
 * @opensearch.internal
 */
public class RemoteDirectory extends Directory {

    protected final BlobContainer blobContainer;
    private static final Logger logger = LogManager.getLogger(RemoteDirectory.class);

    private final UnaryOperator<OffsetRangeInputStream> uploadRateLimiter;

    private final UnaryOperator<InputStream> downloadRateLimiter;

    /**
     * Number of bytes in the segment file to store checksum
     */
    private static final int SEGMENT_CHECKSUM_BYTES = 8;

    public BlobContainer getBlobContainer() {
        return blobContainer;
    }

    public RemoteDirectory(BlobContainer blobContainer) {
        this(blobContainer, UnaryOperator.identity(), UnaryOperator.identity());
    }

    public RemoteDirectory(
        BlobContainer blobContainer,
        UnaryOperator<OffsetRangeInputStream> uploadRateLimiter,
        UnaryOperator<InputStream> downloadRateLimiter
    ) {
        this.blobContainer = blobContainer;
        this.uploadRateLimiter = uploadRateLimiter;
        this.downloadRateLimiter = downloadRateLimiter;
    }

    /**
     * Returns names of all files stored in this directory. The output must be in sorted (UTF-16,
     * java's {@link String#compareTo}) order.
     */
    @Override
    public String[] listAll() throws IOException {
        return blobContainer.listBlobs().keySet().stream().sorted().toArray(String[]::new);
    }

    /**
     * Returns names of files with given prefix in this directory.
     * @param filenamePrefix The prefix to match against file names in the directory
     * @return A list of the matching filenames in the directory
     * @throws IOException if there were any failures in reading from the blob container
     */
    public Collection<String> listFilesByPrefix(String filenamePrefix) throws IOException {
        return blobContainer.listBlobsByPrefix(filenamePrefix).keySet();
    }

    public List<String> listFilesByPrefixInLexicographicOrder(String filenamePrefix, int limit) throws IOException {
        List<String> sortedBlobList = new ArrayList<>();
        AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<List<BlobMetadata>> actionListener = new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(List<BlobMetadata> blobMetadata) {
                sortedBlobList.addAll(blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList()));
            }

            @Override
            public void onFailure(Exception e) {
                exception.set(e);
            }
        }, latch);

        try {
            blobContainer.listBlobsByPrefixInSortedOrder(
                filenamePrefix,
                limit,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC,
                actionListener
            );
            latch.await();
        } catch (InterruptedException e) {
            throw new IOException("Exception in listFilesByPrefixInLexicographicOrder with prefix: " + filenamePrefix, e);
        }
        if (exception.get() != null) {
            throw new IOException(exception.get());
        } else {
            return sortedBlobList;
        }
    }

    /**
     * Returns stream emitted from by blob object. Should be used with a closeable block.
     *
     * @param fileName Name of file
     * @return Stream from the blob object
     * @throws IOException if fetch of stream fails with IO error
     */
    public InputStream getBlobStream(String fileName) throws IOException {
        return blobContainer.readBlob(fileName);
    }

    /**
     * Removes an existing file in the directory.
     *
     * <p>This method will not throw an exception when the file doesn't exist and simply ignores this case.
     * This is a deviation from the {@code Directory} interface where it is expected to throw either
     * {@link NoSuchFileException} or {@link FileNotFoundException} if {@code name} points to a non-existing file.
     *
     * @param name the name of an existing file.
     * @throws IOException if the file exists but could not be deleted.
     */
    @Override
    public void deleteFile(String name) throws IOException {
        // ToDo: Add a check for file existence
        blobContainer.deleteBlobsIgnoringIfNotExists(Collections.singletonList(name));
    }

    /**
     * Creates and returns a new instance of {@link RemoteIndexOutput} which will be used to copy files to the remote
     * store.
     *
     * <p> In the {@link Directory} interface, it is expected to throw {@link java.nio.file.FileAlreadyExistsException}
     * if the file already exists in the remote store. As this method does not open a file, it does not throw the
     * exception.
     *
     * @param name the name of the file to copy to remote store.
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return new RemoteIndexOutput(name, blobContainer);
    }

    /**
     * Opens a stream for reading an existing file and returns {@link RemoteIndexInput} enclosing the stream.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return openInput(name, fileLength(name), context);
    }

    public IndexInput openInput(String name, long fileLength, IOContext context) throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = blobContainer.readBlob(name);
            return new RemoteIndexInput(name, downloadRateLimiter.apply(inputStream), fileLength);
        } catch (Exception e) {
            // Incase the RemoteIndexInput creation fails, close the input stream to avoid file handler leak.
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            logger.error("Exception while reading blob for file: " + name + " for path " + blobContainer.path());
            throw e;
        }
    }

    /**
     * Closes the remote directory. Currently, it is a no-op.
     * If remote directory maintains a state in future, we need to clean it before closing the directory
     */
    @Override
    public void close() throws IOException {
        // Do nothing
    }

    /**
     * Returns the byte length of a file in the directory.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     * @throws NoSuchFileException if the file does not exist
     */
    @Override
    public long fileLength(String name) throws IOException {
        // ToDo: Instead of calling remote store each time, keep a cache with segment metadata
        List<BlobMetadata> metadata = blobContainer.listBlobsByPrefixInSortedOrder(name, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
        if (metadata.size() == 1 && metadata.get(0).name().equals(name)) {
            return metadata.get(0).length();
        }
        throw new NoSuchFileException(name);
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Once soft deleting is supported segment files in the remote store, this method will provide details of
     * number of files marked as deleted but not actually deleted from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Set<String> getPendingDeletions() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Temporary IndexOutput is not required while working with Remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Segment upload to the remote store will be permanent and does not require a separate sync API.
     * This may change in the future if segment upload to remote store happens via cache and we need sync API to write
     * the cache contents to the store permanently.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void sync(Collection<String> names) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Once metadata to be stored with each shard is finalized, syncMetaData method will be used to sync the directory
     * metadata to the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void syncMetaData() {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * As this method is used by IndexWriter to publish commits, the implementation of this method is required when
     * IndexWriter is backed by RemoteDirectory.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void rename(String source, String dest) throws IOException {
        throw new UnsupportedOperationException();

    }

    /**
     * Guaranteed to throw an exception and leave the directory unmodified.
     * Once locking segment files in remote store is supported, implementation of this method is required with
     * remote store specific LockFactory.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Lock obtainLock(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void delete() throws IOException {
        blobContainer.delete();
    }

    public boolean copyFrom(
        Directory from,
        String src,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) {
        if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
            try {
                uploadBlob(from, src, remoteFileName, context, postUploadRunner, listener, lowPriorityUpload);
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return true;
        }
        return false;
    }

    private void uploadBlob(
        Directory from,
        String src,
        String remoteFileName,
        IOContext ioContext,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    ) throws Exception {
        long expectedChecksum = calculateChecksumOfChecksum(from, src);
        long contentLength;
        try (IndexInput indexInput = from.openInput(src, ioContext)) {
            contentLength = indexInput.length();
        }
        boolean remoteIntegrityEnabled = false;
        if (getBlobContainer() instanceof AsyncMultiStreamBlobContainer) {
            remoteIntegrityEnabled = ((AsyncMultiStreamBlobContainer) getBlobContainer()).remoteIntegrityCheckSupported();
        }
        RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
            src,
            remoteFileName,
            contentLength,
            true,
            lowPriorityUpload ? WritePriority.LOW : WritePriority.NORMAL,
            (size, position) -> uploadRateLimiter.apply(new OffsetRangeIndexInputStream(from.openInput(src, ioContext), size, position)),
            expectedChecksum,
            remoteIntegrityEnabled
        );
        ActionListener<Void> completionListener = ActionListener.wrap(resp -> {
            try {
                postUploadRunner.run();
                listener.onResponse(null);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Exception in segment postUpload for file [{}]", src), e);
                listener.onFailure(e);
            }
        }, ex -> {
            logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", src), ex);
            IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(ex);
            if (corruptIndexException != null) {
                listener.onFailure(corruptIndexException);
                return;
            }
            Throwable throwable = ExceptionsHelper.unwrap(ex, CorruptFileException.class);
            if (throwable != null) {
                CorruptFileException corruptFileException = (CorruptFileException) throwable;
                listener.onFailure(new CorruptIndexException(corruptFileException.getMessage(), corruptFileException.getFileName()));
                return;
            }
            listener.onFailure(ex);
        });

        completionListener = ActionListener.runBefore(completionListener, () -> {
            try {
                remoteTransferContainer.close();
            } catch (Exception e) {
                logger.warn("Error occurred while closing streams", e);
            }
        });

        WriteContext writeContext = remoteTransferContainer.createWriteContext();
        ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(writeContext, completionListener);
    }

    private long calculateChecksumOfChecksum(Directory directory, String file) throws IOException {
        try (IndexInput indexInput = directory.openInput(file, IOContext.DEFAULT)) {
            try {
                return checksumOfChecksum(indexInput, SEGMENT_CHECKSUM_BYTES);
            } catch (Exception e) {
                throw new ChecksumCombinationException(
                    "Potentially corrupted file: Checksum combination failed while combining stored checksum "
                        + "and calculated checksum of stored checksum in segment file: "
                        + file
                        + ", directory: "
                        + directory,
                    file,
                    e
                );
            }
        }
    }
}
