/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * EncryptedBlobContainer is an encrypted BlobContainer that is backed by a
 * {@link AsyncMultiStreamBlobContainer}
 *
 * @opensearch.internal
 */
public class AsyncMultiStreamEncryptedBlobContainer<T, U> extends EncryptedBlobContainer<T, U> implements AsyncMultiStreamBlobContainer {

    private final AsyncMultiStreamBlobContainer blobContainer;
    private final CryptoHandler<T, U> cryptoHandler;

    public AsyncMultiStreamEncryptedBlobContainer(AsyncMultiStreamBlobContainer blobContainer, CryptoHandler<T, U> cryptoHandler) {
        super(blobContainer, cryptoHandler);
        this.blobContainer = blobContainer;
        this.cryptoHandler = cryptoHandler;
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        EncryptedWriteContext<T, U> encryptedWriteContext = new EncryptedWriteContext<>(writeContext, cryptoHandler);
        blobContainer.asyncBlobUpload(encryptedWriteContext, completionListener);
    }

    @Override
    public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
        try {
            final U cryptoContext = cryptoHandler.loadEncryptionMetadata(getEncryptedHeaderContentSupplier(blobName));
            ActionListener<ReadContext> decryptingCompletionListener = ActionListener.map(
                listener,
                readContext -> new DecryptedReadContext<>(readContext, cryptoHandler, cryptoContext)
            );

            blobContainer.readBlobAsync(blobName, decryptingCompletionListener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public boolean remoteIntegrityCheckSupported() {
        return false;
    }

    static class EncryptedWriteContext<T, U> extends WriteContext {

        private final T encryptionMetadata;
        private final CryptoHandler<T, U> cryptoHandler;
        private final long fileSize;

        /**
         * Construct a new encrypted WriteContext object
         */
        public EncryptedWriteContext(WriteContext writeContext, CryptoHandler<T, U> cryptoHandler) {
            super(writeContext);
            this.cryptoHandler = cryptoHandler;
            this.encryptionMetadata = this.cryptoHandler.initEncryptionMetadata();
            this.fileSize = this.cryptoHandler.estimateEncryptedLengthOfEntireContent(encryptionMetadata, writeContext.getFileSize());
        }

        public StreamContext getStreamProvider(long partSize) {
            long adjustedPartSize = cryptoHandler.adjustContentSizeForPartialEncryption(encryptionMetadata, partSize);
            StreamContext streamContext = super.getStreamProvider(adjustedPartSize);
            return new EncryptedStreamContext<>(streamContext, cryptoHandler, encryptionMetadata);
        }

        /**
         * @return The total size of the encrypted file
         */
        public long getFileSize() {
            return fileSize;
        }
    }

    static class EncryptedStreamContext<T, U> extends StreamContext {

        private final CryptoHandler<T, U> cryptoHandler;
        private final T encryptionMetadata;

        /**
         * Construct a new encrypted StreamContext object
         */
        public EncryptedStreamContext(StreamContext streamContext, CryptoHandler<T, U> cryptoHandler, T encryptionMetadata) {
            super(streamContext);
            this.cryptoHandler = cryptoHandler;
            this.encryptionMetadata = encryptionMetadata;
        }

        @Override
        public InputStreamContainer provideStream(int partNumber) throws IOException {
            InputStreamContainer inputStreamContainer = super.provideStream(partNumber);
            return cryptoHandler.createEncryptingStreamOfPart(encryptionMetadata, inputStreamContainer, getNumberOfParts(), partNumber);
        }

    }

    /**
     * DecryptedReadContext decrypts the encrypted {@link ReadContext} by acting as a transformation wrapper around
     * the encrypted object
     * @param <T> Encryption Metadata / CryptoContext for the {@link CryptoHandler} instance
     * @param <U> Parsed Encryption Metadata / CryptoContext for the {@link CryptoHandler} instance
     */
    static class DecryptedReadContext<T, U> extends ReadContext {

        private final CryptoHandler<T, U> cryptoHandler;
        private final U cryptoContext;
        private Long blobSize;

        public DecryptedReadContext(ReadContext readContext, CryptoHandler<T, U> cryptoHandler, U cryptoContext) {
            super(readContext);
            this.cryptoHandler = cryptoHandler;
            this.cryptoContext = cryptoContext;
        }

        @Override
        public long getBlobSize() {
            // initializes the value lazily
            if (blobSize == null) {
                this.blobSize = this.cryptoHandler.estimateDecryptedLength(cryptoContext, super.getBlobSize());
            }
            return this.blobSize;
        }

        @Override
        public List<StreamPartCreator> getPartStreams() {
            return super.getPartStreams().stream()
                .map(supplier -> (StreamPartCreator) () -> supplier.get().thenApply(this::decryptInputStreamContainer))
                .collect(Collectors.toUnmodifiableList());
        }

        /**
         * Transforms an encrypted {@link InputStreamContainer} to a decrypted instance
         * @param inputStreamContainer encrypted input stream container instance
         * @return decrypted input stream container instance
         */
        private InputStreamContainer decryptInputStreamContainer(InputStreamContainer inputStreamContainer) {
            long startOfStream = inputStreamContainer.getOffset();
            long endOfStream = startOfStream + inputStreamContainer.getContentLength() - 1;
            DecryptedRangedStreamProvider decryptedStreamProvider = cryptoHandler.createDecryptingStreamOfRange(
                cryptoContext,
                startOfStream,
                endOfStream
            );

            long adjustedPos = decryptedStreamProvider.getAdjustedRange()[0];
            long adjustedLength = decryptedStreamProvider.getAdjustedRange()[1] - adjustedPos + 1;
            final InputStream decryptedStream = decryptedStreamProvider.getDecryptedStreamProvider()
                .apply(inputStreamContainer.getInputStream());
            return new InputStreamContainer(decryptedStream, adjustedLength, adjustedPos);
        }
    }

    @Override
    public void deleteAsync(ActionListener<DeleteResult> completionListener) {
        blobContainer.deleteAsync(completionListener);
    }

    @Override
    public void deleteBlobsAsyncIgnoringIfNotExists(List<String> blobNames, ActionListener<Void> completionListener) {
        blobContainer.deleteBlobsAsyncIgnoringIfNotExists(blobNames, completionListener);
    }
}
