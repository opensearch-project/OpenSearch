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
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
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
        DecryptingReadContextListener<T, U> decryptingReadContextListener = new DecryptingReadContextListener<>(
            listener,
            cryptoHandler,
            getEncryptedHeaderContentSupplier(blobName)
        );
        blobContainer.readBlobAsync(blobName, decryptingReadContextListener);
    }

    private EncryptedHeaderContentSupplier getEncryptedHeaderContentSupplier(String blobName) {
        return (start, end) -> {
            byte[] buffer;
            int length = (int) (end - start + 1);
            try (InputStream inputStream = blobContainer.readBlob(blobName, start, length)) {
                buffer = new byte[length];
                inputStream.readNBytes(buffer, (int) start, buffer.length);
            }
            return buffer;
        };
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

    static class DecryptingReadContextListener<T, U> implements ActionListener<ReadContext> {

        private final ActionListener<ReadContext> completionListener;
        private final CryptoHandler<T, U> cryptoHandler;
        private final EncryptedHeaderContentSupplier encryptedHeaderContentSupplier;

        public DecryptingReadContextListener(
            ActionListener<ReadContext> completionListener,
            CryptoHandler<T, U> cryptoHandler,
            EncryptedHeaderContentSupplier headerContentSupplier
        ) {
            this.completionListener = completionListener;
            this.cryptoHandler = cryptoHandler;
            this.encryptedHeaderContentSupplier = headerContentSupplier;
        }

        @Override
        public void onResponse(ReadContext readContext) {
            try {
                DecryptedReadContext<T, U> decryptedReadContext = new DecryptedReadContext<>(
                    readContext,
                    cryptoHandler,
                    encryptedHeaderContentSupplier
                );
                completionListener.onResponse(decryptedReadContext);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            completionListener.onFailure(e);
        }
    }

    static class DecryptedReadContext<T, U> extends ReadContext {

        private final U cryptoContext;
        private final CryptoHandler<T, U> cryptoHandler;
        private final long fileSize;

        public DecryptedReadContext(
            ReadContext readContext,
            CryptoHandler<T, U> cryptoHandler,
            EncryptedHeaderContentSupplier headerContentSupplier
        ) {
            super(readContext);
            try {
                this.cryptoHandler = cryptoHandler;
                this.cryptoContext = this.cryptoHandler.loadEncryptionMetadata(headerContentSupplier);
                this.fileSize = this.cryptoHandler.estimateDecryptedLength(cryptoContext, readContext.getBlobSize());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long getBlobSize() {
            return fileSize;
        }

        @Override
        public List<InputStreamContainer> getPartStreams() {
            return super.getPartStreams().stream().map(this::decrpytInputStreamContainer).collect(Collectors.toList());
        }

        private InputStreamContainer decrpytInputStreamContainer(InputStreamContainer inputStreamContainer) {
            long startOfStream = inputStreamContainer.getOffset();
            long endOfStream = startOfStream + inputStreamContainer.getContentLength() - 1;
            DecryptedRangedStreamProvider decryptedStreamProvider = cryptoHandler.createDecryptingStreamOfRange(
                cryptoContext,
                startOfStream,
                endOfStream
            );

            long adjustedPos = decryptedStreamProvider.getAdjustedRange()[0];
            long adjustedLength = decryptedStreamProvider.getAdjustedRange()[1] - adjustedPos + 1;
            return new InputStreamContainer(
                decryptedStreamProvider.getDecryptedStreamProvider().apply(inputStreamContainer.getInputStream()),
                adjustedPos,
                adjustedLength
            );
        }
    }
}
