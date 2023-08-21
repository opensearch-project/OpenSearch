/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.crypto.CryptoProvider;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;

/**
 * EncryptedBlobContainer is an encrypted BlobContainer that is backed by a
 * {@link AsyncMultiStreamBlobContainer}
 *
 * @opensearch.internal
 */
public class AsyncMultiStreamEncryptedBlobContainer extends EncryptedBlobContainer implements AsyncMultiStreamBlobContainer {

    private final AsyncMultiStreamBlobContainer blobContainer;
    private final CryptoProvider cryptoProvider;

    public AsyncMultiStreamEncryptedBlobContainer(AsyncMultiStreamBlobContainer blobContainer, CryptoProvider cryptoProvider) {
        super(blobContainer, cryptoProvider);
        this.blobContainer = blobContainer;
        this.cryptoProvider = cryptoProvider;
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        EncryptedWriteContext encryptedWriteContext = new EncryptedWriteContext(writeContext, cryptoProvider);
        blobContainer.asyncBlobUpload(encryptedWriteContext, completionListener);
    }

    @Override
    public boolean remoteIntegrityCheckSupported() {
        return false;
    }

    static class EncryptedWriteContext extends WriteContext {

        private final Object encryptionMetadata;
        private final CryptoProvider cryptoProvider;
        private final long fileSize;

        /**
         * Construct a new encrypted WriteContext object
         */
        public EncryptedWriteContext(WriteContext writeContext, CryptoProvider cryptoProvider) {
            super(writeContext);
            this.cryptoProvider = cryptoProvider;
            this.encryptionMetadata = this.cryptoProvider.initEncryptionMetadata();
            this.fileSize = this.cryptoProvider.estimateEncryptedLengthOfEntireContent(encryptionMetadata, writeContext.getFileSize());
        }

        public StreamContext getStreamProvider(long partSize) {
            long adjustedPartSize = cryptoProvider.adjustContentSizeForPartialEncryption(encryptionMetadata, partSize);
            StreamContext streamContext = super.getStreamProvider(adjustedPartSize);
            return new EncryptedStreamContext(streamContext, cryptoProvider, encryptionMetadata);
        }

        /**
         * @return The total size of the encrypted file
         */
        public long getFileSize() {
            return fileSize;
        }
    }

    static class EncryptedStreamContext extends StreamContext {

        private final CryptoProvider cryptoProvider;
        private final Object encryptionMetadata;

        /**
         * Construct a new encrypted StreamContext object
         */
        public EncryptedStreamContext(StreamContext streamContext, CryptoProvider cryptoProvider, Object encryptionMetadata) {
            super(streamContext);
            this.cryptoProvider = cryptoProvider;
            this.encryptionMetadata = encryptionMetadata;
        }

        @Override
        public InputStreamContainer provideStream(int partNumber) throws IOException {
            InputStreamContainer inputStreamContainer = super.provideStream(partNumber);
            return cryptoProvider.createEncryptingStreamOfPart(encryptionMetadata, inputStreamContainer, getNumberOfParts(), partNumber);
        }

    }
}
