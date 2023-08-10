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
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.crypto.CryptoManager;

import java.io.IOException;

/**
 * EncryptedBlobContainer is an encrypted BlobContainer that is backed by a
 * {@link AsyncMultiStreamBlobContainer}
 *
 * @opensearch.internal
 */
public class AsyncMultiStreamEncryptedBlobContainer extends EncryptedBlobContainer implements AsyncMultiStreamBlobContainer {

    private final AsyncMultiStreamBlobContainer blobContainer;
    private final CryptoManager cryptoManager;

    public AsyncMultiStreamEncryptedBlobContainer(AsyncMultiStreamBlobContainer blobContainer, CryptoManager cryptoManager) {
        super(blobContainer, cryptoManager);
        this.blobContainer = blobContainer;
        this.cryptoManager = cryptoManager;
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        EncryptedWriteContext encryptedWriteContext = new EncryptedWriteContext(writeContext, cryptoManager);
        blobContainer.asyncBlobUpload(encryptedWriteContext, completionListener);
    }

    @Override
    public boolean remoteIntegrityCheckSupported() {
        return false;
    }

    static class EncryptedWriteContext extends WriteContext {

        private final Object encryptionMetadata;
        private final CryptoManager cryptoManager;
        private final long fileSize;

        /**
         * Construct a new encrypted WriteContext object
         */
        public EncryptedWriteContext(WriteContext writeContext, CryptoManager cryptoManager) {
            super(writeContext);
            this.cryptoManager = cryptoManager;
            this.encryptionMetadata = cryptoManager.initEncryptionMetadata();
            this.fileSize = cryptoManager.estimateEncryptedLengthOfEntireContent(encryptionMetadata, writeContext.getFileSize());
        }

        public StreamContext getStreamProvider(long partSize) {
            long adjustedPartSize = cryptoManager.adjustContentSizeForPartialEncryption(encryptionMetadata, partSize);
            StreamContext streamContext = super.getStreamProvider(adjustedPartSize);
            return new EncryptedStreamContext(streamContext, cryptoManager, encryptionMetadata);
        }

        /**
         * @return The total size of the encrypted file
         */
        public long getFileSize() {
            return fileSize;
        }
    }

    static class EncryptedStreamContext extends StreamContext {

        private final CryptoManager cryptoManager;
        private final Object encryptionMetadata;
        /**
         * Construct a new encrypted StreamContext object
         */
        public EncryptedStreamContext(StreamContext streamContext, CryptoManager cryptoManager, Object encryptionMetadata) {
            super(streamContext);
            this.cryptoManager = cryptoManager;
            this.encryptionMetadata = encryptionMetadata;
        }

        @Override
        public InputStreamContainer provideStream(int partNumber) throws IOException{
            InputStreamContainer inputStreamContainer = super.provideStream(partNumber);
            return cryptoManager.createEncryptingStreamOfPart(
                    encryptionMetadata,
                    inputStreamContainer,
                    getNumberOfParts(),
                    partNumber
            );
        }

    }
}
