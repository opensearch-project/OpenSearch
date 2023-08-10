/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.crypto.CryptoManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * EncryptedBlobContainer is a wrapper around BlobContainer that encrypts the data on the fly.
 */
public class EncryptedBlobContainer implements BlobContainer {

    private final BlobContainer blobContainer;
    private final CryptoManager cryptoManager;

    public EncryptedBlobContainer(BlobContainer blobContainer, CryptoManager cryptoManager) {
        this.blobContainer = blobContainer;
        this.cryptoManager = cryptoManager;
    }

    @Override
    public BlobPath path() {
        return blobContainer.path();
    }

    @Override
    public boolean blobExists(String blobName) throws IOException {
        return blobContainer.blobExists(blobName);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        InputStream inputStream = blobContainer.readBlob(blobName);
        return cryptoManager.createDecryptingStream(inputStream);
    }

    private EncryptedHeaderContentSupplier  getEncryptedHeaderContentSupplier(String blobName) {
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
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        Object encryptionMetadata = cryptoManager.loadEncryptionMetadata(getEncryptedHeaderContentSupplier(blobName));
        DecryptedRangedStreamProvider decryptedStreamProvider = cryptoManager.createDecryptingStreamOfRange(
                encryptionMetadata,
                position,
                position + length - 1
        );
        long adjustedPos = decryptedStreamProvider.getAdjustedRange()[0];
        long adjustedLength = decryptedStreamProvider.getAdjustedRange()[1] - adjustedPos + 1;
        InputStream encryptedStream = blobContainer.readBlob(blobName, adjustedPos, adjustedLength);
        return decryptedStreamProvider.getDecryptedStreamProvider().apply(encryptedStream);
    }

    @Override
    public long readBlobPreferredLength() {
        return blobContainer.readBlobPreferredLength();
    }

    private void executeWrite(InputStream inputStream, long blobSize,
                              BiConsumer<InputStream, Long> writeConsumer) {
        Object cryptoContext = cryptoManager.initEncryptionMetadata();
        InputStreamContainer streamContainer = new InputStreamContainer(inputStream, blobSize, 0);
        InputStreamContainer encryptedStream = cryptoManager.createEncryptingStream(cryptoContext, streamContainer);
        long cryptoLength = cryptoManager.estimateEncryptedLengthOfEntireContent(cryptoContext, blobSize);
        writeConsumer.accept(encryptedStream.getInputStream(), cryptoLength);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        IOException[] ioException = new IOException[1];
        executeWrite(inputStream, blobSize, (encryptedStream, encryptedLength) -> {
            try {
                blobContainer.writeBlob(blobName, encryptedStream, encryptedLength, failIfAlreadyExists);
            } catch (IOException ex) {
                ioException[0] = ex;
            }
        });
        if (ioException[0] != null) {
            throw ioException[0];
        }
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        IOException[] ioException = new IOException[1];
        executeWrite(inputStream, blobSize, (encryptedStream, encryptedLength) -> {
            try {
                blobContainer.writeBlobAtomic(blobName, encryptedStream, encryptedLength, failIfAlreadyExists);
            } catch (IOException ex) {
                ioException[0] = ex;
            }
        });
        if (ioException[0] != null) {
            throw ioException[0];
        }
    }

    @Override
    public DeleteResult delete() throws IOException {
        return blobContainer.delete();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        blobContainer.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        Map<String, BlobMetadata> blobMetadataMap = blobContainer.listBlobs();
        return convertToEncryptedMetadataMap(blobMetadataMap);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        Map<String, BlobContainer> children = blobContainer.children();
        if (children != null) {
            return children.entrySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> new EncryptedBlobContainer(entry.getValue(), cryptoManager)
                            )
                    );
        } else {
            return null;
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        Map<String, BlobMetadata> blobMetadataMap = blobContainer.listBlobsByPrefix(blobNamePrefix);
        return convertToEncryptedMetadataMap(blobMetadataMap);
    }

    private Map<String, BlobMetadata> convertToEncryptedMetadataMap(Map<String, BlobMetadata> blobMetadataMap) {
        if (blobMetadataMap == null) {
            return null;
        }

        return blobMetadataMap.entrySet()
                .stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new EncryptedBlobMetadata(
                                        entry.getValue(),
                                        cryptoManager,
                                        getEncryptedHeaderContentSupplier(entry.getKey())
                                ))
                );

    }

    @Override
    public void listBlobsByPrefixInSortedOrder(String blobNamePrefix, int limit, BlobNameSortOrder blobNameSortOrder, ActionListener<List<BlobMetadata>> listener) {
        ActionListener<List<BlobMetadata>> encryptedMetadataListener = ActionListener.delegateFailure(listener,
                (delegatedListener, metadataList) -> {
                    if (metadataList != null) {
                        List<BlobMetadata> encryptedMetadata = metadataList
                                .stream()
                                .map(blobMetadata -> new EncryptedBlobMetadata(
                                        blobMetadata,
                                        cryptoManager,
                                        getEncryptedHeaderContentSupplier(blobMetadata.name()))
                                ).collect(Collectors.toList());
                        delegatedListener.onResponse(encryptedMetadata);
                    } else {
                        delegatedListener.onResponse(null);
                    }
                });
        blobContainer.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, blobNameSortOrder, encryptedMetadataListener);
    }
}
