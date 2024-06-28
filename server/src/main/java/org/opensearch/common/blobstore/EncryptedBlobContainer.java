/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.CheckedBiConsumer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * EncryptedBlobContainer is a wrapper around BlobContainer that encrypts the data on the fly.
 */
public class EncryptedBlobContainer<T, U> implements BlobContainer {

    private final BlobContainer blobContainer;
    private final CryptoHandler<T, U> cryptoHandler;

    public EncryptedBlobContainer(BlobContainer blobContainer, CryptoHandler<T, U> cryptoHandler) {
        this.blobContainer = blobContainer;
        this.cryptoHandler = cryptoHandler;
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
        return cryptoHandler.createDecryptingStream(inputStream);
    }

    @ExperimentalApi
    @Override
    public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
        InputStreamWithMetadata inputStreamWithMetadata = blobContainer.readBlobWithMetadata(blobName);
        InputStream decryptInputStream = cryptoHandler.createDecryptingStream(inputStreamWithMetadata.getInputStream());
        return new InputStreamWithMetadata(decryptInputStream, inputStreamWithMetadata.getMetadata());
    }

    EncryptedHeaderContentSupplier getEncryptedHeaderContentSupplier(String blobName) {
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
        U encryptionMetadata = cryptoHandler.loadEncryptionMetadata(getEncryptedHeaderContentSupplier(blobName));
        DecryptedRangedStreamProvider decryptedStreamProvider = cryptoHandler.createDecryptingStreamOfRange(
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

    private void executeWrite(InputStream inputStream, long blobSize, CheckedBiConsumer<InputStream, Long, IOException> writeConsumer)
        throws IOException {
        T cryptoContext = cryptoHandler.initEncryptionMetadata();
        InputStreamContainer streamContainer = new InputStreamContainer(inputStream, blobSize, 0);
        InputStreamContainer encryptedStream = cryptoHandler.createEncryptingStream(cryptoContext, streamContainer);
        long cryptoLength = cryptoHandler.estimateEncryptedLengthOfEntireContent(cryptoContext, blobSize);
        writeConsumer.accept(encryptedStream.getInputStream(), cryptoLength);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        executeWrite(
            inputStream,
            blobSize,
            (encryptedStream, encryptedLength) -> blobContainer.writeBlob(blobName, encryptedStream, encryptedLength, failIfAlreadyExists)
        );
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        executeWrite(
            inputStream,
            blobSize,
            (encryptedStream, encryptedLength) -> blobContainer.writeBlobAtomic(
                blobName,
                encryptedStream,
                encryptedLength,
                failIfAlreadyExists
            )
        );
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
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> new EncryptedBlobContainer<>(entry.getValue(), cryptoHandler)));
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
                    entry -> new EncryptedBlobMetadata<>(entry.getValue(), cryptoHandler, getEncryptedHeaderContentSupplier(entry.getKey()))
                )
            );

    }

    @Override
    public void listBlobsByPrefixInSortedOrder(
        String blobNamePrefix,
        int limit,
        BlobNameSortOrder blobNameSortOrder,
        ActionListener<List<BlobMetadata>> listener
    ) {
        ActionListener<List<BlobMetadata>> encryptedMetadataListener = ActionListener.delegateFailure(
            listener,
            (delegatedListener, metadataList) -> {
                if (metadataList != null) {
                    List<BlobMetadata> encryptedMetadata = metadataList.stream()
                        .map(
                            blobMetadata -> new EncryptedBlobMetadata<>(
                                blobMetadata,
                                cryptoHandler,
                                getEncryptedHeaderContentSupplier(blobMetadata.name())
                            )
                        )
                        .collect(Collectors.toList());
                    delegatedListener.onResponse(encryptedMetadata);
                } else {
                    delegatedListener.onResponse(null);
                }
            }
        );
        blobContainer.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, blobNameSortOrder, encryptedMetadataListener);
    }
}
