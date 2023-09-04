/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.crypto.CryptoManagerRegistry;
import org.opensearch.crypto.CryptoRegistryException;
import org.opensearch.encryption.CryptoManager;

import java.io.IOException;
import java.util.Map;

/**
 * The EncryptedBlobStore is a decorator class that wraps an existing BlobStore and provides encryption and decryption
 * capabilities for the stored data. It uses a CryptoManager to handle encryption and decryption operations based on
 * the provided CryptoMetadata. The EncryptedBlobStore ensures that all data written to and read from the underlying
 * BlobStore is encrypted and decrypted transparently.
 */
public class EncryptedBlobStore implements BlobStore {

    private final BlobStore blobStore;
    private final CryptoManager<?, ?> cryptoManager;

    /**
     * Constructs an EncryptedBlobStore that wraps the provided BlobStore with encryption capabilities based on the
     * given CryptoMetadata.
     *
     * @param blobStore     The underlying BlobStore to be wrapped and used for storing encrypted data.
     * @param cryptoMetadata The CryptoMetadata containing information about the key provider and settings for encryption.
     * @throws CryptoRegistryException If the CryptoManager is not found during encrypted BlobStore creation.
     */
    public EncryptedBlobStore(BlobStore blobStore, CryptoMetadata cryptoMetadata) {
        CryptoManagerRegistry cryptoManagerRegistry = CryptoManagerRegistry.getInstance();
        assert cryptoManagerRegistry != null : "CryptoManagerRegistry is not initialized";
        this.cryptoManager = cryptoManagerRegistry.fetchCryptoManager(cryptoMetadata);
        if (cryptoManager == null) {
            throw new CryptoRegistryException(
                cryptoMetadata.keyProviderName(),
                cryptoMetadata.keyProviderType(),
                "Crypto manager not found during encrypted blob store creation."
            );
        }
        this.cryptoManager.incRef();
        this.blobStore = blobStore;
    }

    /**
     * Retrieves a BlobContainer from the underlying BlobStore based on the provided BlobPath. The returned BlobContainer
     * is wrapped in an EncryptedBlobContainer to enable transparent encryption and decryption of data.
     *
     * @param path The BlobPath specifying the location of the BlobContainer.
     * @return An EncryptedBlobContainer wrapping the BlobContainer retrieved from the underlying BlobStore.
     */
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        BlobContainer blobContainer = blobStore.blobContainer(path);
        if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
            return new AsyncMultiStreamEncryptedBlobContainer<>(
                (AsyncMultiStreamBlobContainer) blobContainer,
                cryptoManager.getCryptoProvider()
            );
        }
        return new EncryptedBlobContainer<>(blobContainer, cryptoManager.getCryptoProvider());
    }

    /**
     * Retrieves statistics about the BlobStore. Delegates the call to the underlying BlobStore's stats() method.
     *
     * @return A map containing statistics about the BlobStore.
     */
    @Override
    public Map<String, Long> stats() {
        return blobStore.stats();
    }

    /**
     * Closes the EncryptedBlobStore by decrementing the reference count of the CryptoManager and closing the
     * underlying BlobStore. This ensures proper cleanup of resources.
     *
     * @throws IOException If an I/O error occurs while closing the BlobStore.
     */
    @Override
    public void close() throws IOException {
        cryptoManager.decRef();
        blobStore.close();
    }

}
