/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.crypto.CryptoHandlerRegistry;
import org.opensearch.crypto.CryptoRegistryException;
import org.opensearch.repositories.blobstore.EncryptionContextUtils;

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
    private final CryptoHandler<?, ?> cryptoHandler;
    private final CryptoMetadata repositoryCryptoMetadata;  // Store for merging with index metadata

    /**
     * Constructs an EncryptedBlobStore that wraps the provided BlobStore with encryption capabilities based on the
     * given CryptoMetadata.
     *
     * @param blobStore     The underlying BlobStore to be wrapped and used for storing encrypted data.
     * @param cryptoMetadata The CryptoMetadata containing information about the key provider and settings for encryption.
     * @throws CryptoRegistryException If the CryptoManager is not found during encrypted BlobStore creation.
     */
    public EncryptedBlobStore(BlobStore blobStore, CryptoMetadata cryptoMetadata) {
        CryptoHandlerRegistry cryptoHandlerRegistry = CryptoHandlerRegistry.getInstance();
        assert cryptoHandlerRegistry != null : "CryptoManagerRegistry is not initialized";
        this.cryptoHandler = cryptoHandlerRegistry.fetchCryptoHandler(cryptoMetadata);
        if (cryptoHandler == null) {
            throw new CryptoRegistryException(
                cryptoMetadata.keyProviderName(),
                cryptoMetadata.keyProviderType(),
                "Crypto manager not found during encrypted blob store creation."
            );
        }
        this.blobStore = blobStore;
        this.repositoryCryptoMetadata = cryptoMetadata;
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
            return new AsyncMultiStreamEncryptedBlobContainer<>((AsyncMultiStreamBlobContainer) blobContainer, cryptoHandler);
        }
        return new EncryptedBlobContainer<>(blobContainer, cryptoHandler);
    }

    // overloadded method to get blob container with the new crypto metadata
    public BlobContainer blobContainer(BlobPath path, CryptoMetadata cryptoMetadata) {

        // Merge index metadata with repository metadata for context merging
        CryptoMetadata merged = (cryptoMetadata != null) ? mergeCryptoMetadata(cryptoMetadata) : this.repositoryCryptoMetadata;

        CryptoHandlerRegistry cryptoHandlerRegistry = CryptoHandlerRegistry.getInstance();
        assert cryptoHandlerRegistry != null : "CryptoManagerRegistry is not initialized";
        CryptoHandler IndexCryptoHandler = cryptoHandlerRegistry.fetchCryptoHandler(merged);

        BlobContainer blobContainer = blobStore.blobContainer(path);
        if (blobContainer instanceof AsyncMultiStreamBlobContainer) {
            return new AsyncMultiStreamEncryptedBlobContainer<>((AsyncMultiStreamBlobContainer) blobContainer, IndexCryptoHandler);
        }
        return new EncryptedBlobContainer<>(blobContainer, IndexCryptoHandler);
    }

    /**
     * Reoload blobstore metadata
     * @param repositoryMetadata new repository metadata
     */
    @Override
    public void reload(RepositoryMetadata repositoryMetadata) {
        blobStore.reload(repositoryMetadata);
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
     * Retrieves extended statistics about the BlobStore. Delegates the call to the underlying BlobStore's extendedStats() method.
     *
     * @return A map containing extended statistics about the BlobStore.
     */
    @Override
    public Map<Metric, Map<String, Long>> extendedStats() {
        return blobStore.extendedStats();
    }

    @Override
    public boolean isBlobMetadataEnabled() {
        return blobStore.isBlobMetadataEnabled();
    }

    /**
     * Merges index-level and repository-level CryptoMetadata.
     * Priority: Index key provider if present, else repository
     * Context: Index context merged with repository context (EncA + EncB)
     *
     * @param indexMetadata The index-level CryptoMetadata
     * @return Merged CryptoMetadata with combined key and context
     */
    private CryptoMetadata mergeCryptoMetadata(CryptoMetadata indexMetadata) {
        // Delegate to centralized utility class
        return EncryptionContextUtils.mergeCryptoMetadata(indexMetadata, this.repositoryCryptoMetadata);
    }

    /**
     * Closes the EncryptedBlobStore by decrementing the reference count of the CryptoManager and closing the
     * underlying BlobStore. This ensures proper cleanup of resources.
     *
     * @throws IOException If an I/O error occurs while closing the BlobStore.
     */
    @Override
    public void close() throws IOException {
        cryptoHandler.close();
        blobStore.close();
    }

}
