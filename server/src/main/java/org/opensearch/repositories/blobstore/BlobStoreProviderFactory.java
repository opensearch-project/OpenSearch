/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;

/**
 * Factory class for BlobStoreProvider.
 */
public class BlobStoreProviderFactory {
    private final Lifecycle lifecycle;
    private final RepositoryMetadata metadata;
    private final Object lock;
    private final BlobStoreRepository repository;
    private static ServerSideEncryptionEnabledBlobStoreProvider serverSideEncryptionEnabledBlobStoreProvider;
    private static BlobStoreProvider blobStoreProvider;

    public BlobStoreProviderFactory(BlobStoreRepository repository, RepositoryMetadata metadata, Lifecycle lifecycle, Object lock) {
        this.lifecycle = lifecycle;
        this.metadata = metadata;
        this.lock = lock;
        this.repository = repository;
    }

    public BlobStoreProvider getBlobStoreProvider() {
        if (RemoteStoreNodeAttribute.isRemoteStoreMetadata(metadata.settings())) {
            if (serverSideEncryptionEnabledBlobStoreProvider == null) {
                serverSideEncryptionEnabledBlobStoreProvider = new ServerSideEncryptionEnabledBlobStoreProvider(repository, metadata, lifecycle, lock);
            }
            return serverSideEncryptionEnabledBlobStoreProvider;
        } else {
            if (blobStoreProvider == null) {
                blobStoreProvider = new BlobStoreProvider(repository, metadata, lifecycle, lock);
            }
            return blobStoreProvider;
        }
    }
}
