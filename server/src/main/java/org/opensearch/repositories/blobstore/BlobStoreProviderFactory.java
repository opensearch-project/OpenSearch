/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.SetOnce;
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
    private final SetOnce<ServerSideEncryptionEnabledBlobStoreProvider> serverSideEncryptionEnabledBlobStoreProvider = new SetOnce<>();
    private final SetOnce<BlobStoreProvider> blobStoreProvider = new SetOnce<>();

    public BlobStoreProviderFactory(BlobStoreRepository repository, RepositoryMetadata metadata, Lifecycle lifecycle, Object lock) {
        this.lifecycle = lifecycle;
        this.metadata = metadata;
        this.lock = lock;
        this.repository = repository;
    }

    public BlobStoreProvider getBlobStoreProvider() {
        synchronized (lock) {
            if (RemoteStoreNodeAttribute.isServerSideEncryptionEnabled(metadata.settings())) {
                if (serverSideEncryptionEnabledBlobStoreProvider.get() == null) {
                    ServerSideEncryptionEnabledBlobStoreProvider serverSideEncryptionEnabledBlobStoreProvider =
                        new ServerSideEncryptionEnabledBlobStoreProvider(repository, metadata, lifecycle, lock);
                    this.serverSideEncryptionEnabledBlobStoreProvider.set(serverSideEncryptionEnabledBlobStoreProvider);
                }
                return serverSideEncryptionEnabledBlobStoreProvider.get();
            } else {
                if (blobStoreProvider.get() == null) {
                    BlobStoreProvider blobStoreProvider = new BlobStoreProvider(repository, metadata, lifecycle, lock);
                    this.blobStoreProvider.set(blobStoreProvider);
                }
                return blobStoreProvider.get();
            }
        }
    }
}
