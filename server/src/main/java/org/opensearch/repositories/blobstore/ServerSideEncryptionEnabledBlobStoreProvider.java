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
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.index.IndexSettings;
import org.opensearch.repositories.RepositoryException;

/**
 * BlobStoreProvider for RemoteStoreProvider
 */
public class ServerSideEncryptionEnabledBlobStoreProvider extends BlobStoreProvider {
    private final SetOnce<BlobStore> serverSideEncryptedBlobStore = new SetOnce<>();

    public ServerSideEncryptionEnabledBlobStoreProvider(BlobStoreRepository repository, RepositoryMetadata metadata, Lifecycle lifecycle, Object lock) {
        super(repository, metadata, lifecycle, lock);
    }

    public BlobStore getBlobStore(IndexSettings indexSettings) {
        boolean serverSideEncryptionEnabled = indexSettings != null && indexSettings.isServerSideEncryptionEnabled();

        if (serverSideEncryptionEnabled) {
            return serverSideEncryptedBlobStore.get();
        }
        return blobStore.get();
    }

    /**
     *
     */
    public BlobStore blobStore(IndexSettings indexSettings) {
        boolean serverSideEncryptionEnabled = indexSettings != null && indexSettings.isServerSideEncryptionEnabled();
        System.out.println("serverSideEncryption = " + serverSideEncryptionEnabled);
        BlobStore store = null;
        if (serverSideEncryptionEnabled) {
            store = serverSideEncryptedBlobStore.get();
            if (store == null) {
                store = super.createBlobStore(serverSideEncryptedBlobStore, true);
            }
        } else {
            store = super.blobStore(indexSettings);
        }
        return store;
    }

    /**
     *
     */
    protected BlobStore initBlobStore(boolean serverSideEncryption) {
        if (lifecycle.started() == false) {
            throw new RepositoryException(metadata.name(), "repository is not in started state" + lifecycle.state());
        }
        try {
            if (serverSideEncryption) {
                return repository.createServerSideEncryptedBlobStore();
            } else {
                return repository.createClientSideEncryptedBlobStore();
            }
        }  catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new RepositoryException(metadata.name(), "cannot create blob store", e);
        }
    }
}
