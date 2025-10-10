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
import org.opensearch.repositories.RepositoryException;

/**
 * BlobStoreProvider for RemoteStoreProvider
 */
public class RemoteStoreBlobStoreProvider extends BlobStoreProvider {
    private final SetOnce<BlobStore> serverSideEncryptedBlobStore = new SetOnce<>();

    public RemoteStoreBlobStoreProvider(BlobStoreRepository repository, RepositoryMetadata metadata, Lifecycle lifecycle, Object lock) {
        super(repository, metadata, lifecycle, lock);
    }

    public BlobStore getBlobStore(boolean serverSideEncryption) {
        if (serverSideEncryption) {
            return serverSideEncryptedBlobStore.get();
        }
        return blobStore.get();
    }

    /**
     *
     */
    public BlobStore blobStore(boolean serverSideEncryption) {
        System.out.println("serverSideEncryption = " + serverSideEncryption);
        BlobStore store = null;
        if (serverSideEncryption) {
            store = serverSideEncryptedBlobStore.get();
            if (store == null) {
                store = super.createBlobStore(serverSideEncryptedBlobStore, true);
            }
        } else {
            store = super.blobStore(false);
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
