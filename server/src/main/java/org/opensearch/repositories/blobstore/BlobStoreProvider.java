/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.EncryptedBlobStore;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.repositories.RepositoryException;

/**
 * Provide for the BlobStore class
 *
 * @opensearch.internal
 */
public class BlobStoreProvider {
    private static final Logger logger = LogManager.getLogger(BlobStoreProvider.class);
    protected final Lifecycle lifecycle;
    protected final RepositoryMetadata metadata;
    protected final Object lock;
    protected final BlobStoreRepository repository;
    private final SetOnce<BlobStore> blobStore = new SetOnce<>();
    private final SetOnce<BlobStore> serverSideEncryptedBlobStore = new SetOnce<>();

    public BlobStoreProvider(BlobStoreRepository repository, RepositoryMetadata metadata, Lifecycle lifecycle, Object lock) {
        this.lifecycle = lifecycle;
        this.metadata = metadata;
        this.lock = lock;
        this.repository = repository;
    }

    protected BlobStore blobStore(boolean serverSideEncryptionEnabled) {
        if (serverSideEncryptionEnabled) {
            return createBlobStore(serverSideEncryptedBlobStore, true);
        }
        return createBlobStore(blobStore, false);
    }

    protected BlobStore createBlobStore(SetOnce<BlobStore> blobStore, boolean serverSideEncryption) {
        // assertSnapshotOrGenericThread();
        BlobStore store = blobStore.get();
        logger.debug("blob store fetched = " + store);
        if (store == null) {
            synchronized (lock) {
                store = blobStore.get();
                if (store == null) {
                    store = initBlobStore();
                    if (!serverSideEncryption && metadata.cryptoMetadata() != null) {
                        store = new EncryptedBlobStore(store, metadata.cryptoMetadata());
                    }
                    blobStore.set(store);
                }
            }
        }
        return store;
    }

    public BlobStore getBlobStore(boolean serverSideEncryptionEnabled) {
        if (serverSideEncryptionEnabled) {
            return serverSideEncryptedBlobStore.get();
        }
        return blobStore.get();
    }

    protected BlobStore initBlobStore() {
        if (lifecycle.started() == false) {
            throw new RepositoryException(metadata.name(), "repository is not in started state" + lifecycle.state());
        }
        try {
            return repository.createBlobStore();
        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new RepositoryException(metadata.name(), "cannot create blob store", e);
        }
    }

    public void close() {
        try {
            if (blobStore.get() != null) {
                blobStore.get().close();
            }
            if (serverSideEncryptedBlobStore.get() != null) {
                serverSideEncryptedBlobStore.get().close();
            }
        } catch (Exception t) {
            logger.warn("cannot close blob store", t);
        }
    }
}
