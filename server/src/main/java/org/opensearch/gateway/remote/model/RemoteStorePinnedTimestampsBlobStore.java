/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * Extends the RemoteClusterStateBlobStore to support {@link RemotePinnedTimestamps}
 */
public class RemoteStorePinnedTimestampsBlobStore extends RemoteClusterStateBlobStore<
    RemotePinnedTimestamps.PinnedTimestamps,
    RemotePinnedTimestamps> {

    private final BlobStoreRepository blobStoreRepository;

    public RemoteStorePinnedTimestampsBlobStore(
        BlobStoreTransferService blobStoreTransferService,
        BlobStoreRepository blobStoreRepository,
        String clusterName,
        ThreadPool threadPool,
        String executor
    ) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool, executor);
        this.blobStoreRepository = blobStoreRepository;
    }

    @Override
    public BlobPath getBlobPathForUpload(final AbstractRemoteWritableBlobEntity<RemotePinnedTimestamps.PinnedTimestamps> obj) {
        return blobStoreRepository.basePath();
    }
}
