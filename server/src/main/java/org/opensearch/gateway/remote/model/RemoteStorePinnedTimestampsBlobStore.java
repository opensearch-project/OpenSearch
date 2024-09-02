/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.remote.RemoteWriteableBlobEntity;
import org.opensearch.common.remote.RemoteWriteableEntityBlobStore;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * Extends the RemoteClusterStateBlobStore to support {@link RemotePinnedTimestamps}
 */
@ExperimentalApi
public class RemoteStorePinnedTimestampsBlobStore extends RemoteWriteableEntityBlobStore<
    RemotePinnedTimestamps.PinnedTimestamps,
    RemotePinnedTimestamps> {

    public static final String PINNED_TIMESTAMPS_PATH_TOKEN = "pinned_timestamps";
    private final BlobStoreRepository blobStoreRepository;

    public RemoteStorePinnedTimestampsBlobStore(
        BlobStoreTransferService blobStoreTransferService,
        BlobStoreRepository blobStoreRepository,
        String clusterName,
        ThreadPool threadPool,
        String executor
    ) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool, executor, PINNED_TIMESTAMPS_PATH_TOKEN);
        this.blobStoreRepository = blobStoreRepository;
    }

    @Override
    public BlobPath getBlobPathForUpload(final RemoteWriteableBlobEntity<RemotePinnedTimestamps.PinnedTimestamps> obj) {
        return blobStoreRepository.basePath().add(PINNED_TIMESTAMPS_PATH_TOKEN);
    }
}
