/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.RemoteWritableEntityStore;
import org.opensearch.common.remote.RemoteWriteableEntity;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

/**
 * Abstract class for a blob type storage
 *
 * @param <T> The entity which can be uploaded to / downloaded from blob store
 * @param <U> The concrete class implementing {@link RemoteWriteableEntity} which is used as a wrapper for T entity.
 */
public class RemoteClusterStateBlobStore<T, U extends AbstractRemoteWritableBlobEntity<T>> implements RemoteWritableEntityStore<T, U> {

    private final BlobStoreTransferService transferService;
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterName;
    private final ExecutorService executorService;

    public RemoteClusterStateBlobStore(
        final BlobStoreTransferService blobStoreTransferService,
        final BlobStoreRepository blobStoreRepository,
        final String clusterName,
        final ThreadPool threadPool,
        final String executor
    ) {
        this.transferService = blobStoreTransferService;
        this.blobStoreRepository = blobStoreRepository;
        this.clusterName = clusterName;
        this.executorService = threadPool.executor(executor);
    }

    @Override
    public void writeAsync(final U entity, final ActionListener<Void> listener) {
        try {
            try (InputStream inputStream = entity.serialize()) {
                BlobPath blobPath = getBlobPathForUpload(entity);
                entity.setFullBlobName(blobPath);
                transferService.uploadBlob(
                    inputStream,
                    getBlobPathForUpload(entity),
                    entity.getBlobFileName(),
                    WritePriority.URGENT,
                    listener
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public T read(final U entity) throws IOException {
        // TODO Add timing logs and tracing
        assert entity.getFullBlobName() != null;
        return entity.deserialize(transferService.downloadBlob(getBlobPathForDownload(entity), entity.getBlobFileName()));
    }

    @Override
    public void readAsync(final U entity, final ActionListener<T> listener) {
        executorService.execute(() -> {
            try {
                listener.onResponse(read(entity));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private BlobPath getBlobPathForUpload(final AbstractRemoteWritableBlobEntity<T> obj) {
        BlobPath blobPath = blobStoreRepository.basePath()
            .add(RemoteClusterStateUtils.encodeString(clusterName))
            .add("cluster-state")
            .add(obj.clusterUUID());
        for (String token : obj.getBlobPathParameters().getPathTokens()) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    private BlobPath getBlobPathForDownload(final AbstractRemoteWritableBlobEntity<T> obj) {
        String[] pathTokens = obj.getBlobPathTokens();
        BlobPath blobPath = new BlobPath();
        if (pathTokens == null || pathTokens.length < 1) {
            return blobPath;
        }
        // Iterate till second last path token to get the blob folder
        for (int i = 0; i < pathTokens.length - 1; i++) {
            blobPath = blobPath.add(pathTokens[i]);
        }
        return blobPath;
    }

}
