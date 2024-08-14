/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutorService;

/**
 * Abstract class for a blob type storage
 *
 * @param <T> The entity which can be uploaded to / downloaded from blob store
 * @param <U> The concrete class implementing {@link RemoteWriteableEntity} which is used as a wrapper for T entity.
 */
public class RemoteWriteableEntityBlobStore<T, U extends RemoteWriteableBlobEntity<T>> implements RemoteWritableEntityStore<T, U> {

    private final BlobStoreTransferService transferService;
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterName;
    private final ExecutorService executorService;
    private final String pathToken;

    public RemoteWriteableEntityBlobStore(
        final BlobStoreTransferService blobStoreTransferService,
        final BlobStoreRepository blobStoreRepository,
        final String clusterName,
        final ThreadPool threadPool,
        final String executor,
        final String pathToken
    ) {
        this.transferService = blobStoreTransferService;
        this.blobStoreRepository = blobStoreRepository;
        this.clusterName = clusterName;
        this.executorService = threadPool.executor(executor);
        this.pathToken = pathToken;
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
        try (InputStream inputStream = transferService.downloadBlob(getBlobPathForDownload(entity), entity.getBlobFileName())) {
            return entity.deserialize(inputStream);
        }
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

    public String getClusterName() {
        return clusterName;
    }

    public BlobPath getBlobPathPrefix(String clusterUUID) {
        return blobStoreRepository.basePath().add(encodeString(getClusterName())).add(pathToken).add(clusterUUID);
    }

    public BlobPath getBlobPathForUpload(final RemoteWriteableBlobEntity<T> obj) {
        BlobPath blobPath = getBlobPathPrefix(obj.clusterUUID());
        for (String token : obj.getBlobPathParameters().getPathTokens()) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    public BlobPath getBlobPathForDownload(final RemoteWriteableBlobEntity<T> obj) {
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

    private static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

}
