/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * Abstract class for a blob type storage
 *
 * @param <T> The entity which can be uploaded to / downloaded from blob store
 * @param <U> The concrete class implementing {@link RemoteObject} which is used as a wrapper for T entity.
 */
public class AbstractRemoteBlobStore<T, U extends AbstractRemoteBlobObject<T>> implements RemoteObjectStore<T, U> {

    private final BlobStoreTransferService transferService;
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterName;
    private final ExecutorService executorService;

    public AbstractRemoteBlobStore(BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName,
        ThreadPool threadPool) {
        this.transferService = blobStoreTransferService;
        this.blobStoreRepository = blobStoreRepository;
        this.clusterName = clusterName;
        this.executorService = threadPool.executor(ThreadPool.Names.GENERIC);
    }

    @Override
    public void writeAsync(U obj, ActionListener<Void> listener) {
        assert obj.get() != null;
        try {
            InputStream inputStream = obj.serialize();
            BlobPath blobPath = getBlobPathForUpload(obj);
            obj.setFullBlobName(blobPath);
            transferService.uploadBlob(inputStream, getBlobPathForUpload(obj), obj.getBlobFileName(), WritePriority.URGENT, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public T read(U obj) throws IOException {
        assert obj.getFullBlobName() != null;
        return obj.deserialize(
            transferService.downloadBlob(getBlobPathForDownload(obj), obj.getBlobFileName()));
    }

    @Override
    public void readAsync(U obj, ActionListener<T> listener) {
        executorService.execute(() -> {
            try {
                listener.onResponse(read(obj));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private BlobPath getBlobPathForUpload(AbstractRemoteBlobObject<T> obj) {
        BlobPath blobPath = blobStoreRepository.basePath().add(RemoteClusterStateUtils.encodeString(clusterName)).add("cluster-state").add(obj.clusterUUID());
        for (String token : obj.getBlobPathParameters().getPathTokens()) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    private BlobPath getBlobPathForDownload(AbstractRemoteBlobObject<T> obj) {
        String[] pathTokens = extractBlobPathTokens(obj.getFullBlobName());
        BlobPath blobPath = new BlobPath();
        for (String token : pathTokens) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    private static String[] extractBlobPathTokens(String blobName) {
        String[] blobNameTokens = blobName.split(PATH_DELIMITER);
        return Arrays.copyOfRange(blobNameTokens, 0, blobNameTokens.length - 1);
    }
}
