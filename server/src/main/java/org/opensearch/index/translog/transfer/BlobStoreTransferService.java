/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRunnable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.translog.FileInfo;
import org.opensearch.index.translog.transfer.listener.FileTransferListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Service that handles remote transfer of translog related operations
 */
public class BlobStoreTransferService implements TransferService {

    private final BlobStore blobStore;
    private final ThreadPool threadPool;
    private final FileTransferListener fileTransferListener;

    private static final Logger logger = LogManager.getLogger(BlobStoreTransferService.class);

    public BlobStoreTransferService(BlobStore blobStore, ThreadPool threadPool, FileTransferListener fileTransferListener) {
        this.blobStore = blobStore;
        this.threadPool = threadPool;
        this.fileTransferListener = fileTransferListener;
    }

    @Override
    public void uploadFile(final FileInfo fileInfo, RemotePathProvider remotePathProvider, ActionListener<Void> listener)
        throws IOException {
        BlobPath blobPath = blobPath(remotePathProvider);
        threadPool.executor(ThreadPool.Names.TRANSLOG_TRANSFER).execute(ActionRunnable.wrap(listener, l -> {
            try {
                blobStore.blobContainer(blobPath)
                    .writeBlobAtomic(
                        fileInfo.getName(),
                        new ByteArrayInputStream(fileInfo.getContent()),
                        fileInfo.getContentLength(),
                        true
                    );
                fileTransferListener.onSuccess(fileInfo);
                l.onResponse(null);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("Failed to upload some blobs"), e);
                fileTransferListener.onFailure(fileInfo, e);
                l.onFailure(e);
            }
        }));

    }

    private BlobPath blobPath(RemotePathProvider remotePathProvider) {
        return new BlobPath();
    }
}
