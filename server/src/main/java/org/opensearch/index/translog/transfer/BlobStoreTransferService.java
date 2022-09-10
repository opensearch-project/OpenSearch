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
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Service that handles remote transfer of translog and checkpoint files
 */
public class BlobStoreTransferService implements TransferService {

    private final BlobStore blobStore;
    private final ThreadPool threadPool;

    private static final Logger logger = LogManager.getLogger(BlobStoreTransferService.class);

    public BlobStoreTransferService(BlobStore blobStore, ThreadPool threadPool) {
        this.blobStore = blobStore;
        this.threadPool = threadPool;
    }

    @Override
    public void uploadFileAsync(
        final TransferFileSnapshot fileSnapshot,
        Iterable<String> remoteTransferPath,
        ActionListener<TransferFileSnapshot> listener
    ) {
        assert remoteTransferPath instanceof BlobPath;
        BlobPath blobPath = (BlobPath) remoteTransferPath;
        threadPool.executor(ThreadPool.Names.TRANSLOG_TRANSFER).execute(ActionRunnable.wrap(listener, l -> {
            try {
                blobStore.blobContainer(blobPath)
                    .writeBlobAtomic(
                        fileSnapshot.getName(),
                        new ByteArrayInputStream(fileSnapshot.getContent()),
                        fileSnapshot.getContentLength(),
                        true
                    );
                l.onResponse(fileSnapshot);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), e);
                l.onFailure(new FileTransferException(fileSnapshot, e));
            }
        }));
    }

    @Override
    public void uploadFile(final TransferFileSnapshot fileSnapshot, Iterable<String> remoteTransferPath) throws IOException {
        assert remoteTransferPath instanceof BlobPath;
        BlobPath blobPath = (BlobPath) remoteTransferPath;
        try {
            blobStore.blobContainer(blobPath)
                .writeBlobAtomic(
                    fileSnapshot.getName(),
                    new ByteArrayInputStream(fileSnapshot.getContent()),
                    fileSnapshot.getContentLength(),
                    true
                );
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), ex);
            throw ex;
        }
    }
}
