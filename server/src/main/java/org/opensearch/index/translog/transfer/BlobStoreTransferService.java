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
import org.opensearch.threadpool.ThreadPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

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
    public void uploadFile(final File file, RemotePathProvider remotePathProvider, ActionListener<Void> listener) throws IOException {
        try (InputStream stream = new FileInputStream(file)) {
            BlobPath blobPath = blobPath(remotePathProvider);
            logger.trace(() -> new ParameterizedMessage("Writing [{}] to {} atomically", file.getName(), blobPath));
            try (CheckedInputStream streamInput = new CheckedInputStream(stream, new CRC32())) {
                FileInfo fileInfo = new FileInfo(file.getName(), file.toPath(), file.length(), streamInput.getChecksum().getValue());
                threadPool.executor(ThreadPool.Names.TRANSLOG_TRANSFER).execute(ActionRunnable.wrap(listener, l -> {
                    try {
                        blobStore.blobContainer(blobPath).writeBlobAtomic(file.getName(), streamInput, file.length(), true);
                        fileTransferListener.onSuccess(fileInfo);
                        l.onResponse(null);
                    } catch (Exception e) {
                        logger.warn(() -> new ParameterizedMessage("Failed to upload some blobs"), e);
                        fileTransferListener.onFailure(fileInfo, e);
                        l.onFailure(e);
                    }
                }));
            }
        }
    }

    private BlobPath blobPath(RemotePathProvider remotePathProvider) {
        return new BlobPath();
    }
}
