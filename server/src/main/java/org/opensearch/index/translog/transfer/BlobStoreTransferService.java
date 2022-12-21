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

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Service that handles remote transfer of translog and checkpoint files
 *
 * @opensearch.internal
 */
public class BlobStoreTransferService implements TransferService {

    private final BlobStore blobStore;
    private final ExecutorService executorService;

    private static final Logger logger = LogManager.getLogger(BlobStoreTransferService.class);

    public BlobStoreTransferService(BlobStore blobStore, ExecutorService executorService) {
        this.blobStore = blobStore;
        this.executorService = executorService;
    }

    @Override
    public void uploadBlobAsync(
        final TransferFileSnapshot fileSnapshot,
        Iterable<String> remoteTransferPath,
        ActionListener<TransferFileSnapshot> listener
    ) {
        assert remoteTransferPath instanceof BlobPath;
        BlobPath blobPath = (BlobPath) remoteTransferPath;
        executorService.execute(ActionRunnable.wrap(listener, l -> {
            try (InputStream inputStream = fileSnapshot.inputStream()) {
                blobStore.blobContainer(blobPath)
                    .writeBlobAtomic(fileSnapshot.getName(), inputStream, fileSnapshot.getContentLength(), true);
                l.onResponse(fileSnapshot);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), e);
                l.onFailure(new FileTransferException(fileSnapshot, e));
            }
        }));
    }

    @Override
    public void uploadBlob(final TransferFileSnapshot fileSnapshot, Iterable<String> remoteTransferPath) throws IOException {
        assert remoteTransferPath instanceof BlobPath;
        BlobPath blobPath = (BlobPath) remoteTransferPath;
        try (InputStream inputStream = fileSnapshot.inputStream()) {
            blobStore.blobContainer(blobPath).writeBlobAtomic(fileSnapshot.getName(), inputStream, fileSnapshot.getContentLength(), true);
        } catch (Exception ex) {
            throw ex;
        }
    }

    @Override
    public InputStream downloadBlob(Iterable<String> path, String fileName) throws IOException {
        return blobStore.blobContainer((BlobPath) path).readBlob(fileName);
    }

    @Override
    public Set<String> listAll(Iterable<String> path) throws IOException {
        return blobStore.blobContainer((BlobPath) path).listBlobs().keySet();
    }
}
