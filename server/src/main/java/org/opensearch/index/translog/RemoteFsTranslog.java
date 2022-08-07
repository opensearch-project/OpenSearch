/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TransferSnapshotProvider;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class RemoteFsTranslog extends Translog {

    private final BlobStore blobStore;
    private final TranslogTransferManager translogTransferManager;

    public RemoteFsTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStore blobStore,
        ThreadPool threadPool
    ) throws IOException {
        super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer);
        this.blobStore = blobStore;
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId);
        this.translogTransferManager = new TranslogTransferManager(
            new BlobStoreTransferService(blobStore, threadPool),
            new BlobPath().add(UUIDs.base64UUID())
                .add(shardId.getIndex().getUUID())
                .add(String.valueOf(shardId.id()))
                .add(String.valueOf(primaryTermSupplier.getAsLong())),
            fileTransferTracker,
            fileTransferTracker::exclusionFilter
        );
    }

    @Override
    boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            assert location.generation <= current.getGeneration();
            if (location.generation == current.getGeneration()) {
                ensureOpen();
                prepareUpload(location.generation);
            }
            return upload();
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    @Override
    public void rollGeneration() throws IOException {
        syncBeforeRollGeneration();
        if (current.totalOperations() == 0 && primaryTermSupplier.getAsLong() == current.getPrimaryTerm()) {
            return;
        }
        prepareUpload(null);
        upload();
    }

    private void prepareUpload(Long generation) throws IOException {
        try (Releasable ignored = writeLock.acquire()) {
            if (generation == null || generation == current.getGeneration()) {
                try {
                    final TranslogReader reader = current.closeIntoReader();
                    readers.add(reader);
                    if (closed.get() == false) {
                        // create a new translog file; this will sync it and update the checkpoint data;
                        logger.trace("Creating new writer for gen: [{}]", current.getGeneration() + 1);
                        current = createWriter(current.getGeneration() + 1);
                        logger.trace("current translog set to [{}]", current.getGeneration());
                    }
                } catch (final Exception e) {
                    tragedy.setTragicException(e);
                    closeOnTragicEvent(e);
                    throw e;
                }
            }
        }
    }

    private boolean upload() throws IOException {
        TransferSnapshotProvider transferSnapshotProvider = new TransferSnapshotProvider(this.location, readers);
        Releasable transferReleasable = Releasables.wrap(deletionPolicy.acquireTranslogGen(getMinFileGeneration()));
        return translogTransferManager.uploadTranslog(transferSnapshotProvider.get(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) throws IOException {
                transferReleasable.close();
                closeFilesIfNoPendingRetentionLocks();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) throws IOException {
                transferReleasable.close();
                closeFilesIfNoPendingRetentionLocks();
            }
        });
    }

    private boolean syncToDisk() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.sync();
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    @Override
    public void sync() throws IOException {

        try {
            if(syncToDisk()) {
                upload();
            }
        } catch (final Exception e) {
            tragedy.setTragicException(e);
            closeOnTragicEvent(e);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        assert Translog.calledFromOutsideOrViaTragedyClose()
            : "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                prepareUpload(null);
                upload();
            } finally {
                logger.debug("translog closed");
            }
        }
    }
}
