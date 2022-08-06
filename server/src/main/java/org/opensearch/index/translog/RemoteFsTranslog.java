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

    /**
     * Creates a new Translog instance. This method will create a new transaction log unless the given {@link TranslogGeneration} is
     * {@code null}. If the generation is {@code null} this method is destructive and will delete all files in the translog path given. If
     * the generation is not {@code null}, this method tries to open the given translog generation. The generation is treated as the last
     * generation referenced from already committed data. This means all operations that have not yet been committed should be in the
     * translog file referenced by this generation. The translog creation will fail if this generation can't be opened.
     *
     * @param config                          the configuration of this translog
     * @param translogUUID                    the translog uuid to open, null for a new translog
     * @param deletionPolicy                  an instance of {@link TranslogDeletionPolicy} that controls when a translog file can be safely
     *                                        deleted
     * @param globalCheckpointSupplier        a supplier for the global checkpoint
     * @param primaryTermSupplier             a supplier for the latest value of primary term of the owning index shard. The latest term value is
     *                                        examined and stored in the header whenever a new generation is rolled. It's guaranteed from outside
     *                                        that a new generation is rolled when the term is increased. This guarantee allows to us to validate
     *                                        and reject operation whose term is higher than the primary term stored in the translog header.
     * @param persistedSequenceNumberConsumer a callback that's called whenever an operation with a given sequence number is successfully
     */

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
        return false;
    }

    @Override
    public void rollGeneration() throws IOException {
        super.rollGeneration();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            TransferSnapshotProvider transferSnapshotProvider = new TransferSnapshotProvider(this.location, readers);
            Releasable transferReleasable = Releasables.wrap(deletionPolicy.acquireTranslogGen(getMinFileGeneration()));
            translogTransferManager.uploadTranslog(transferSnapshotProvider.get(), new TranslogTransferListener() {
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
        } catch (final Exception e) {
            tragedy.setTragicException(e);
            closeOnTragicEvent(e);
            throw e;
        }
    }
}
