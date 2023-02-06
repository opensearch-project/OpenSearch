/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.common.SetOnce;
import org.opensearch.common.io.FileSystemUtils;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogCheckpointTransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * A Translog implementation which syncs local FS with a remote store
 * The current impl uploads translog , ckp and metadata to remote store
 * for every sync, post syncing to disk. Post that, a new generation is
 * created.
 *
 * @opensearch.internal
 */
public class RemoteFsTranslog extends Translog {

    private final BlobStoreRepository blobStoreRepository;
    private final TranslogTransferManager translogTransferManager;
    private final FileTransferTracker fileTransferTracker;
    private final BooleanSupplier primaryModeSupplier;
    private volatile long maxRemoteTranslogGenerationUploaded;

    private volatile long minSeqNoToKeep;

    // min generation referred by last uploaded translog
    private volatile long minRemoteGenReferenced;

    // clean up translog folder uploaded by previous primaries once
    private final SetOnce<Boolean> olderPrimaryCleaned = new SetOnce<>();

    public RemoteFsTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStoreRepository blobStoreRepository,
        ExecutorService executorService,
        BooleanSupplier primaryModeSupplier
    ) throws IOException {
        super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer);
        this.blobStoreRepository = blobStoreRepository;
        this.primaryModeSupplier = primaryModeSupplier;
        fileTransferTracker = new FileTransferTracker(shardId);
        this.translogTransferManager = buildTranslogTransferManager(blobStoreRepository, executorService, shardId, fileTransferTracker);

        try {
            download(translogTransferManager, location);
            Checkpoint checkpoint = readCheckpoint(location);
            ((ReplicationTracker) globalCheckpointSupplier).updateGlobalCheckpoint(checkpoint.globalCheckpoint);
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                throw new IllegalStateException("at least one reader must be recovered");
            }
            boolean success = false;
            current = null;
            try {
                current = createWriter(
                    checkpoint.generation + 1,
                    getMinFileGeneration(),
                    checkpoint.globalCheckpoint,
                    persistedSequenceNumberConsumer
                );
                success = true;
            } finally {
                // we have to close all the recovered ones otherwise we leak file handles here
                // for instance if we have a lot of tlog and we can't create the writer we keep
                // on holding
                // on to all the uncommitted tlog files if we don't close
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readers);
                }
            }
        } catch (Exception e) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    public static void download(TranslogTransferManager translogTransferManager, Path location) throws IOException {

        TranslogTransferMetadata translogMetadata = translogTransferManager.readMetadata();
        if (translogMetadata != null) {
            if (Files.notExists(location)) {
                Files.createDirectories(location);
            }
            // Delete translog files on local before downloading from remote
            for (Path file : FileSystemUtils.files(location)) {
                Files.delete(file);
            }
            Map<String, String> generationToPrimaryTermMapper = translogMetadata.getGenerationToPrimaryTermMapper();
            for (long i = translogMetadata.getGeneration(); i >= translogMetadata.getMinTranslogGeneration(); i--) {
                String generation = Long.toString(i);
                translogTransferManager.downloadTranslog(generationToPrimaryTermMapper.get(generation), generation, location);
            }
            // We copy the latest generation .ckp file to translog.ckp so that flows that depend on
            // existence of translog.ckp file work in the same way
            Files.copy(
                location.resolve(Translog.getCommitCheckpointFileName(translogMetadata.getGeneration())),
                location.resolve(Translog.CHECKPOINT_FILE_NAME)
            );
        }
    }

    public static TranslogTransferManager buildTranslogTransferManager(
        BlobStoreRepository blobStoreRepository,
        ExecutorService executorService,
        ShardId shardId,
        FileTransferTracker fileTransferTracker
    ) {
        return new TranslogTransferManager(
            new BlobStoreTransferService(blobStoreRepository.blobStore(), executorService),
            blobStoreRepository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())),
            fileTransferTracker
        );
    }

    @Override
    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            assert location.generation <= current.getGeneration();
            if (location.generation == current.getGeneration()) {
                ensureOpen();
                return prepareAndUpload(primaryTermSupplier.getAsLong(), location.generation);
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return false;
    }

    @Override
    public void rollGeneration() throws IOException {
        syncBeforeRollGeneration();
        if (current.totalOperations() == 0 && primaryTermSupplier.getAsLong() == current.getPrimaryTerm()) {
            return;
        }
        prepareAndUpload(primaryTermSupplier.getAsLong(), null);
    }

    private boolean prepareAndUpload(Long primaryTerm, Long generation) throws IOException {
        try (Releasable ignored = writeLock.acquire()) {
            if (generation == null || generation == current.getGeneration()) {
                try {
                    final TranslogReader reader = current.closeIntoReader();
                    readers.add(reader);
                    copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));
                    if (closed.get() == false) {
                        logger.trace("Creating new writer for gen: [{}]", current.getGeneration() + 1);
                        current = createWriter(current.getGeneration() + 1);
                    }
                } catch (final Exception e) {
                    tragedy.setTragicException(e);
                    closeOnTragicEvent(e);
                    throw e;
                }
            } else if (generation < current.getGeneration()) {
                return false;
            }

            // Do we need remote writes in sync fashion ?
            // If we don't , we should swallow FileAlreadyExistsException while writing to remote store
            // and also verify for same during primary-primary relocation
            // Writing remote in sync fashion doesn't hurt as global ckp update
            // is not updated in remote translog except in primary to primary recovery.
            if (generation == null) {
                if (closed.get() == false) {
                    return upload(primaryTerm, current.getGeneration() - 1);
                } else {
                    return upload(primaryTerm, current.getGeneration());
                }
            } else {
                return upload(primaryTerm, generation);
            }
        }
    }

    private boolean upload(Long primaryTerm, Long generation) throws IOException {
        // During primary relocation (primary-primary peer recovery), both the old and the new primary have engine
        // created with the RemoteFsTranslog. Both primaries are equipped to upload the translogs. The primary mode check
        // below ensures that the real primary only is uploading. Before the primary mode is set as true for the new
        // primary, the engine is reset to InternalEngine which also initialises the RemoteFsTranslog which in turns
        // downloads all the translogs from remote store and does a flush before the relocation finishes.
        if (primaryModeSupplier.getAsBoolean() == false) {
            logger.trace("skipped uploading translog for {} {}", primaryTerm, generation);
            // NO-OP
            return true;
        }
        logger.trace("uploading translog for {} {}", primaryTerm, generation);
        try (
            TranslogCheckpointTransferSnapshot transferSnapshotProvider = new TranslogCheckpointTransferSnapshot.Builder(
                primaryTerm,
                generation,
                location,
                readers,
                Translog::getCommitCheckpointFileName
            ).build()
        ) {
            Releasable transferReleasable = Releasables.wrap(deletionPolicy.acquireTranslogGen(getMinFileGeneration()));
            return translogTransferManager.transferSnapshot(transferSnapshotProvider, new TranslogTransferListener() {
                @Override

                public void onUploadComplete(TransferSnapshot transferSnapshot) throws IOException {
                    transferReleasable.close();
                    closeFilesIfNoPendingRetentionLocks();
                    maxRemoteTranslogGenerationUploaded = generation;
                    minRemoteGenReferenced = getMinFileGeneration();
                    logger.trace("uploaded translog for {} {} ", primaryTerm, generation);
                }

                @Override
                public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) throws IOException {
                    transferReleasable.close();
                    closeFilesIfNoPendingRetentionLocks();
                    if (ex instanceof IOException) {
                        throw (IOException) ex;
                    } else {
                        throw (RuntimeException) ex;
                    }
                }
            });
        }

    }

    // Visible for testing
    public Set<String> allUploaded() {
        return fileTransferTracker.allUploaded();
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
            if (syncToDisk() || syncNeeded()) {
                prepareAndUpload(primaryTermSupplier.getAsLong(), null);
            }
        } catch (final Exception e) {
            tragedy.setTragicException(e);
            closeOnTragicEvent(e);
            throw e;
        }
    }

    /**
     * Returns <code>true</code> if an fsync and/or remote transfer is required to ensure durability of the translogs operations or it's metadata.
     */
    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.syncNeeded()
                || (maxRemoteTranslogGenerationUploaded + 1 < this.currentFileGeneration() && current.totalOperations() == 0);
        }
    }

    @Override
    public void close() throws IOException {
        assert Translog.calledFromOutsideOrViaTragedyClose()
            : "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                sync();
            } finally {
                logger.debug("translog closed");
                closeFilesIfNoPendingRetentionLocks();
            }
        }
    }

    protected long getMinReferencedGen() throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        long minReferencedGen = Math.min(
            deletionPolicy.minTranslogGenRequired(readers, current),
            minGenerationForSeqNo(Math.min(deletionPolicy.getLocalCheckpointOfSafeCommit() + 1, minSeqNoToKeep), current, readers)
        );
        assert minReferencedGen >= getMinFileGeneration() : "deletion policy requires a minReferenceGen of ["
            + minReferencedGen
            + "] but the lowest gen available is ["
            + getMinFileGeneration()
            + "]";
        assert minReferencedGen <= currentFileGeneration() : "deletion policy requires a minReferenceGen of ["
            + minReferencedGen
            + "] which is higher than the current generation ["
            + currentFileGeneration()
            + "]";
        return minReferencedGen;
    }

    protected void setMinSeqNoToKeep(long seqNo) {
        if (seqNo < this.minSeqNoToKeep) {
            throw new IllegalArgumentException(
                "min seq number required can't go backwards: " + "current [" + this.minSeqNoToKeep + "] new [" + seqNo + "]"
            );
        }
        this.minSeqNoToKeep = seqNo;
    }

    private void deleteRemoteGeneration(Set<Long> generations) {
        try {
            translogTransferManager.deleteTranslogAsync(primaryTermSupplier.getAsLong(), generations);
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Exception occurred while deleting generation {}", generations), e);
        }
    }

    @Override
    public void trimUnreferencedReaders() throws IOException {
        // clean up local translog files and updates readers
        super.trimUnreferencedReaders();

        // cleans up remote translog files not referenced in latest uploaded metadata.
        // This enables us to restore translog from the metadata in case of failover or relocation.
        Set<Long> generationsToDelete = new HashSet<>();
        for (long generation = minRemoteGenReferenced - 1; generation >= 0; generation--) {
            if (fileTransferTracker.uploaded(Translog.getFilename(generation)) == false) {
                break;
            }
            generationsToDelete.add(generation);
        }
        if (generationsToDelete.isEmpty() == false) {
            deleteRemoteGeneration(generationsToDelete);
            deleteOlderPrimaryTranslogFilesFromRemoteStore();
        }
    }

    /**
     * This method must be called only after there are valid generations to delete in trimUnreferencedReaders as it ensures
     * implicitly that minimum primary term in latest translog metadata in remote store is the current primary term.
     */
    private void deleteOlderPrimaryTranslogFilesFromRemoteStore() {
        // The deletion of older translog files in remote store is on best-effort basis, there is a possibility that there
        // are older files that are no longer needed and should be cleaned up. In here, we delete all files that are part
        // of older primary term.
        if (olderPrimaryCleaned.trySet(Boolean.TRUE)) {
            logger.info("Cleaning up translog uploaded by previous primaries");
            long minPrimaryTermInMetadata = current.getPrimaryTerm();
            Set<Long> primaryTermsInRemote;
            try {
                primaryTermsInRemote = translogTransferManager.listPrimaryTerms();
            } catch (IOException e) {
                logger.error("Exception occurred while getting primary terms from remote store", e);
                // If there are exceptions encountered, then we try to delete all older primary terms lesser than the
                // minimum referenced primary term in remote translog metadata.
                primaryTermsInRemote = LongStream.range(0, current.getPrimaryTerm()).boxed().collect(Collectors.toSet());
            }
            // Delete all primary terms that are no more referenced by the metadata file and exists in the
            Set<Long> primaryTermsToDelete = primaryTermsInRemote.stream()
                .filter(term -> term < minPrimaryTermInMetadata)
                .collect(Collectors.toSet());
            primaryTermsToDelete.forEach(term -> translogTransferManager.deleteTranslogAsync(term, new ActionListener<>() {
                @Override
                public void onResponse(Void response) {
                    // NO-OP
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage("Exception occurred while deleting older translog files for primary_term={}", term),
                        e
                    );
                }
            }));
        }
    }
}
