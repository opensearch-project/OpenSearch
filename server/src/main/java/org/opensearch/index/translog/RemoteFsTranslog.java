/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.index.remote.RemoteStorePathType;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogCheckpointTransferSnapshot;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static org.opensearch.index.remote.RemoteStoreDataEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreDataEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreDataEnums.DataType.METADATA;

/**
 * A Translog implementation which syncs local FS with a remote store
 * The current impl uploads translog , ckp and metadata to remote store
 * for every sync, post syncing to disk. Post that, a new generation is
 * created.
 *
 * @opensearch.internal
 */
public class RemoteFsTranslog extends Translog {

    private final Logger logger;
    private final TranslogTransferManager translogTransferManager;
    private final FileTransferTracker fileTransferTracker;
    private final BooleanSupplier startedPrimarySupplier;
    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    private volatile long maxRemoteTranslogGenerationUploaded;

    private volatile long minSeqNoToKeep;

    // min generation referred by last uploaded translog
    private volatile long minRemoteGenReferenced;

    // clean up translog folder uploaded by previous primaries once
    private final SetOnce<Boolean> olderPrimaryCleaned = new SetOnce<>();

    private static final int REMOTE_DELETION_PERMITS = 2;
    private static final int DOWNLOAD_RETRIES = 2;

    // Semaphore used to allow only single remote generation to happen at a time
    private final Semaphore remoteGenerationDeletionPermits = new Semaphore(REMOTE_DELETION_PERMITS);

    // These permits exist to allow any inflight background triggered upload.
    private static final int SYNC_PERMIT = 1;
    private final Semaphore syncPermit = new Semaphore(SYNC_PERMIT);
    private final AtomicBoolean pauseSync = new AtomicBoolean(false);

    public RemoteFsTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        BooleanSupplier startedPrimarySupplier,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker
    ) throws IOException {
        super(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer);
        logger = Loggers.getLogger(getClass(), shardId);
        this.startedPrimarySupplier = startedPrimarySupplier;
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        this.translogTransferManager = buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            indexSettings().getRemoteStorePathType()
        );
        try {
            download(translogTransferManager, location, logger);
            Checkpoint checkpoint = readCheckpoint(location);
            logger.info("Downloaded data from remote translog till maxSeqNo = {}", checkpoint.maxSeqNo);
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                String errorMsg = String.format(Locale.ROOT, "%s at least one reader must be recovered", shardId);
                logger.error(errorMsg);
                throw new IllegalStateException(errorMsg);
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

    // visible for testing
    RemoteTranslogTransferTracker getRemoteTranslogTracker() {
        return remoteTranslogTransferTracker;
    }

    public static void download(
        Repository repository,
        ShardId shardId,
        ThreadPool threadPool,
        Path location,
        RemoteStorePathType pathType,
        Logger logger
    ) throws IOException {
        assert repository instanceof BlobStoreRepository : String.format(
            Locale.ROOT,
            "%s repository should be instance of BlobStoreRepository",
            shardId
        );
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        // We use a dummy stats tracker to ensure the flow doesn't break.
        // TODO: To be revisited as part of https://github.com/opensearch-project/OpenSearch/issues/7567
        RemoteTranslogTransferTracker remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 1000);
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        TranslogTransferManager translogTransferManager = buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            pathType
        );
        RemoteFsTranslog.download(translogTransferManager, location, logger);
        logger.trace(remoteTranslogTransferTracker.toString());
    }

    static void download(TranslogTransferManager translogTransferManager, Path location, Logger logger) throws IOException {
        /*
        In Primary to Primary relocation , there can be concurrent upload and download of translog.
        While translog files are getting downloaded by new primary, it might hence be deleted by the primary
        Hence we retry if tlog/ckp files are not found .

        This doesn't happen in last download , where it is ensured that older primary has stopped modifying tlog data.
         */
        IOException ex = null;
        for (int i = 0; i <= DOWNLOAD_RETRIES; i++) {
            boolean success = false;
            long startTimeMs = System.currentTimeMillis();
            try {
                downloadOnce(translogTransferManager, location, logger);
                success = true;
                return;
            } catch (FileNotFoundException | NoSuchFileException e) {
                // continue till download retries
                ex = e;
            } finally {
                logger.trace("downloadOnce success={} timeElapsed={}", success, (System.currentTimeMillis() - startTimeMs));
            }
        }
        logger.info("Exhausted all download retries during translog/checkpoint file download");
        throw ex;
    }

    private static void downloadOnce(TranslogTransferManager translogTransferManager, Path location, Logger logger) throws IOException {
        logger.debug("Downloading translog files from remote");
        RemoteTranslogTransferTracker statsTracker = translogTransferManager.getRemoteTranslogTransferTracker();
        long prevDownloadBytesSucceeded = statsTracker.getDownloadBytesSucceeded();
        long prevDownloadTimeInMillis = statsTracker.getTotalDownloadTimeInMillis();
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
            logger.info(
                "Downloaded translog and checkpoint files from={} to={}",
                translogMetadata.getMinTranslogGeneration(),
                translogMetadata.getGeneration()
            );

            statsTracker.recordDownloadStats(prevDownloadBytesSucceeded, prevDownloadTimeInMillis);

            // We copy the latest generation .ckp file to translog.ckp so that flows that depend on
            // existence of translog.ckp file work in the same way
            Files.copy(
                location.resolve(Translog.getCommitCheckpointFileName(translogMetadata.getGeneration())),
                location.resolve(Translog.CHECKPOINT_FILE_NAME)
            );
        } else {
            // When code flow reaches this block, it means we don't have any translog files uploaded to remote store.
            // If local filesystem contains empty translog or no translog, we don't do anything.
            // If local filesystem contains non-empty translog, we clean up these files and create empty translog.
            logger.debug("No translog files found on remote, checking local filesystem for cleanup");
            if (FileSystemUtils.exists(location.resolve(CHECKPOINT_FILE_NAME))) {
                final Checkpoint checkpoint = readCheckpoint(location);
                if (isEmptyTranslog(checkpoint) == false) {
                    logger.debug("Translog files exist on local without any metadata in remote, cleaning up these files");
                    // Creating empty translog will cleanup the older un-referenced tranlog files, we don't have to explicitly delete
                    Translog.createEmptyTranslog(location, translogTransferManager.getShardId(), checkpoint);
                } else {
                    logger.debug("Empty translog on local, skipping clean-up");
                }
            }
        }
        logger.debug("downloadOnce execution completed");
    }

    private static boolean isEmptyTranslog(Checkpoint checkpoint) {
        return checkpoint.generation == checkpoint.minTranslogGeneration
            && checkpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED
            && checkpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED
            && checkpoint.numOps == 0;
    }

    public static TranslogTransferManager buildTranslogTransferManager(
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        ShardId shardId,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker tracker,
        RemoteStorePathType pathType
    ) {
        assert Objects.nonNull(pathType);
        String indexUUID = shardId.getIndex().getUUID();
        String shardIdStr = String.valueOf(shardId.id());
        BlobPath dataPath = pathType.path(blobStoreRepository.basePath(), indexUUID, shardIdStr, TRANSLOG, DATA);
        BlobPath mdPath = pathType.path(blobStoreRepository.basePath(), indexUUID, shardIdStr, TRANSLOG, METADATA);
        BlobStoreTransferService transferService = new BlobStoreTransferService(blobStoreRepository.blobStore(), threadPool);
        return new TranslogTransferManager(shardId, transferService, dataPath, mdPath, fileTransferTracker, tracker);
    }

    @Override
    public boolean ensureSynced(Location location) throws IOException {
        assert location.generation <= current.getGeneration();
        if (location.generation == current.getGeneration()) {
            ensureOpen();
            return prepareAndUpload(primaryTermSupplier.getAsLong(), location.generation);
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
        // During primary relocation, both the old and new primary have engine created with RemoteFsTranslog and having
        // ReplicationTracker.primaryMode() as true. However, before we perform the `internal:index/shard/replication/segments_sync`
        // action which re-downloads the segments and translog on the new primary. We are ensuring 2 things here -
        // 1. Using startedPrimarySupplier, we prevent the new primary to do pre-emptive syncs
        // 2. Using syncPermits, we prevent syncs at the desired time during primary relocation.
        if (startedPrimarySupplier.getAsBoolean() == false || syncPermit.tryAcquire(SYNC_PERMIT) == false) {
            logger.debug("skipped uploading translog for {} {} syncPermits={}", primaryTerm, generation, syncPermit.availablePermits());
            // NO-OP
            return false;
        }
        long maxSeqNo = -1;
        try (Releasable ignored = writeLock.acquire()) {
            if (generation == null || generation == current.getGeneration()) {
                try {
                    if (closed.get() == false) {
                        maxSeqNo = getMaxSeqNo();
                    }
                    final TranslogReader reader = current.closeIntoReader();
                    readers.add(reader);
                    copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));
                    if (closed.get() == false) {
                        logger.trace("Creating new writer for gen: [{}]", current.getGeneration() + 1);
                        current = createWriter(current.getGeneration() + 1);
                    }
                    assert writeLock.isHeldByCurrentThread() : "Write lock must be held before we acquire the read lock";
                    // Here we are downgrading the write lock by acquiring the read lock and releasing the write lock
                    // This ensures that other threads can still acquire the read locks while also protecting the
                    // readers and writer to not be mutated any further.
                    readLock.acquire();
                } catch (final Exception e) {
                    tragedy.setTragicException(e);
                    closeOnTragicEvent(e);
                    throw e;
                }
            } else if (generation < current.getGeneration()) {
                return false;
            }
        }

        assert readLock.isHeldByCurrentThread() == true;
        try (Releasable ignored = readLock; Releasable ignoredGenLock = deletionPolicy.acquireTranslogGen(getMinFileGeneration())) {
            // Do we need remote writes in sync fashion ?
            // If we don't , we should swallow FileAlreadyExistsException while writing to remote store
            // and also verify for same during primary-primary relocation
            // Writing remote in sync fashion doesn't hurt as global ckp update
            // is not updated in remote translog except in primary to primary recovery.
            if (generation == null) {
                if (closed.get() == false) {
                    return upload(primaryTerm, current.getGeneration() - 1, maxSeqNo);
                } else {
                    return upload(primaryTerm, current.getGeneration(), maxSeqNo);
                }
            } else {
                return upload(primaryTerm, generation, maxSeqNo);
            }
        }
    }

    private boolean upload(long primaryTerm, long generation, long maxSeqNo) throws IOException {
        logger.trace("uploading translog for primary term {} generation {}", primaryTerm, generation);
        try (
            TranslogCheckpointTransferSnapshot transferSnapshotProvider = new TranslogCheckpointTransferSnapshot.Builder(
                primaryTerm,
                generation,
                location,
                readers,
                Translog::getCommitCheckpointFileName,
                config.getNodeId()
            ).build()
        ) {
            return translogTransferManager.transferSnapshot(
                transferSnapshotProvider,
                new RemoteFsTranslogTransferListener(generation, primaryTerm, maxSeqNo)
            );
        } finally {
            syncPermit.release(SYNC_PERMIT);
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
        if (syncToDisk() || syncNeeded()) {
            prepareAndUpload(primaryTermSupplier.getAsLong(), null);
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
        assert Translog.calledFromOutsideOrViaTragedyClose() : shardId
            + "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        try (ReleasableLock lock = writeLock.acquire()) {
            if (closed.compareAndSet(false, true)) {
                try {
                    sync();
                } finally {
                    logger.debug("translog closed");
                    closeFilesIfNoPendingRetentionLocks();
                }
            }
        }
    }

    protected long getMinReferencedGen() throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        long minReferencedGen = Math.min(
            deletionPolicy.minTranslogGenRequired(readers, current),
            minGenerationForSeqNo(minSeqNoToKeep, current, readers)
        );

        assert minReferencedGen >= getMinFileGeneration() : shardId
            + " deletion policy requires a minReferenceGen of ["
            + minReferencedGen
            + "] but the lowest gen available is ["
            + getMinFileGeneration()
            + "]";
        assert minReferencedGen <= currentFileGeneration() : shardId
            + " deletion policy requires a minReferenceGen of ["
            + minReferencedGen
            + "] which is higher than the current generation ["
            + currentFileGeneration()
            + "]";
        return minReferencedGen;
    }

    protected void setMinSeqNoToKeep(long seqNo) {
        if (seqNo < this.minSeqNoToKeep) {
            throw new IllegalArgumentException(
                shardId + " min seq number required can't go backwards: " + "current [" + this.minSeqNoToKeep + "] new [" + seqNo + "]"
            );
        }
        this.minSeqNoToKeep = seqNo;
    }

    @Override
    protected Releasable drainSync() {
        try {
            if (syncPermit.tryAcquire(SYNC_PERMIT, 1, TimeUnit.MINUTES)) {
                boolean result = pauseSync.compareAndSet(false, true);
                assert result && syncPermit.availablePermits() == 0;
                logger.info("All inflight remote translog syncs finished and further syncs paused");
                return Releasables.releaseOnce(() -> {
                    syncPermit.release(SYNC_PERMIT);
                    boolean wasSyncPaused = pauseSync.getAndSet(false);
                    assert syncPermit.availablePermits() == SYNC_PERMIT : "Available permits is " + syncPermit.availablePermits();
                    assert wasSyncPaused : "RemoteFsTranslog sync was not paused before re-enabling it";
                    logger.info("Resumed remote translog sync back on relocation failure");
                });
            } else {
                throw new TimeoutException("Timeout while acquiring all permits");
            }
        } catch (TimeoutException | InterruptedException e) {
            throw new RuntimeException("Failed to acquire all permits", e);
        }
    }

    @Override
    public void trimUnreferencedReaders() throws IOException {
        // clean up local translog files and updates readers
        super.trimUnreferencedReaders();

        // This is to ensure that after the permits are acquired during primary relocation, there are no further modification on remote
        // store.
        if (startedPrimarySupplier.getAsBoolean() == false || pauseSync.get()) {
            return;
        }

        // Since remote generation deletion is async, this ensures that only one generation deletion happens at a time.
        // Remote generations involves 2 async operations - 1) Delete translog generation files 2) Delete metadata files
        // We try to acquire 2 permits and if we can not, we return from here itself.
        if (remoteGenerationDeletionPermits.tryAcquire(REMOTE_DELETION_PERMITS) == false) {
            return;
        }

        // cleans up remote translog files not referenced in latest uploaded metadata.
        // This enables us to restore translog from the metadata in case of failover or relocation.
        Set<Long> generationsToDelete = new HashSet<>();
        for (long generation = minRemoteGenReferenced - 1 - indexSettings().getRemoteTranslogExtraKeep(); generation >= 0; generation--) {
            if (fileTransferTracker.uploaded(Translog.getFilename(generation)) == false) {
                break;
            }
            generationsToDelete.add(generation);
        }
        if (generationsToDelete.isEmpty() == false) {
            deleteRemoteGeneration(generationsToDelete);
            translogTransferManager.deleteStaleTranslogMetadataFilesAsync(remoteGenerationDeletionPermits::release);
            deleteStaleRemotePrimaryTerms();
        } else {
            remoteGenerationDeletionPermits.release(REMOTE_DELETION_PERMITS);
        }
    }

    /**
     * Deletes remote translog and metadata files asynchronously corresponding to the generations.
     * @param generations generations to be deleted.
     */
    private void deleteRemoteGeneration(Set<Long> generations) {
        translogTransferManager.deleteGenerationAsync(
            primaryTermSupplier.getAsLong(),
            generations,
            remoteGenerationDeletionPermits::release
        );
    }

    /**
     * This method must be called only after there are valid generations to delete in trimUnreferencedReaders as it ensures
     * implicitly that minimum primary term in latest translog metadata in remote store is the current primary term.
     * <br>
     * This will also delete all stale translog metadata files from remote except the latest basis the metadata file comparator.
     */
    private void deleteStaleRemotePrimaryTerms() {
        // The deletion of older translog files in remote store is on best-effort basis, there is a possibility that there
        // are older files that are no longer needed and should be cleaned up. In here, we delete all files that are part
        // of older primary term.
        if (olderPrimaryCleaned.trySet(Boolean.TRUE)) {
            if (readers.isEmpty()) {
                logger.trace("Translog reader list is empty, returning from deleteStaleRemotePrimaryTerms");
                return;
            }
            // First we delete all stale primary terms folders from remote store
            long minimumReferencedPrimaryTerm = readers.stream().map(BaseTranslogReader::getPrimaryTerm).min(Long::compare).get();
            translogTransferManager.deletePrimaryTermsAsync(minimumReferencedPrimaryTerm);
        }
    }

    public static void cleanup(Repository repository, ShardId shardId, ThreadPool threadPool, RemoteStorePathType pathType)
        throws IOException {
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        // We use a dummy stats tracker to ensure the flow doesn't break.
        // TODO: To be revisited as part of https://github.com/opensearch-project/OpenSearch/issues/7567
        RemoteTranslogTransferTracker remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 1000);
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
        TranslogTransferManager translogTransferManager = buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            pathType
        );
        // clean up all remote translog files
        translogTransferManager.deleteTranslogFiles();
    }

    protected void onDelete() {
        ClusterService.assertClusterOrClusterManagerStateThread();
        // clean up all remote translog files
        translogTransferManager.delete();
    }

    // Visible for testing
    boolean isRemoteGenerationDeletionPermitsAvailable() {
        return remoteGenerationDeletionPermits.availablePermits() == REMOTE_DELETION_PERMITS;
    }

    /**
     * TranslogTransferListener implementation for RemoteFsTranslog
     *
     * @opensearch.internal
     */
    private class RemoteFsTranslogTransferListener implements TranslogTransferListener {

        /**
         * Generation for the translog
         */
        private final long generation;

        /**
         * Primary Term for the translog
         */
        private final long primaryTerm;

        private final long maxSeqNo;

        RemoteFsTranslogTransferListener(long generation, long primaryTerm, long maxSeqNo) {
            this.generation = generation;
            this.primaryTerm = primaryTerm;
            this.maxSeqNo = maxSeqNo;
        }

        @Override
        public void onUploadComplete(TransferSnapshot transferSnapshot) throws IOException {
            maxRemoteTranslogGenerationUploaded = generation;
            minRemoteGenReferenced = getMinFileGeneration();
            logger.debug(
                "Successfully uploaded translog for primary term = {}, generation = {}, maxSeqNo = {}",
                primaryTerm,
                generation,
                maxSeqNo
            );
        }

        @Override
        public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) throws IOException {
            if (ex instanceof IOException) {
                throw (IOException) ex;
            } else {
                throw (RuntimeException) ex;
            }
        }
    }

    @Override
    public long getMinUnreferencedSeqNoInSegments(long minUnrefCheckpointInLastCommit) {
        return minSeqNoToKeep;
    }

    // Visible for testing
    int availablePermits() {
        return syncPermit.availablePermits();
    }
}
