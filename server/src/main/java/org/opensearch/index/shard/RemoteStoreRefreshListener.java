/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public final class RemoteStoreRefreshListener extends ReleasableRetryableRefreshListener {

    private final Logger logger;

    /**
     * The initial retry interval at which the retry job gets scheduled after a failure.
     */
    private static final int REMOTE_REFRESH_RETRY_BASE_INTERVAL_MILLIS = 1_000;

    /**
     * In an exponential back off setup, the maximum retry interval after the retry interval increases exponentially.
     */
    private static final int REMOTE_REFRESH_RETRY_MAX_INTERVAL_MILLIS = 10_000;

    private static final int INVALID_PRIMARY_TERM = -1;

    /**
     * Exponential back off policy with max retry interval.
     */
    private static final BackoffPolicy EXPONENTIAL_BACKOFF_POLICY = BackoffPolicy.exponentialEqualJitterBackoff(
        REMOTE_REFRESH_RETRY_BASE_INTERVAL_MILLIS,
        REMOTE_REFRESH_RETRY_MAX_INTERVAL_MILLIS
    );

    public static final Set<String> EXCLUDE_FILES = Set.of("write.lock");

    private final IndexShard indexShard;
    private final CompositeStoreDirectory compositeStoreDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final RemoteSegmentTransferTracker segmentTracker;
    private final Map<String, String> localSegmentChecksumMap;
    private volatile long primaryTerm;
    private volatile Iterator<TimeValue> backoffDelayIterator;
    private final SegmentReplicationCheckpointPublisher checkpointPublisher;
    private final RemoteStoreSettings remoteStoreSettings;
    private final RemoteStoreUploader remoteStoreUploader;

    public RemoteStoreRefreshListener(
        IndexShard indexShard,
        SegmentReplicationCheckpointPublisher checkpointPublisher,
        RemoteSegmentTransferTracker segmentTracker,
        RemoteStoreSettings remoteStoreSettings
    ) {
        super(indexShard.getThreadPool());
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.compositeStoreDirectory = indexShard.store().compositeStoreDirectory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        remoteStoreUploader = new RemoteStoreUploaderService(indexShard, compositeStoreDirectory, remoteDirectory);
        localSegmentChecksumMap = new HashMap<>();
        RemoteSegmentMetadata remoteSegmentMetadata = null;
        if (indexShard.routingEntry().primary()) {
            try {
                remoteSegmentMetadata = this.remoteDirectory.init();
            } catch (IOException e) {
                logger.error("Exception while initialising RemoteSegmentStoreDirectory", e);
            }
        }
        // initializing primary term with the primary term of latest metadata in remote store.
        // if no metadata is present, this value will be initialized with -1.
        this.primaryTerm = remoteSegmentMetadata != null ? remoteSegmentMetadata.getPrimaryTerm() : INVALID_PRIMARY_TERM;
        this.segmentTracker = segmentTracker;
        resetBackOffDelayIterator();
        this.checkpointPublisher = checkpointPublisher;
        this.remoteStoreSettings = remoteStoreSettings;
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    protected void runAfterRefreshExactlyOnce(boolean didRefresh) {
        // We have 2 separate methods to check if sync needs to be done or not. This is required since we use the return boolean
        // from isReadyForUpload to schedule refresh retries as the index shard or the primary mode are not in complete
        // ready state.
        if (shouldSync(didRefresh, true) && isReadyForUpload()) {
            try {
                segmentTracker.updateLocalRefreshTimeAndSeqNo();
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotRef = indexShard.getCatalogSnapshotFromEngine()) {
            Collection<FileMetadata> localFilesPostRefresh = catalogSnapshotRef.getRef().getFileMetadataList();
                    updateLocalSizeMapAndTracker(localFilesPostRefresh);
                }
            } catch (Throwable t) {
                logger.error("Exception in runAfterRefreshExactlyOnce() method", t);
            }
        }
    }

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * This method also uploads remote_segments_metadata file which contains metadata of each segment file uploaded.
     *
     * @param didRefresh true if the refresh opened a new reference
     * @return true if the method runs successfully.
     */
    @Override
    protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
        boolean successful;
        if (shouldSync(didRefresh, false)) {
            successful = syncSegments();
        } else {
            successful = true;
        }
        return successful;
    }

    /**
     * This checks if there is a sync required to remote.
     *
     * @param didRefresh             if the readers changed.
     * @param skipPrimaryTermCheck consider change in primary term or not for should sync
     * @return true if sync is needed
     */
    private boolean shouldSync(boolean didRefresh, boolean skipPrimaryTermCheck) {
        // Ignore syncing segments if the underlying shard is closed
        // This also makes sure that retries are not scheduled for shards
        // with failed syncSegments invocation after they are closed
        if (shardClosed()) {
            logger.info("Shard is already closed. Not attempting sync to remote store");
            return false;
        }
        boolean shouldSync = didRefresh // If the readers change, didRefresh is always true.
            // The third condition exists for uploading the zero state segments where the refresh has not changed the reader
            // reference, but it is important to upload the zero state segments so that the restore does not break.
            || remoteDirectory.getSegmentsUploadedToRemoteStore().isEmpty()
            // When the shouldSync is called the first time, then 1st condition on primary term is true. But after that
            // we update the primary term and the same condition would not evaluate to true again in syncSegments.
            // Below check ensures that if there is commit, then that gets picked up by both 1st and 2nd shouldSync call.
            || isRefreshAfterCommitSafe()
            || isRemoteSegmentStoreInSync() == false;
        if (shouldSync || skipPrimaryTermCheck) {
            return shouldSync;
        }
        return this.primaryTerm != indexShard.getOperationPrimaryTerm();
    }

    /**
     * Checks if all files present in local store are uploaded to remote store or part of excluded files.
     *
     * Different from IndexShard#isRemoteSegmentStoreInSync as
     *  it uses files uploaded cache in RemoteDirector and it doesn't make a remote store call.
     *  Doesn't throw an exception on store getting closed as store will be open
     *
     *
     * @return true iff all the local files are uploaded to remote store.
     */
    boolean isRemoteSegmentStoreInSync() {
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotRef = indexShard.getCatalogSnapshotFromEngine()) {
            Collection<FileMetadata> localFiles = catalogSnapshotRef.getRef().getFileMetadataList();
            return localFiles.stream().allMatch(this::skipUpload);
        } catch (Throwable throwable) {
            logger.error("Throwable thrown during isRemoteSegmentStoreInSync", throwable);
        }
        return false;
    }

    /*
     @return false if retry is needed
     */
    private boolean syncSegments() {
        if (isReadyForUpload() == false) {
            // Following check is required to enable retry and make sure that we do not lose this refresh event
            // When primary shard is restored from remote store, the recovery happens first followed by changing
            // primaryMode to true. Due to this, the refresh that is triggered post replay of translog will not go through
            // if following condition does not exist. The segments created as part of translog replay will not be present
            // in the remote store.
            return indexShard.state() != IndexShardState.STARTED || !(indexShard.getIndexer() instanceof InternalEngine || indexShard.getIndexer() instanceof CompositeEngine);
        }
        beforeSegmentsSync();
        long refreshTimeMs = segmentTracker.getLocalRefreshTimeMs(), refreshClockTimeMs = segmentTracker.getLocalRefreshClockTimeMs();
        long refreshSeqNo = segmentTracker.getLocalRefreshSeqNo();
        long bytesBeforeUpload = segmentTracker.getUploadBytesSucceeded(), startTimeInNS = System.nanoTime();
        final AtomicBoolean successful = new AtomicBoolean(false);

        try {
            try {
                initializeRemoteDirectoryOnTermUpdate();
                // if a new segments_N file is present in local that is not uploaded to remote store yet, it
                // is considered as a first refresh post commit. A cleanup of stale commit files is triggered.
                // This is done to avoid delete post each refresh.
                // ToDo: @Kamal, Update while implementing Snapshot restore
                if (isRefreshAfterCommit()) {
                    remoteDirectory.deleteStaleSegmentsAsync(indexShard.getRemoteStoreSettings().getMinRemoteSegmentMetadataFiles());
                }

                CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotRef = indexShard.getCatalogSnapshotFromEngine();
                CatalogSnapshot catalogSnapshot = catalogSnapshotRef.getRef();
                final ReplicationCheckpoint checkpoint = indexShard.computeReplicationCheckpoint(catalogSnapshot);
                if (checkpoint.getPrimaryTerm() != indexShard.getOperationPrimaryTerm()) {
                    throw new IllegalStateException(
                        String.format(
                            Locale.ROOT,
                            "primaryTerm mismatch during segments upload to remote store [%s] != [%s]",
                            checkpoint.getPrimaryTerm(),
                            indexShard.getOperationPrimaryTerm()
                        )
                    );
                }
                // Capture replication checkpoint before uploading the segments as upload can take some time and checkpoint can
                // move.
                long lastRefreshedCheckpoint = indexShard.getIndexer().lastRefreshedCheckpoint();

                Collection<FileMetadata> localFilesPostRefresh = catalogSnapshot.getFileMetadataList();

                // Log format-aware statistics
                Map<String, Long> formatCounts = localFilesPostRefresh.stream()
                    .collect(Collectors.groupingBy(
                        fm -> fm.dataFormat(),
                        Collectors.counting()
                    ));

                logger.debug("Format-aware segment upload initiated: totalFiles={}, formatBreakdown={}",
                    localFilesPostRefresh.size(), formatCounts);

                Map<FileMetadata, Long> fileMetadataToSizeMap = updateLocalSizeMapAndTracker(localFilesPostRefresh);

                CountDownLatch latch = new CountDownLatch(1);
                ActionListener<Void> segmentUploadsCompletedListener = new LatchedActionListener<>(
                    ActionListener.runAfter(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            try {
                                logger.debug("New segments upload successful");
                                // Start metadata file upload
                                uploadMetadata(localFilesPostRefresh, catalogSnapshot, checkpoint);
                                logger.debug("Metadata upload successful");
                                clearStaleFilesFromLocalSegmentChecksumMap(localFilesPostRefresh);
                                onSuccessfulSegmentsSync(
                                    refreshTimeMs,
                                    refreshClockTimeMs,
                                    refreshSeqNo,
                                    lastRefreshedCheckpoint,
                                    fileMetadataToSizeMap,
                                    checkpoint
                                );
                                // At this point since we have uploaded new segments, segment infos and segment metadata file,
                                // along with marking minSeqNoToKeep, upload has succeeded completely.
                                successful.set(true);
                            } catch (Exception e) {
                                // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                                // as part of exponential back-off retry logic. This should not affect durability of the indexed data
                                // with remote trans-log integration.
                                logger.warn("Exception in post new segment upload actions", e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("Exception while uploading new segments to the remote segment store", e);
                        }
                    }, () -> releaseCatalogSnapshot(catalogSnapshotRef)),
                    latch
                );

                // Start the segments files upload using FileMetadata
                uploadNewSegments(localFilesPostRefresh, fileMetadataToSizeMap, segmentUploadsCompletedListener);
                if (latch.await(
                    remoteStoreSettings.getClusterRemoteSegmentTransferTimeout().millis(),
                    TimeUnit.MILLISECONDS
                ) == false) {
                    throw new SegmentUploadFailedException("Timeout while waiting for remote segment transfer to complete");
                }
            }
            catch (IOException e) {
                // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                // as part of exponential back-off retry logic. This should not affect durability of the indexed data
                // with remote trans-log integration.
                logger.warn("Exception while uploading new segments to the remote segment store", e);
            }
        } catch (Throwable t) {
            logger.error("Exception in RemoteStoreRefreshListener.afterRefresh()", t);
        }
        updateFinalStatusInSegmentTracker(successful.get(), bytesBeforeUpload, startTimeInNS);
        // If there are failures in uploading segments, then we should retry as search idle can lead to
        // refresh not occurring until write happens.
        logger.debug("syncSegments runStatus={}", successful.get());
        return successful.get();
    }

    private void releaseCatalogSnapshot(CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotRef) {
        try {
            catalogSnapshotRef.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Uploads new segment files to the remote store.
     *
     * @param localFilesPostRefresh collection of FileMetadata objects containing format and file information
     * @param localFileMetadataSizeMap map of segment file names to their sizes
     * @param segmentUploadsCompletedListener listener to be notified when upload completes
     */
    private void uploadNewSegments(
        Collection<FileMetadata> localFilesPostRefresh,
        Map<FileMetadata, Long> localFileMetadataSizeMap,
        ActionListener<Void> segmentUploadsCompletedListener
    ) {
        // Filter FileMetadata objects based on whether their files should be uploaded
        Collection<FileMetadata> filteredFileMetadata = localFilesPostRefresh.stream()
            .filter(fileMetadata -> !skipUpload(fileMetadata))
            .collect(Collectors.toList());

        // Log format-aware upload statistics
        Map<String, Long> uploadFormatCounts = filteredFileMetadata.stream()
            .collect(Collectors.groupingBy(
                fm -> fm.dataFormat(),
                Collectors.counting()
            ));

        Map<String, Long> skippedFormatCounts = localFilesPostRefresh.stream()
            .filter(fileMetadata -> skipUpload(fileMetadata))
            .collect(Collectors.groupingBy(
                fm -> fm.dataFormat(),
                Collectors.counting()
            ));

        logger.debug("Format-aware upload filtering: totalFiles={}, uploadFiles={}, skippedFiles={}, " +
                    "uploadFormats={}, skippedFormats={}",
                    localFilesPostRefresh.size(), filteredFileMetadata.size(),
                    localFilesPostRefresh.size() - filteredFileMetadata.size(),
                    uploadFormatCounts, skippedFormatCounts);

        Function<Map<FileMetadata, Long>, UploadListener> uploadListenerFunction = (Map<FileMetadata, Long> sizeMap) -> createUploadListener(
            localFileMetadataSizeMap
        );

        // Pass FileMetadata collection to RemoteStoreUploaderService
        remoteStoreUploader.uploadSegments(
            filteredFileMetadata,
            localFileMetadataSizeMap,
            segmentUploadsCompletedListener,
            uploadListenerFunction,
            isLowPriorityUpload()
        );
    }

    /**
     * Clears the stale files from the latest local segment checksum map.
     *
     * @param localSegmentsPostRefresh collection of FileMetadata for files present post refresh
     */
    private void clearStaleFilesFromLocalSegmentChecksumMap(Collection<FileMetadata> localSegmentsPostRefresh) {
        Set<String> currentFileNames = localSegmentsPostRefresh.stream()
            .map(FileMetadata::file)
            .collect(Collectors.toSet());
        localSegmentChecksumMap.keySet().removeIf(key -> !currentFileNames.contains(key));
    }

    private void beforeSegmentsSync() {
        if(segmentTracker != null) {
            // Start tracking total uploads started
            segmentTracker.incrementTotalUploadsStarted();
        }
    }

    private void onSuccessfulSegmentsSync(
        long refreshTimeMs,
        long refreshClockTimeMs,
        long refreshSeqNo,
        long lastRefreshedCheckpoint,
        Map<FileMetadata, Long> localFileSizeMap,
        ReplicationCheckpoint checkpoint
    ) {
        // Update latest uploaded segment files name in segment tracker
        segmentTracker.setLatestUploadedFiles(localFileSizeMap.keySet());
        // Update the remote refresh time and refresh seq no
        updateRemoteRefreshTimeAndSeqNo(refreshTimeMs, refreshClockTimeMs, refreshSeqNo);
        // Reset the backoffDelayIterator for the future failures
        resetBackOffDelayIterator();
        // Set the minimum sequence number for keeping translog
        indexShard.getIndexer().translogManager().setMinSeqNoToKeep(lastRefreshedCheckpoint + 1);
        // Publishing the new checkpoint which is used for remote store + segrep indexes
        checkpointPublisher.publish(indexShard, checkpoint);
        logger.debug("onSuccessfulSegmentsSync lastRefreshedCheckpoint={} checkpoint={}", lastRefreshedCheckpoint, checkpoint);
    }

    /**
     * Resets the backoff delay iterator so that the next set of failures starts with the base delay and goes upto max delay.
     */
    private void resetBackOffDelayIterator() {
        backoffDelayIterator = EXPONENTIAL_BACKOFF_POLICY.iterator();
    }

    @Override
    protected TimeValue getNextRetryInterval() {
        return backoffDelayIterator.next();
    }

    @Override
    protected String getRetryThreadPoolName() {
        return ThreadPool.Names.REMOTE_REFRESH_RETRY;
    }

    private boolean isRefreshAfterCommit() throws IOException {
        // ToDo:@Kamal Get last commit generation from catalogSnapshot
        // String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(compositeStoreDirectory);
//        return (lastCommittedLocalSegmentFileName != null
//            && !remoteDirectory.containsFile(lastCommittedLocalSegmentFileName, getChecksumOfLocalFile(lastCommittedLocalSegmentFileName)));
        return true;
    }

    /**
     * Returns if the current refresh has happened after a commit.
     * @return true if this refresh has happened on account of a commit. If otherwise or exception, returns false.
     */
    private boolean isRefreshAfterCommitSafe() {
        try {
            return isRefreshAfterCommit();
        } catch (Exception e) {
            logger.info("Exception occurred in isRefreshAfterCommitSafe", e);
        }
        return false;
    }

    // ToDo:@Kamal Update MaxSeqNo
    void uploadMetadata(Collection<FileMetadata> localFilesPostRefresh, CatalogSnapshot catalogSnapshot, ReplicationCheckpoint replicationCheckpoint)
        throws IOException {
        final long maxSeqNo = indexShard.getIndexer().currentOngoingRefreshCheckpoint();

        CatalogSnapshot catalogSnapshotCloned = catalogSnapshot.cloneNoAcquire();

        // Create mutable copy and update checkpoint fields while preserving ALL existing metadata
        catalogSnapshotCloned.getUserData().put(LOCAL_CHECKPOINT_KEY, String.valueOf(maxSeqNo));
        catalogSnapshotCloned.getUserData().put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));

        // Log for verification during debugging
        logger.debug("Uploading metadata with userData: translog_uuid={}, history_uuid={}, all_keys={}",
                   catalogSnapshotCloned.getUserData().get(Translog.TRANSLOG_UUID_KEY),
                   catalogSnapshotCloned.getUserData().get(org.opensearch.index.engine.Engine.HISTORY_UUID_KEY),
                   catalogSnapshotCloned.getUserData().keySet());

        Translog.TranslogGeneration translogGeneration = indexShard.getIndexer().translogManager().getTranslogGeneration();
        if (translogGeneration == null) {
            throw new UnsupportedOperationException("Encountered null TranslogGeneration while uploading metadata to remote segment store");
        } else {
            long translogFileGeneration = translogGeneration.translogFileGeneration;
            remoteDirectory.uploadMetadata(
                localFilesPostRefresh.stream().map(FileMetadata::serialize).collect(Collectors.toList()),
                catalogSnapshotCloned,
                compositeStoreDirectory,
                translogFileGeneration,
                replicationCheckpoint,
                indexShard.getNodeId()
            );
        }
    }

    boolean isLowPriorityUpload() {
        return isLocalOrSnapshotRecoveryOrSeeding();
    }

    /**
     * Whether to upload a file or not depending on whether file is in excluded list or has been already uploaded.
     *
     * @param fileMetadata that needs to be uploaded.
     * @return true if the upload has to be skipped for the file.
     */
    private boolean skipUpload(FileMetadata fileMetadata) {
        try {
            // Exclude files that are already uploaded and the exclude files to come up with the list of files to be uploaded.
            return EXCLUDE_FILES.contains(fileMetadata.file()) || remoteDirectory.containsFile(fileMetadata.serialize(), getChecksumOfLocalFile(fileMetadata));
        } catch (IOException e) {
            logger.error(
                "Exception while reading checksum of local segment file: {}, format: {}, ignoring the exception and re-uploading the file",
                fileMetadata.file(), fileMetadata.dataFormat()
            );
        }
        return false;
    }

    private String getChecksumOfLocalFile(FileMetadata fileMetadata) throws IOException {
        if (!localSegmentChecksumMap.containsKey(fileMetadata.file())) {
            try{
                String checksum = Long.toString(compositeStoreDirectory.calculateChecksum(fileMetadata));
                localSegmentChecksumMap.put(fileMetadata.file(), checksum);
                logger.debug("Calculated checksum for file: {}, format: {}, checksum: {}",
                            fileMetadata.file(), fileMetadata.dataFormat(), checksum);
            }
            catch (IOException e){
                logger.error("Failed to calculate checksum for file: {}, format: {}",
                           fileMetadata.file(), fileMetadata.dataFormat(), e);
                throw e;
            }
        }
        return localSegmentChecksumMap.get(fileMetadata.file());
    }

    /**
     * Updates the last refresh time and refresh seq no which is seen by remote store.
     */
    private void updateRemoteRefreshTimeAndSeqNo(long refreshTimeMs, long refreshClockTimeMs, long refreshSeqNo) {
        segmentTracker.updateRemoteRefreshClockTimeMs(refreshClockTimeMs);
        segmentTracker.updateRemoteRefreshTimeMs(refreshTimeMs);
        segmentTracker.updateRemoteRefreshSeqNo(refreshSeqNo);
    }

    /**
     * Updates map of FileMetadata to size of the input segment files in the segment tracker.
     * Uses CompositeStoreDirectory.fileLength(FileMetadata) for efficient format-aware file size retrieval.
     *
     * @param localFilesPostRefresh collection of FileMetadata for files that are part of the most recent local refresh.
     *
     * @return updated map of FileMetadata to file size
     */
    private Map<FileMetadata, Long> updateLocalSizeMapAndTracker(Collection<FileMetadata> localFilesPostRefresh) {
        Map<FileMetadata, Long> fileSizeMap = new HashMap<>();

        for (FileMetadata fileMetadata : localFilesPostRefresh) {
            try {
                long fileSize = compositeStoreDirectory.fileLength(fileMetadata);
                fileSizeMap.put(fileMetadata, fileSize);
            } catch (IOException e) {
                logger.warn("Failed to get file length for file: {}, format: {}",
                           fileMetadata.file(), fileMetadata.dataFormat(), e);
            }
        }

        // Update segment tracker with FileMetadata-based map
        segmentTracker.updateLatestLocalFileNameLengthMap(fileSizeMap);

        return fileSizeMap;
    }

    private void updateFinalStatusInSegmentTracker(boolean uploadStatus, long bytesBeforeUpload, long startTimeInNS) {
        if (uploadStatus) {
            long bytesUploaded = segmentTracker.getUploadBytesSucceeded() - bytesBeforeUpload;
            long timeTakenInMS = TimeValue.nsecToMSec(System.nanoTime() - startTimeInNS);
            segmentTracker.incrementTotalUploadsSucceeded();
            segmentTracker.updateUploadBytesMovingAverage(bytesUploaded);
            segmentTracker.updateUploadBytesPerSecMovingAverage((bytesUploaded * 1_000L) / Math.max(1, timeTakenInMS));
            segmentTracker.updateUploadTimeMovingAverage(timeTakenInMS);
        } else {
            segmentTracker.incrementTotalUploadsFailed();
        }
    }

    /**
     * On primary term update, we (re)initialise the remote segment directory to reflect the latest metadata file that
     * has been uploaded to remote store successfully. This method also updates the segment tracker about the latest
     * uploaded segment files onto remote store.
     */
    private void initializeRemoteDirectoryOnTermUpdate() throws IOException {
        if (this.primaryTerm != indexShard.getOperationPrimaryTerm()) {
            logger.trace("primaryTerm update from={} to={}", primaryTerm, indexShard.getOperationPrimaryTerm());
            this.primaryTerm = indexShard.getOperationPrimaryTerm();
            RemoteSegmentMetadata uploadedMetadata = this.remoteDirectory.init();

            // During failover, the uploaded metadata would have names of files that have been uploaded to remote store.
            // Here we update the tracker with latest remote uploaded files.
            if (uploadedMetadata != null) {
                segmentTracker.setLatestUploadedFiles(uploadedMetadata.getMetadata().keySet().stream().map(FileMetadata::new).collect(Collectors.toSet()));
            }
        }
    }

    /**
     * This checks for readiness of the index shard and primary mode. This has separated from shouldSync since we use the
     * returned value of this method for scheduling retries in syncSegments method.
     * @return true iff the shard is a started with primary mode true or it is local or snapshot recovery.
     */
    private boolean isReadyForUpload() {
        boolean isReady = indexShard.isStartedPrimary() || isLocalOrSnapshotRecoveryOrSeeding();

        if (isReady == false) {
            StringBuilder sb = new StringBuilder("Skipped syncing segments with");
            if (indexShard.getReplicationTracker() != null) {
                sb.append(" primaryMode=").append(indexShard.getReplicationTracker().isPrimaryMode());
            }
            if (indexShard.state() != null) {
                sb.append(" indexShardState=").append(indexShard.state());
            }
            if (indexShard.getIndexerOrNull() != null) {
                sb.append(" engineType=").append(indexShard.getIndexer().getClass().getSimpleName());
            }
            if (indexShard.recoveryState() != null) {
                sb.append(" recoverySourceType=").append(indexShard.recoveryState().getRecoverySource().getType());
                sb.append(" primary=").append(indexShard.shardRouting.primary());
            }
            logger.info(sb.toString());
        }
        return isReady;
    }

    boolean isLocalOrSnapshotRecoveryOrSeeding() {
        // In this case when the primary mode is false, we need to upload segments to Remote Store
        // This is required in case of remote migration seeding/snapshots/shrink/ split/clone where we need to durable persist
        // all segments to remote before completing the recovery to ensure durability.
        return (indexShard.state() == IndexShardState.RECOVERING && indexShard.shardRouting.primary())
            && indexShard.recoveryState() != null
            && (indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS
                || indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT
                || indexShard.shouldSeedRemoteStore());
    }

    /**
     * Creates an {@link UploadListener} containing the stats population logic which would be triggered before and after segment upload events
     *
     * @param fileSizeMap updated map of current snapshot of local segments to their sizes
     */
    private UploadListener createUploadListener(Map<FileMetadata, Long> fileSizeMap) {
        return new UploadListener() {
            private long uploadStartTime = 0;

            @Override
            public void beforeUpload(FileMetadata fileMetadata) {
                // Start tracking the upload bytes started
                segmentTracker.addUploadBytesStarted(fileSizeMap.get(fileMetadata));
                uploadStartTime = System.currentTimeMillis();
            }

            @Override
            public void onSuccess(FileMetadata fileMetadata) {
                // Track upload success
                segmentTracker.addUploadBytesSucceeded(fileSizeMap.get(fileMetadata));
                segmentTracker.addToLatestUploadedFiles(fileMetadata);
                segmentTracker.addUploadTimeInMillis(Math.max(1, System.currentTimeMillis() - uploadStartTime));
            }

            @Override
            public void onFailure(FileMetadata fileMetadata) {
                // Track upload failure
                segmentTracker.addUploadBytesFailed(fileSizeMap.get(fileMetadata));
                segmentTracker.addUploadTimeInMillis(Math.max(1, System.currentTimeMillis() - uploadStartTime));
            }
        };
    }

    /**
     * Checks if the underlying IndexShard instance is closed
     *
     * @return true if it is closed, false otherwise
     */
    private boolean shardClosed() {
        return indexShard.state() == IndexShardState.CLOSED;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected boolean isRetryEnabled() {
        return true;
    }
}
