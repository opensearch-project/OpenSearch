/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
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
public final class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    /**
     * The initial retry interval at which the retry job gets scheduled after a failure.
     */
    private static final int REMOTE_REFRESH_RETRY_BASE_INTERVAL_MILLIS = 1_000;

    /**
     * In an exponential back off setup, the maximum retry interval after the retry interval increases exponentially.
     */
    private static final int REMOTE_REFRESH_RETRY_MAX_INTERVAL_MILLIS = 10_000;

    /**
     * Exponential back off policy with max retry interval.
     */
    private static final BackoffPolicy EXPONENTIAL_BACKOFF_POLICY = BackoffPolicy.exponentialEqualJitterBackoff(
        REMOTE_REFRESH_RETRY_BASE_INTERVAL_MILLIS,
        REMOTE_REFRESH_RETRY_MAX_INTERVAL_MILLIS
    );

    // Visible for testing
    static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    // Visible for testing
    static final int LAST_N_METADATA_FILES_TO_KEEP = 10;
    static final String SEGMENT_INFO_SNAPSHOT_FILENAME_PREFIX = "segment_infos_snapshot_filename";

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final RemoteRefreshSegmentTracker segmentTracker;
    private final Map<String, String> localSegmentChecksumMap;
    private long primaryTerm;

    /**
     * Semaphore that ensures there is only 1 retry scheduled at any time.
     */
    private final Semaphore SCHEDULE_RETRY_PERMITS = new Semaphore(1);

    private volatile Iterator<TimeValue> backoffDelayIterator;

    private volatile Scheduler.ScheduledCancellable scheduledCancellableRetry;

    /**
     * This keeps the pending refresh time which is set before the refresh. This is used to set the last refresh time.
     */
    private volatile long pendingRefreshTimeMs;

    /**
     * Keeps track of segment files and their size in bytes which are part of the most recent refresh.
     */
    private final Map<String, Long> latestFileNameSizeOnLocalMap = new HashMap<>();

    private final SegmentReplicationCheckpointPublisher checkpointPublisher;

    public RemoteStoreRefreshListener(
        IndexShard indexShard,
        SegmentReplicationCheckpointPublisher checkpointPublisher,
        RemoteRefreshSegmentTracker segmentTracker
    ) {
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        this.primaryTerm = indexShard.getOperationPrimaryTerm();
        localSegmentChecksumMap = new HashMap<>();
        if (indexShard.routingEntry().primary()) {
            try {
                this.remoteDirectory.init();
            } catch (IOException e) {
                logger.error("Exception while initialising RemoteSegmentStoreDirectory", e);
            }
        }
        this.segmentTracker = segmentTracker;
        resetBackOffDelayIterator();
        this.checkpointPublisher = checkpointPublisher;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Set the pending refresh time which gets set on account of success refresh.
        pendingRefreshTimeMs = System.nanoTime() / 1_000_000L;
    }

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * This method also uploads remote_segments_metadata file which contains metadata of each segment file uploaded.
     *
     * @param didRefresh true if the refresh opened a new reference
     */
    @Override
    public void afterRefresh(boolean didRefresh) {

        if (didRefresh) {
            updateLocalRefreshTimeAndSeqNo();
        }

        try {
            indexShard.getThreadPool().executor(ThreadPool.Names.REMOTE_REFRESH).submit(() -> syncSegments(false)).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.info("Exception occurred while scheduling syncSegments", e);
        }
    }

    private synchronized void syncSegments(boolean isRetry) {
        if (indexShard.getReplicationTracker().isPrimaryMode() == false) {
            return;
        }
        beforeSegmentsSync(isRetry);
        long refreshTimeMs = segmentTracker.getLocalRefreshTimeMs(), refreshSeqNo = segmentTracker.getLocalRefreshSeqNo();
        long bytesBeforeUpload = segmentTracker.getUploadBytesSucceeded(), startTimeInNS = System.nanoTime();
        boolean shouldRetry = true;
        try {
            // Start tracking total uploads started
            segmentTracker.incrementTotalUploadsStarted();

            if (this.primaryTerm != indexShard.getOperationPrimaryTerm()) {
                this.primaryTerm = indexShard.getOperationPrimaryTerm();
                this.remoteDirectory.init();
            }
            try {
                // if a new segments_N file is present in local that is not uploaded to remote store yet, it
                // is considered as a first refresh post commit. A cleanup of stale commit files is triggered.
                // This is done to avoid delete post each refresh.
                // Ideally, we want this to be done in async flow. (GitHub issue #4315)
                if (isRefreshAfterCommit()) {
                    deleteStaleCommits();
                }

                String segmentInfoSnapshotFilename = null;
                try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                    SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();
                    // Capture replication checkpoint before uploading the segments as upload can take some time and checkpoint can
                    // move.
                    ReplicationCheckpoint checkpoint = indexShard.getLatestReplicationCheckpoint();
                    Collection<String> localSegmentsPostRefresh = segmentInfos.files(true);

                    List<String> segmentInfosFiles = localSegmentsPostRefresh.stream()
                        .filter(file -> file.startsWith(IndexFileNames.SEGMENTS))
                        .collect(Collectors.toList());
                    Optional<String> latestSegmentInfos = segmentInfosFiles.stream()
                        .max(Comparator.comparingLong(SegmentInfos::generationFromSegmentsFileName));

                    if (latestSegmentInfos.isPresent()) {
                        // SegmentInfosSnapshot is a snapshot of reader's view of segments and may not contain
                        // all the segments from last commit if they are merged away but not yet committed.
                        // Each metadata file in the remote segment store represents a commit and the following
                        // statement keeps sure that each metadata will always contain all the segments from last commit + refreshed
                        // segments.
                        localSegmentsPostRefresh.addAll(SegmentInfos.readCommit(storeDirectory, latestSegmentInfos.get()).files(true));
                        segmentInfosFiles.stream()
                            .filter(file -> !file.equals(latestSegmentInfos.get()))
                            .forEach(localSegmentsPostRefresh::remove);

                        // Create a map of file name to size and update the refresh segment tracker
                        Map<String, Long> currentLocalSizeMap = createSizeMap(localSegmentsPostRefresh);
                        segmentTracker.setLatestLocalFileNameLengthMap(currentLocalSizeMap);

                        // Start the segments files upload
                        boolean newSegmentsUploadStatus = uploadNewSegments(localSegmentsPostRefresh, currentLocalSizeMap);
                        if (newSegmentsUploadStatus) {
                            segmentInfoSnapshotFilename = uploadSegmentInfosSnapshot(latestSegmentInfos.get(), segmentInfos);
                            localSegmentsPostRefresh.add(segmentInfoSnapshotFilename);

                            // Start metadata file upload
                            remoteDirectory.uploadMetadata(
                                localSegmentsPostRefresh,
                                storeDirectory,
                                indexShard.getOperationPrimaryTerm(),
                                segmentInfos.getGeneration()
                            );
                            // Update latest uploaded segment files name in segment tracker
                            segmentTracker.setLatestUploadedFiles(currentLocalSizeMap.keySet());
                            // Update the remote refresh time and refresh seq no
                            updateRemoteRefreshTimeAndSeqNo(refreshTimeMs, refreshSeqNo);
                            clearStaleFilesFromLocalSegmentChecksumMap(localSegmentsPostRefresh);
                            onSuccessfulSegmentsSync();
                            final long lastRefreshedCheckpoint = ((InternalEngine) indexShard.getEngine()).lastRefreshedCheckpoint();
                            indexShard.getEngine().translogManager().setMinSeqNoToKeep(lastRefreshedCheckpoint + 1);
                            checkpointPublisher.publish(indexShard, checkpoint);
                            // At this point since we have uploaded new segments, segment infos and segment metadata file,
                            // along with marking minSeqNoToKeep, upload has succeeded completely.
                            shouldRetry = false;
                        }
                    }
                } catch (EngineException e) {
                    logger.warn("Exception while reading SegmentInfosSnapshot", e);
                } finally {
                    try {
                        if (segmentInfoSnapshotFilename != null) {
                            storeDirectory.deleteFile(segmentInfoSnapshotFilename);
                        }
                    } catch (IOException e) {
                        logger.warn("Exception while deleting: " + segmentInfoSnapshotFilename, e);
                    }
                }
            } catch (IOException e) {
                // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                // in the next refresh. This should not affect durability of the indexed data after remote trans-log integration.
                logger.warn("Exception while uploading new segments to the remote segment store", e);
            }
        } catch (Throwable t) {
            logger.error("Exception in RemoteStoreRefreshListener.afterRefresh()", t);
        } finally {
            // Update the segment tracker with the final upload status as seen at the end
            updateFinalUploadStatusInSegmentTracker(shouldRetry == false, bytesBeforeUpload, startTimeInNS);
        }
        afterSegmentsSync(isRetry, shouldRetry);
    }

    /**
     * Clears the stale files from the latest local segment checksum map.
     *
     * @param localSegmentsPostRefresh list of segment files present post refresh
     */
    private void clearStaleFilesFromLocalSegmentChecksumMap(Collection<String> localSegmentsPostRefresh) {
        localSegmentChecksumMap.keySet()
            .stream()
            .filter(file -> !localSegmentsPostRefresh.contains(file))
            .collect(Collectors.toSet())
            .forEach(localSegmentChecksumMap::remove);
    }

    private void beforeSegmentsSync(boolean isRetry) {
        if (isRetry) {
            logger.info("Retrying to sync the segments to remote store");
        }
    }

    private void onSuccessfulSegmentsSync() {
        // Reset the backoffDelayIterator for the future failures
        resetBackOffDelayIterator();
        // Cancel the scheduled cancellable retry if possible and set it to null
        cancelAndResetScheduledCancellableRetry();
    }

    /**
     * Cancels the scheduled retry if there is one scheduled, and it has not started yet. Clears the reference as the
     * schedule retry has been cancelled, or it was null in the first place, or it is running/ran already.
     */
    private void cancelAndResetScheduledCancellableRetry() {
        if (scheduledCancellableRetry != null && scheduledCancellableRetry.getDelay(TimeUnit.NANOSECONDS) > 0) {
            scheduledCancellableRetry.cancel();
        }
        scheduledCancellableRetry = null;
    }

    /**
     * Resets the backoff delay iterator so that the next set of failures starts with the base delay and goes upto max delay.
     */
    private void resetBackOffDelayIterator() {
        backoffDelayIterator = EXPONENTIAL_BACKOFF_POLICY.iterator();
    }

    private void afterSegmentsSync(boolean isRetry, boolean shouldRetry) {
        // If this was a retry attempt, then we release the semaphore at the end so that further retries can be scheduled
        if (isRetry) {
            SCHEDULE_RETRY_PERMITS.release();
        }

        // If there are failures in uploading segments, then we should retry as search idle can lead to
        // refresh not occurring until write happens.
        if (shouldRetry && indexShard.state() != IndexShardState.CLOSED && SCHEDULE_RETRY_PERMITS.tryAcquire()) {
            scheduledCancellableRetry = indexShard.getThreadPool()
                .schedule(() -> this.syncSegments(true), backoffDelayIterator.next(), ThreadPool.Names.REMOTE_REFRESH);
        }
    }

    private boolean isRefreshAfterCommit() throws IOException {
        String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(storeDirectory);
        return (lastCommittedLocalSegmentFileName != null
            && !remoteDirectory.containsFile(lastCommittedLocalSegmentFileName, getChecksumOfLocalFile(lastCommittedLocalSegmentFileName)));
    }

    String uploadSegmentInfosSnapshot(String latestSegmentsNFilename, SegmentInfos segmentInfosSnapshot) throws IOException {
        final long maxSeqNoFromSegmentInfos = indexShard.getEngine().getMaxSeqNoFromSegmentInfos(segmentInfosSnapshot);

        Map<String, String> userData = segmentInfosSnapshot.getUserData();
        userData.put(LOCAL_CHECKPOINT_KEY, String.valueOf(maxSeqNoFromSegmentInfos));
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNoFromSegmentInfos));
        segmentInfosSnapshot.setUserData(userData, false);

        long commitGeneration = SegmentInfos.generationFromSegmentsFileName(latestSegmentsNFilename);
        String segmentInfoSnapshotFilename = SEGMENT_INFO_SNAPSHOT_FILENAME_PREFIX + "__" + commitGeneration;
        try (IndexOutput indexOutput = storeDirectory.createOutput(segmentInfoSnapshotFilename, IOContext.DEFAULT)) {
            segmentInfosSnapshot.write(indexOutput);
        }
        storeDirectory.sync(Collections.singleton(segmentInfoSnapshotFilename));
        remoteDirectory.copyFrom(storeDirectory, segmentInfoSnapshotFilename, segmentInfoSnapshotFilename, IOContext.DEFAULT, true);
        return segmentInfoSnapshotFilename;
    }

    // Visible for testing
    boolean uploadNewSegments(Collection<String> localSegmentsPostRefresh, Map<String, Long> sizeMap) throws IOException {
        AtomicBoolean uploadSuccess = new AtomicBoolean(true);
        // Exclude files that are already uploaded and the exclude files to come up with the list of files to be uploaded.
        List<String> filesToUpload = localSegmentsPostRefresh.stream().filter(file -> !EXCLUDE_FILES.contains(file)).filter(file -> {
            try {
                return !remoteDirectory.containsFile(file, getChecksumOfLocalFile(file));
            } catch (IOException e) {
                logger.info(
                    "Exception while reading checksum of local segment file: {}, ignoring the exception and re-uploading the file",
                    file
                );
                return true;
            }
        }).collect(Collectors.toList());

        // Start tracking the upload bytes started
        filesToUpload.forEach(file -> segmentTracker.addUploadBytesStarted(sizeMap.get(file)));

        // Starting the uploads now
        filesToUpload.forEach(file -> {
            boolean success = false;
            long fileSize = sizeMap.get(file);
            try {
                // Start upload to remote store
                remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
                // Upload succeeded
                segmentTracker.addUploadBytesSucceeded(fileSize);
                segmentTracker.addToLatestUploadedFiles(file);
                success = true;
            } catch (IOException e) {
                // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", file), e);
            } finally {
                if (success == false) {
                    uploadSuccess.set(false);
                    // Upload failed
                    segmentTracker.addUploadBytesFailed(fileSize);
                }
            }
        });
        return uploadSuccess.get();
    }

    private String getChecksumOfLocalFile(String file) throws IOException {
        if (!localSegmentChecksumMap.containsKey(file)) {
            try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.DEFAULT)) {
                String checksum = Long.toString(CodecUtil.retrieveChecksum(indexInput));
                localSegmentChecksumMap.put(file, checksum);
            }
        }
        return localSegmentChecksumMap.get(file);
    }

    private void deleteStaleCommits() {
        try {
            remoteDirectory.deleteStaleSegments(LAST_N_METADATA_FILES_TO_KEEP);
        } catch (IOException e) {
            logger.info("Exception while deleting stale commits from remote segment store, will retry delete post next commit", e);
        }
    }

    /**
     * Updates the last refresh time and refresh seq no which is seen by local store.
     */
    private void updateLocalRefreshTimeAndSeqNo() {
        segmentTracker.updateLocalRefreshTimeMs(pendingRefreshTimeMs);
        segmentTracker.updateLocalRefreshSeqNo(segmentTracker.getLocalRefreshSeqNo() + 1);
    }

    /**
     * Updates the last refresh time and refresh seq no which is seen by remote store.
     */
    private void updateRemoteRefreshTimeAndSeqNo(long refreshTimeMs, long refreshSeqNo) {
        segmentTracker.updateRemoteRefreshTimeMs(refreshTimeMs);
        segmentTracker.updateRemoteRefreshSeqNo(refreshSeqNo);
    }

    /**
     * Returns map of file name to size of the input segment files. Tries to reuse existing information by caching the size
     * data, otherwise uses {@code storeDirectory.fileLength(file)} to get the size. This method also removes from the map
     * such files that are not present in the list of segment files given in the input.
     *
     * @param segmentFiles list of segment files for which size needs to be known
     * @return the map as mentioned above
     */
    private Map<String, Long> createSizeMap(Collection<String> segmentFiles) {
        // Create a map of file name to size
        Map<String, Long> sizeMap = segmentFiles.stream()
            .filter(file -> !EXCLUDE_FILES.contains(file))
            .collect(Collectors.toMap(Function.identity(), file -> {
                if (latestFileNameSizeOnLocalMap.containsKey(file)) {
                    return latestFileNameSizeOnLocalMap.get(file);
                }
                long fileSize = 0;
                try {
                    fileSize = storeDirectory.fileLength(file);
                    latestFileNameSizeOnLocalMap.put(file, fileSize);
                } catch (IOException e) {
                    logger.warn(new ParameterizedMessage("Exception while reading the fileLength of file={}", file), e);
                }
                return fileSize;
            }));
        // Remove keys from the fileSizeMap that do not exist in the latest segment files
        latestFileNameSizeOnLocalMap.entrySet().removeIf(entry -> sizeMap.containsKey(entry.getKey()) == false);
        return sizeMap;
    }

    private void updateFinalUploadStatusInSegmentTracker(boolean uploadStatus, long bytesBeforeUpload, long startTimeInNS) {
        if (uploadStatus) {
            long bytesUploaded = segmentTracker.getUploadBytesSucceeded() - bytesBeforeUpload;
            long timeTakenInMS = (System.nanoTime() - startTimeInNS) / 1_000_000L;

            segmentTracker.incrementTotalUploadsSucceeded();
            segmentTracker.addUploadBytes(bytesUploaded);
            segmentTracker.addUploadBytesPerSec((bytesUploaded * 1_000L) / timeTakenInMS);
            segmentTracker.addUploadTimeMs(timeTakenInMS);
        } else {
            segmentTracker.incrementTotalUploadsFailed();
        }
    }
}
