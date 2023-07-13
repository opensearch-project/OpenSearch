/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.UploadListener;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public final class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

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

    // Visible for testing
    static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    // Visible for testing
    public static final int LAST_N_METADATA_FILES_TO_KEEP = 10;

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final RemoteRefreshSegmentTracker segmentTracker;
    private final Map<String, String> localSegmentChecksumMap;
    private long primaryTerm;

    /**
     * This boolean is used to ensure that there is only 1 retry scheduled/running at any time.
     */
    private final AtomicBoolean retryScheduled = new AtomicBoolean(false);

    private volatile Iterator<TimeValue> backoffDelayIterator;

    private volatile Scheduler.ScheduledCancellable scheduledCancellableRetry;

    /**
     * Keeps track of segment files and their size in bytes which are part of the most recent refresh.
     */
    private final Map<String, Long> latestFileNameSizeOnLocalMap = ConcurrentCollections.newConcurrentMap();

    private final SegmentReplicationCheckpointPublisher checkpointPublisher;

    private final UploadListener statsListener;

    public RemoteStoreRefreshListener(
        IndexShard indexShard,
        SegmentReplicationCheckpointPublisher checkpointPublisher,
        RemoteRefreshSegmentTracker segmentTracker
    ) {
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
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
        // if no metadata is present, this value will be initilized with -1.
        this.primaryTerm = remoteSegmentMetadata != null ? remoteSegmentMetadata.getPrimaryTerm() : INVALID_PRIMARY_TERM;
        this.segmentTracker = segmentTracker;
        resetBackOffDelayIterator();
        this.checkpointPublisher = checkpointPublisher;
        this.statsListener = new UploadListener() {
            @Override
            public void beforeUpload(String file) {
                // Start tracking the upload bytes started
                segmentTracker.addUploadBytesStarted(latestFileNameSizeOnLocalMap.get(file));
            }

            @Override
            public void onSuccess(String file) {
                // Track upload success
                segmentTracker.addUploadBytesSucceeded(latestFileNameSizeOnLocalMap.get(file));
                segmentTracker.addToLatestUploadedFiles(file);
            }

            @Override
            public void onFailure(String file) {
                // Track upload failure
                segmentTracker.addUploadBytesFailed(latestFileNameSizeOnLocalMap.get(file));
            }
        };
    }

    @Override
    public void beforeRefresh() throws IOException {}

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * This method also uploads remote_segments_metadata file which contains metadata of each segment file uploaded.
     *
     * @param didRefresh true if the refresh opened a new reference
     */
    @Override
    public void afterRefresh(boolean didRefresh) {
        if (this.primaryTerm != indexShard.getOperationPrimaryTerm()
            || didRefresh
            || remoteDirectory.getSegmentsUploadedToRemoteStore().isEmpty()) {
            updateLocalRefreshTimeAndSeqNo();
            try {
                indexShard.getThreadPool().executor(ThreadPool.Names.REMOTE_REFRESH).submit(() -> syncSegments(false)).get();
            } catch (InterruptedException | ExecutionException e) {
                logger.info("Exception occurred while scheduling syncSegments", e);
            }
        }
    }

    private synchronized void syncSegments(boolean isRetry) {
        if (indexShard.getReplicationTracker().isPrimaryMode() == false) {
            return;
        }
        ReplicationCheckpoint checkpoint = indexShard.getLatestReplicationCheckpoint();
        indexShard.onCheckpointPublished(checkpoint);
        beforeSegmentsSync(isRetry);
        long refreshTimeMs = segmentTracker.getLocalRefreshTimeMs(), refreshClockTimeMs = segmentTracker.getLocalRefreshClockTimeMs();
        long refreshSeqNo = segmentTracker.getLocalRefreshSeqNo();
        long bytesBeforeUpload = segmentTracker.getUploadBytesSucceeded(), startTimeInNS = System.nanoTime();
        final AtomicBoolean shouldRetry = new AtomicBoolean(true);

        try {
            if (this.primaryTerm != indexShard.getOperationPrimaryTerm()) {
                this.primaryTerm = indexShard.getOperationPrimaryTerm();
                this.remoteDirectory.init();
            }
            try {
                // if a new segments_N file is present in local that is not uploaded to remote store yet, it
                // is considered as a first refresh post commit. A cleanup of stale commit files is triggered.
                // This is done to avoid delete post each refresh.
                if (isRefreshAfterCommit()) {
                    remoteDirectory.deleteStaleSegmentsAsync(LAST_N_METADATA_FILES_TO_KEEP);
                }

                try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                    SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();
                    // Capture replication checkpoint before uploading the segments as upload can take some time and checkpoint can
                    // move.
                    long lastRefreshedCheckpoint = ((InternalEngine) indexShard.getEngine()).lastRefreshedCheckpoint();
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
                        SegmentInfos segmentCommitInfos;
                        try {
                            segmentCommitInfos = SegmentInfos.readCommit(storeDirectory, latestSegmentInfos.get());
                        } catch (Exception e) {
                            // Seeing discrepancy in segment infos and files on disk. SegmentInfosSnapshot is returning
                            // a segment_N file which does not exist on local disk.
                            logger.error("Exception occurred while SegmentInfos.readCommit(..)", e);
                            logger.error("segmentInfosFiles={} diskFiles={}", localSegmentsPostRefresh, storeDirectory.listAll());
                            throw e;
                        }
                        localSegmentsPostRefresh.addAll(segmentCommitInfos.files(true));
                        segmentInfosFiles.stream()
                            .filter(file -> !file.equals(latestSegmentInfos.get()))
                            .forEach(localSegmentsPostRefresh::remove);

                        // Create a map of file name to size and update the refresh segment tracker
                        updateLocalSizeMapAndTracker(localSegmentsPostRefresh);
                        CountDownLatch latch = new CountDownLatch(1);
                        ActionListener<Void> segmentUploadsCompletedListener = new LatchedActionListener<>(new ActionListener<>() {
                            @Override
                            public void onResponse(Void unused) {
                                try {
                                    // Start metadata file upload
                                    uploadMetadata(localSegmentsPostRefresh, segmentInfos);
                                    clearStaleFilesFromLocalSegmentChecksumMap(localSegmentsPostRefresh);
                                    onSuccessfulSegmentsSync(
                                        refreshTimeMs,
                                        refreshClockTimeMs,
                                        refreshSeqNo,
                                        lastRefreshedCheckpoint,
                                        checkpoint
                                    );
                                    // At this point since we have uploaded new segments, segment infos and segment metadata file,
                                    // along with marking minSeqNoToKeep, upload has succeeded completely.
                                    shouldRetry.set(false);
                                } catch (Exception e) {
                                    // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                                    // in the next refresh. This should not affect durability of the indexed data after remote trans-log
                                    // integration.
                                    logger.warn("Exception in post new segment upload actions", e);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn("Exception while uploading new segments to the remote segment store", e);
                            }
                        }, latch);

                        // Start the segments files upload
                        uploadNewSegments(localSegmentsPostRefresh, segmentUploadsCompletedListener);
                        latch.await();
                    }
                } catch (EngineException e) {
                    logger.warn("Exception while reading SegmentInfosSnapshot", e);
                }
            } catch (IOException e) {
                // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                // in the next refresh. This should not affect durability of the indexed data after remote trans-log integration.
                logger.warn("Exception while uploading new segments to the remote segment store", e);
            }
        } catch (Throwable t) {
            logger.error("Exception in RemoteStoreRefreshListener.afterRefresh()", t);
        }
        updateFinalStatusInSegmentTracker(shouldRetry.get() == false, bytesBeforeUpload, startTimeInNS);
        afterSegmentsSync(isRetry, shouldRetry.get());
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
        // Start tracking total uploads started
        segmentTracker.incrementTotalUploadsStarted();
    }

    private void onSuccessfulSegmentsSync(
        long refreshTimeMs,
        long refreshClockTimeMs,
        long refreshSeqNo,
        long lastRefreshedCheckpoint,
        ReplicationCheckpoint checkpoint
    ) {
        // Update latest uploaded segment files name in segment tracker
        segmentTracker.setLatestUploadedFiles(latestFileNameSizeOnLocalMap.keySet());
        // Update the remote refresh time and refresh seq no
        updateRemoteRefreshTimeAndSeqNo(refreshTimeMs, refreshClockTimeMs, refreshSeqNo);
        // Reset the backoffDelayIterator for the future failures
        resetBackOffDelayIterator();
        // Cancel the scheduled cancellable retry if possible and set it to null
        cancelAndResetScheduledCancellableRetry();
        // Set the minimum sequence number for keeping translog
        indexShard.getEngine().translogManager().setMinSeqNoToKeep(lastRefreshedCheckpoint + 1);
        // Publishing the new checkpoint which is used for remote store + segrep indexes
        checkpointPublisher.publish(indexShard, checkpoint);
    }

    /**
     * Cancels the scheduled retry if there is one scheduled, and it has not started yet. Clears the reference as the
     * schedule retry has been cancelled, or it was null in the first place, or it is running/ran already.
     */
    private void cancelAndResetScheduledCancellableRetry() {
        if (scheduledCancellableRetry != null && scheduledCancellableRetry.getDelay(TimeUnit.NANOSECONDS) > 0) {
            scheduledCancellableRetry.cancel();
            // Since we are cancelling the retry attempt as an internal/external refresh happened already before the retry job could be
            // started and the current run successfully uploaded the segments.
            retryScheduled.set(false);
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
        // If this was a retry attempt, then we set the retryScheduled to false so that the next retry (if needed) can be scheduled
        if (isRetry) {
            retryScheduled.set(false);
        }

        // If there are failures in uploading segments, then we should retry as search idle can lead to
        // refresh not occurring until write happens.
        if (shouldRetry && indexShard.state() != IndexShardState.CLOSED && retryScheduled.compareAndSet(false, true)) {
            scheduledCancellableRetry = indexShard.getThreadPool()
                .schedule(() -> this.syncSegments(true), backoffDelayIterator.next(), ThreadPool.Names.REMOTE_REFRESH);
        }
    }

    private boolean isRefreshAfterCommit() throws IOException {
        String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(storeDirectory);
        return (lastCommittedLocalSegmentFileName != null
            && !remoteDirectory.containsFile(lastCommittedLocalSegmentFileName, getChecksumOfLocalFile(lastCommittedLocalSegmentFileName)));
    }

    void uploadMetadata(Collection<String> localSegmentsPostRefresh, SegmentInfos segmentInfos) throws IOException {
        final long maxSeqNo = ((InternalEngine) indexShard.getEngine()).currentOngoingRefreshCheckpoint();
        SegmentInfos segmentInfosSnapshot = segmentInfos.clone();
        Map<String, String> userData = segmentInfosSnapshot.getUserData();
        userData.put(LOCAL_CHECKPOINT_KEY, String.valueOf(maxSeqNo));
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        segmentInfosSnapshot.setUserData(userData, false);

        Translog.TranslogGeneration translogGeneration = indexShard.getEngine().translogManager().getTranslogGeneration();
        if (translogGeneration == null) {
            throw new UnsupportedOperationException("Encountered null TranslogGeneration while uploading metadata to remote segment store");
        } else {
            long translogFileGeneration = translogGeneration.translogFileGeneration;
            remoteDirectory.uploadMetadata(
                localSegmentsPostRefresh,
                segmentInfosSnapshot,
                storeDirectory,
                indexShard.getOperationPrimaryTerm(),
                translogFileGeneration
            );
        }
    }

    private void uploadNewSegments(Collection<String> localSegmentsPostRefresh, ActionListener<Void> listener) {
        Collection<String> filteredFiles = localSegmentsPostRefresh.stream().filter(file -> !skipUpload(file)).collect(Collectors.toList());
        if (filteredFiles.size() == 0) {
            listener.onResponse(null);
            return;
        }

        ActionListener<Collection<Void>> mappedListener = ActionListener.map(listener, resp -> null);
        GroupedActionListener<Void> batchUploadListener = new GroupedActionListener<>(mappedListener, filteredFiles.size());

        for (String src : filteredFiles) {
            ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
                statsListener.onSuccess(src);
                batchUploadListener.onResponse(resp);
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);
                if (ex instanceof CorruptIndexException) {
                    indexShard.failShard(ex.getMessage(), ex);
                }
                statsListener.onFailure(src);
                batchUploadListener.onFailure(ex);
            });
            statsListener.beforeUpload(src);
            remoteDirectory.copyFrom(storeDirectory, src, IOContext.DEFAULT, aggregatedListener);
        }
    }

    /**
     * Whether to upload a file or not depending on whether file is in excluded list or has been already uploaded.
     *
     * @param file that needs to be uploaded.
     * @return true if the upload has to be skipped for the file.
     */
    private boolean skipUpload(String file) {
        try {
            // Exclude files that are already uploaded and the exclude files to come up with the list of files to be uploaded.
            return EXCLUDE_FILES.contains(file) || remoteDirectory.containsFile(file, getChecksumOfLocalFile(file));
        } catch (IOException e) {
            logger.error(
                "Exception while reading checksum of local segment file: {}, ignoring the exception and re-uploading the file",
                file
            );
        }
        return false;
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

    /**
     * Updates the last refresh time and refresh seq no which is seen by local store.
     */
    private void updateLocalRefreshTimeAndSeqNo() {
        segmentTracker.updateLocalRefreshClockTimeMs(System.currentTimeMillis());
        segmentTracker.updateLocalRefreshTimeMs(System.nanoTime() / 1_000_000L);
        segmentTracker.updateLocalRefreshSeqNo(segmentTracker.getLocalRefreshSeqNo() + 1);
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
     * Updates map of file name to size of the input segment files. Tries to reuse existing information by caching the size
     * data, otherwise uses {@code storeDirectory.fileLength(file)} to get the size. This method also removes from the map
     * such files that are not present in the list of segment files given in the input.
     *
     * @param segmentFiles list of segment files for which size needs to be known
     */
    private void updateLocalSizeMapAndTracker(Collection<String> segmentFiles) {

        // Update the map
        segmentFiles.stream()
            .filter(file -> !EXCLUDE_FILES.contains(file))
            .filter(file -> !latestFileNameSizeOnLocalMap.containsKey(file) || latestFileNameSizeOnLocalMap.get(file) == 0)
            .forEach(file -> {
                long fileSize = 0;
                try {
                    fileSize = storeDirectory.fileLength(file);
                } catch (IOException e) {
                    logger.warn(new ParameterizedMessage("Exception while reading the fileLength of file={}", file), e);
                }
                latestFileNameSizeOnLocalMap.put(file, fileSize);
            });

        Set<String> fileSet = new HashSet<>(segmentFiles);
        // Remove keys from the fileSizeMap that do not exist in the latest segment files
        latestFileNameSizeOnLocalMap.entrySet().removeIf(entry -> fileSet.contains(entry.getKey()) == false);
        // Update the tracker
        segmentTracker.setLatestLocalFileNameLengthMap(latestFileNameSizeOnLocalMap);
    }

    private void updateFinalStatusInSegmentTracker(boolean uploadStatus, long bytesBeforeUpload, long startTimeInNS) {
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
