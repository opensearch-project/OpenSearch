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
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.RemoteSegmentUploadShardStatsTracker;
import org.opensearch.index.RemoteUploadPressureService;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
    // Visible for testing
    static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    // Visible for testing
    static final int LAST_N_METADATA_FILES_TO_KEEP = 10;
    static final String SEGMENT_INFO_SNAPSHOT_FILENAME_PREFIX = "segment_infos_snapshot_filename";

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final Map<String, String> localSegmentChecksumMap;
    private final RemoteUploadPressureService remoteUploadPressureService;
    private long primaryTerm;

    // Stats related variables
    private long pendingRefreshTime;
    private final AtomicLong refreshTime = new AtomicLong(System.nanoTime());
    private final AtomicLong refreshSeqNo = new AtomicLong();
    private final Map<String, Long> fileSizeMap = new HashMap<>();

    public RemoteStoreRefreshListener(IndexShard indexShard) {
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        this.primaryTerm = indexShard.getOperationPrimaryTerm();
        localSegmentChecksumMap = new HashMap<>();
        this.remoteUploadPressureService = indexShard.getRemoteUploadPressureService();
        if (indexShard.shardRouting.primary()) {
            try {
                this.remoteDirectory.init();
            } catch (IOException e) {
                logger.error("Exception while initialising RemoteSegmentStoreDirectory", e);
            }
        }
    }

    @Override
    public void beforeRefresh() throws IOException {
        pendingRefreshTime = System.nanoTime();
    }

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * This method also uploads remote_segments_metadata file which contains metadata of each segment file uploaded.
     *
     * @param didRefresh true if the refresh opened a new reference
     */
    @Override
    public void afterRefresh(boolean didRefresh) {
        RemoteSegmentUploadShardStatsTracker statsTracker = remoteUploadPressureService.getStatsTracker(indexShard.shardId());
        long latestRefreshTime, latestRefreshSeqNo;
        if (didRefresh) {
            latestRefreshTime = updateRefreshTime();
            latestRefreshSeqNo = updateRefreshSeqNo();
            updateRefreshStats(statsTracker, false, latestRefreshTime, latestRefreshSeqNo);
        } else {
            latestRefreshTime = refreshTime.get();
            latestRefreshSeqNo = refreshSeqNo.get();
        }
        try {
            if (indexShard.getReplicationTracker().isPrimaryMode()) {
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
                    UploadStatus segmentsUploadStatus = UploadStatus.NOT_STARTED, metadataUploadStatus = UploadStatus.NOT_STARTED;
                    long bytesBeforeUpload = statsTracker.getUploadBytesSucceeded(), startTimeInNS = System.nanoTime();
                    try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                        SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();

                        HashSet<String> localSegmentsPostRefresh = new HashSet<>(segmentInfos.files(true));

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

                            // Create a map of file name to size and update the stats tracker
                            Map<String, Long> sizeMap = createSizeMap(localSegmentsPostRefresh);
                            statsTracker.updateLatestLocalFileNameLengthMap(sizeMap);

                            // Start tracking total uploads started
                            statsTracker.incrementTotalUploadsStarted();

                            // Start the segments upload
                            segmentsUploadStatus = UploadStatus.STARTED;
                            segmentsUploadStatus = uploadNewSegments(localSegmentsPostRefresh, statsTracker, sizeMap);
                            if (UploadStatus.SUCCEEDED == segmentsUploadStatus) {
                                segmentInfoSnapshotFilename = uploadSegmentInfosSnapshot(latestSegmentInfos.get(), segmentInfos);
                                localSegmentsPostRefresh.add(segmentInfoSnapshotFilename);
                                // Start Metadata upload
                                metadataUploadStatus = UploadStatus.STARTED;
                                remoteDirectory.uploadMetadata(
                                    localSegmentsPostRefresh,
                                    storeDirectory,
                                    indexShard.getOperationPrimaryTerm(),
                                    segmentInfos.getGeneration()
                                );
                                // Metadata upload succeeded
                                metadataUploadStatus = UploadStatus.SUCCEEDED;
                                statsTracker.updateLatestUploadFiles(sizeMap.keySet());
                                updateRefreshStats(statsTracker, true, latestRefreshTime, latestRefreshSeqNo);

                                localSegmentChecksumMap.keySet()
                                    .stream()
                                    .filter(file -> !localSegmentsPostRefresh.contains(file))
                                    .collect(Collectors.toSet())
                                    .forEach(localSegmentChecksumMap::remove);

                                InternalEngine internalEngine = (InternalEngine) indexShard.getEngine();
                                internalEngine.translogManager().setMinSeqNoToKeep(internalEngine.lastRefreshedCheckpoint() + 1);
                            }
                        }
                    } catch (EngineException e) {
                        logger.warn("Exception while reading SegmentInfosSnapshot", e);
                    } finally {
                        // Update the stats tracker with the final upload status as seen at the end
                        updateTotalUploadTerminalStats(
                            metadataUploadStatus,
                            segmentsUploadStatus,
                            statsTracker,
                            bytesBeforeUpload,
                            startTimeInNS
                        );
                        // Deletes the segment info file created for the upload of segment metadata.
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
            }
        } catch (Throwable t) {
            logger.error("Exception in RemoteStoreRefreshListener.afterRefresh()", t);
        }
    }

    private void updateTotalUploadTerminalStats(
        UploadStatus metadataUploadStatus,
        UploadStatus segmentsUploadStatus,
        RemoteSegmentUploadShardStatsTracker statsTracker,
        long bytesBeforeUpload,
        long startTimeInNS
    ) {
        // If the metadata upload status is succeeded, then increment upload success count. If the metadata upload status is not succeeded,
        // then there are 3 cases - 1. metadata upload was skipped as all segments and metadata file are already present in remote store -
        // in which case the upload was skipped 2. segments upload status is started or ahead in which case the upload failed 3. segments
        // upload did not start at all - in which case the upload never started.
        if (metadataUploadStatus == UploadStatus.SUCCEEDED) {
            statsTracker.incrementTotalUploadsSucceeded();
        } else if (Set.of(UploadStatus.STARTED, UploadStatus.SUCCEEDED, UploadStatus.FAILED).contains(segmentsUploadStatus)) {
            statsTracker.incrementTotalUploadsFailed();
        }

        long bytesUploaded = statsTracker.getUploadBytesSucceeded() - bytesBeforeUpload;
        long timeTakenInNS = System.nanoTime() - startTimeInNS;
        if (bytesUploaded != 0) {
            statsTracker.addUploadBytes(bytesUploaded);
            statsTracker.addUploadBytesPerSecond((bytesUploaded * 10 ^ 9L) / timeTakenInNS);
        }
    }

    private boolean shouldUploadMetadata(UploadStatus segmentsUploadStatus, Set<String> localSegments, Set<String> remoteSegments) {
        return UploadStatus.SUCCEEDED == segmentsUploadStatus && !localSegments.equals(remoteSegments);
    }

    private void updateRefreshStats(
        RemoteSegmentUploadShardStatsTracker statsTracker,
        boolean remote,
        long refreshTime,
        long refreshSeqNo
    ) {
        if (remote) {
            statsTracker.updateRemoteRefreshTime(refreshTime);
            statsTracker.updateRemoteRefreshSeqNo(refreshSeqNo);
        } else {
            statsTracker.updateLocalRefreshTime(refreshTime);
            statsTracker.updateLocalRefreshSeqNo(refreshSeqNo);
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
    UploadStatus uploadNewSegments(
        Collection<String> localSegmentsPostRefresh,
        RemoteSegmentUploadShardStatsTracker statsTracker,
        Map<String, Long> sizeMap
    ) throws IOException {
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
        filesToUpload.forEach(file -> statsTracker.incrementUploadBytesStarted(sizeMap.get(file)));

        // Starting the uploads now
        filesToUpload.forEach(file -> {
            boolean success = false;
            long fileSize = sizeMap.get(file);
            try {
                // Start upload to remote store
                remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
                // Upload succeeded
                statsTracker.incrementUploadBytesSucceeded(fileSize);
                success = true;
            } catch (IOException e) {
                // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", file), e);
            } finally {
                if (success == false) {
                    uploadSuccess.set(false);
                    // Upload failed
                    statsTracker.incrementUploadBytesFailed(fileSize);
                }
            }
        });
        return uploadSuccess.get() ? UploadStatus.SUCCEEDED : UploadStatus.FAILED;
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

    private long updateRefreshTime() {
        return refreshTime.updateAndGet(curr -> Math.max(curr, pendingRefreshTime));
    }

    private long updateRefreshSeqNo() {
        return refreshSeqNo.incrementAndGet();
    }

    private Map<String, Long> createSizeMap(Collection<String> segmentFiles) {
        // Create a map of file name to size
        Map<String, Long> sizeMap = segmentFiles.stream()
            .filter(file -> !EXCLUDE_FILES.contains(file))
            .collect(Collectors.toMap(Function.identity(), file -> {
                if (fileSizeMap.containsKey(file)) {
                    return fileSizeMap.get(file);
                }
                long fileSize = 0;
                try {
                    fileSize = storeDirectory.fileLength(file);
                    fileSizeMap.put(file, fileSize);
                } catch (IOException e) {
                    logger.warn(new ParameterizedMessage("Exception while reading the fileLength of file={}", file), e);
                }
                return fileSize;
            }));
        // Remove keys from the fileSizeMap that do not exist in the latest segment files
        fileSizeMap.entrySet().removeIf(entry -> sizeMap.containsKey(entry.getKey()) == false);
        return sizeMap;
    }

    /**
     * Used to track the status of upload of segments file and metadata files.
     */
    private enum UploadStatus {
        NOT_STARTED,
        STARTED,
        FAILED,
        SUCCEEDED
    }
}
