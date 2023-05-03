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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public final class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public static final int REMOTE_REFRESH_RETRY_INTERVAL_DEFAULT_VALUE = 1;

    private static final int MAX_CONCURRENT_SCHEDULED_REMOTE_REFRESH_RETRIES = 1;

    /**
     * If the remote refresh segment sync fails, we retry to sync the segments scheduling it after a retry interval
     * which is controlled by the below setting.
     */
    public static final Setting<Integer> INDEX_REMOTE_REFRESH_RETRY_INTERVAL = Setting.intSetting(
        "index.remote_store.segment_sync.retry_interval",
        REMOTE_REFRESH_RETRY_INTERVAL_DEFAULT_VALUE,
        REMOTE_REFRESH_RETRY_INTERVAL_DEFAULT_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    // Visible for testing
    static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    // Visible for testing
    static final int LAST_N_METADATA_FILES_TO_KEEP = 10;
    static final String SEGMENT_INFO_SNAPSHOT_FILENAME_PREFIX = "segment_infos_snapshot_filename";

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final Map<String, String> localSegmentChecksumMap;
    private long primaryTerm;

    /**
     * Semaphore that ensures there is only 1 retry scheduled at any time.
     */
    private final Semaphore retrySemaphore = new Semaphore(MAX_CONCURRENT_SCHEDULED_REMOTE_REFRESH_RETRIES);

    private volatile int remoteRefreshRetryInterval;

    public RemoteStoreRefreshListener(IndexShard indexShard) {
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        this.primaryTerm = indexShard.getOperationPrimaryTerm();
        localSegmentChecksumMap = new HashMap<>();
        if (indexShard.shardRouting.primary()) {
            try {
                this.remoteDirectory.init();
            } catch (IOException e) {
                logger.error("Exception while initialising RemoteSegmentStoreDirectory", e);
            }
        }

        // Retry remote refresh on failure - This interval is the time delay after which the sync of segments to remote store
        // would be retried. This has been kept as an index scoped setting inline with index.refresh_interval setting.
        remoteRefreshRetryInterval = INDEX_REMOTE_REFRESH_RETRY_INTERVAL.get(indexShard.indexSettings().getSettings());
        indexShard.indexSettings()
            .getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_REMOTE_REFRESH_RETRY_INTERVAL, this::setRemoteRefreshRetryInterval);
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Do Nothing
    }

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * This method also uploads remote_segments_metadata file which contains metadata of each segment file uploaded.
     *
     * @param didRefresh true if the refresh opened a new reference
     */
    @Override
    public void afterRefresh(boolean didRefresh) {
        try {
            indexShard.getThreadPool().executor(ThreadPool.Names.REMOTE_REFRESH).submit(() -> syncSegments(false)).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.info("Exception occurred while scheduling syncSegments", e);
        }
    }

    private synchronized void syncSegments(boolean retry) {
        boolean shouldRetry = false;
        beforeSegmentsSync(retry);
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
                    try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                        SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();

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

                            boolean uploadStatus = uploadNewSegments(localSegmentsPostRefresh);
                            if (uploadStatus) {
                                segmentInfoSnapshotFilename = uploadSegmentInfosSnapshot(latestSegmentInfos.get(), segmentInfos);
                                localSegmentsPostRefresh.add(segmentInfoSnapshotFilename);

                                remoteDirectory.uploadMetadata(
                                    localSegmentsPostRefresh,
                                    storeDirectory,
                                    indexShard.getOperationPrimaryTerm(),
                                    segmentInfos.getGeneration()
                                );
                                localSegmentChecksumMap.keySet()
                                    .stream()
                                    .filter(file -> !localSegmentsPostRefresh.contains(file))
                                    .collect(Collectors.toSet())
                                    .forEach(localSegmentChecksumMap::remove);
                                final long lastRefreshedCheckpoint = ((InternalEngine) indexShard.getEngine()).lastRefreshedCheckpoint();
                                indexShard.getEngine().translogManager().setMinSeqNoToKeep(lastRefreshedCheckpoint + 1);
                            } else {
                                shouldRetry = true;
                            }
                        }
                    } catch (EngineException e) {
                        shouldRetry = true;
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
                    shouldRetry = true;
                    // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                    // in the next refresh. This should not affect durability of the indexed data after remote trans-log integration.
                    logger.warn("Exception while uploading new segments to the remote segment store", e);
                }
            }
        } catch (Throwable t) {
            logger.error("Exception in RemoteStoreRefreshListener.afterRefresh()", t);
        }
        afterSegmentsSync(retry, shouldRetry);
    }

    private void beforeSegmentsSync(boolean isRetry) {
        if (isRetry) {
            logger.info("Retrying to sync the segments to remote store");
        }
    }

    private void afterSegmentsSync(boolean isRetry, boolean shouldRetry) {
        // If this was a retry attempt, then we release the semaphore at the end so that further retries can be scheduled
        if (isRetry) {
            retrySemaphore.release();
        }

        // If there are failures in uploading segments, then we should retry as search idle can lead to
        // refresh not occurring until write happens.
        if (shouldRetry && indexShard.state() != IndexShardState.CLOSED && retrySemaphore.tryAcquire()) {
            indexShard.getThreadPool()
                .schedule(
                    () -> this.syncSegments(true),
                    TimeValue.timeValueSeconds(remoteRefreshRetryInterval),
                    ThreadPool.Names.REMOTE_REFRESH
                );
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
    boolean uploadNewSegments(Collection<String> localFiles) throws IOException {
        AtomicBoolean uploadSuccess = new AtomicBoolean(true);
        localFiles.stream().filter(file -> !EXCLUDE_FILES.contains(file)).filter(file -> {
            try {
                return !remoteDirectory.containsFile(file, getChecksumOfLocalFile(file));
            } catch (IOException e) {
                logger.info(
                    "Exception while reading checksum of local segment file: {}, ignoring the exception and re-uploading the file",
                    file
                );
                return true;
            }
        }).forEach(file -> {
            try {
                remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
            } catch (IOException e) {
                uploadSuccess.set(false);
                // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", file), e);
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

    public void setRemoteRefreshRetryInterval(int remoteRefreshRetryInterval) {
        this.remoteRefreshRetryInterval = remoteRefreshRetryInterval;
    }
}
