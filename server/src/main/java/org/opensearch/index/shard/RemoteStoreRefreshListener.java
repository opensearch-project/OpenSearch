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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.CompositeDirectory;
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
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final RemoteSegmentTransferTracker segmentTracker;
    private final Map<String, String> localSegmentChecksumMap;
    private volatile long primaryTerm;
    private volatile Iterator<TimeValue> backoffDelayIterator;
    private final SegmentReplicationCheckpointPublisher checkpointPublisher;
    private final RemoteStoreSettings remoteStoreSettings;
    private final RemoteUploaderService remoteUploaderService;

    public RemoteStoreRefreshListener(
        IndexShard indexShard,
        SegmentReplicationCheckpointPublisher checkpointPublisher,
        RemoteSegmentTransferTracker segmentTracker,
        RemoteStoreSettings remoteStoreSettings,
        RemoteUploaderService remoteUploaderService
    ) {
        super(indexShard.getThreadPool());
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        this.remoteUploaderService = remoteUploaderService;
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
        this.primaryTerm = remoteSegmentMetadata != null ? remoteSegmentMetadata.getPrimaryTerm() :
            INVALID_PRIMARY_TERM;
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
                try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                    Collection<String> localSegmentsPostRefresh = segmentInfosGatedCloseable.get().files(true);
                    updateLocalSizeMapAndTracker(localSegmentsPostRefresh);
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
            successful = remoteUploaderService.syncSegments();
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
        try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
            return segmentInfosGatedCloseable.get().files(true).stream().allMatch(this::skipUpload);
        } catch (Throwable throwable) {
            logger.error("Throwable thrown during isRemoteSegmentStoreInSync", throwable);
        }
        return false;
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

    //todo: create a common class
    private boolean isRefreshAfterCommit() throws IOException {
        String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(storeDirectory);
        return (lastCommittedLocalSegmentFileName != null
            && !remoteDirectory.containsFile(lastCommittedLocalSegmentFileName, getChecksumOfLocalFile(lastCommittedLocalSegmentFileName)));
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

    //todo
    boolean isLowPriorityUpload() {
        return isLocalOrSnapshotRecoveryOrSeeding();
    }

    /**
     * Whether to upload a file or not depending on whether file is in excluded list or has been already uploaded.
     *
     * @param file that needs to be uploaded.
     * @return true if the upload has to be skipped for the file.
     */
    //todo: create a common class
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
            try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.READONCE)) {
                String checksum = Long.toString(CodecUtil.retrieveChecksum(indexInput));
                localSegmentChecksumMap.put(file, checksum);
            }
        }
        return localSegmentChecksumMap.get(file);
    }

    /**
     * Updates map of file name to size of the input segment files in the segment tracker. Uses {@code storeDirectory.fileLength(file)} to get the size.
     *
     * @param segmentFiles list of segment files that are part of the most recent local refresh.
     *
     * @return updated map of local segment files and filesize
     */
    //todo: create a common class
    private Map<String, Long> updateLocalSizeMapAndTracker(Collection<String> segmentFiles) {
        return segmentTracker.updateLatestLocalFileNameLengthMap(segmentFiles, storeDirectory::fileLength);
    }

    /**
     * This checks for readiness of the index shard and primary mode. This has separated from shouldSync since we use the
     * returned value of this method for scheduling retries in syncSegments method.
     * @return true iff the shard is a started with primary mode true or it is local or snapshot recovery.
     */
    //todo: create a common class
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
            if (indexShard.getEngineOrNull() != null) {
                sb.append(" engineType=").append(indexShard.getEngine().getClass().getSimpleName());
            }
            if (indexShard.recoveryState() != null) {
                sb.append(" recoverySourceType=").append(indexShard.recoveryState().getRecoverySource().getType());
                sb.append(" primary=").append(indexShard.shardRouting.primary());
            }
            logger.info(sb.toString());
        }
        return isReady;
    }

    //todo: create a common class
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
