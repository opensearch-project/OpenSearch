/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import static org.opensearch.index.seqno.SequenceNumbers.MAX_SEQ_NO;

/**
 * This is an {@link Engine} implementation intended for replica shards when Segment Replication
 * is enabled.  This Engine does not create an IndexWriter, rather it refreshes a {@link NRTReplicationReaderManager}
 * with new Segments when received from an external source.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NRTReplicationEngine extends Engine {

    private volatile SegmentInfos lastCommittedSegmentInfos;
    private final NRTReplicationReaderManager readerManager;
    private final CompletionStatsCache completionStatsCache;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final WriteOnlyTranslogManager translogManager;
    private final Lock flushLock = new ReentrantLock();
    protected final ReplicaFileTracker replicaFileTracker;

    private volatile long lastReceivedPrimaryGen = SequenceNumbers.NO_OPS_PERFORMED;

    private static final int SI_COUNTER_INCREMENT = 10;

    public NRTReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);
        store.incRef();
        NRTReplicationReaderManager readerManager = null;
        WriteOnlyTranslogManager translogManagerRef = null;
        boolean success = false;
        try {
            this.replicaFileTracker = new ReplicaFileTracker(store::deleteQuiet);
            this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            // always protect latest commit on disk.
            replicaFileTracker.incRef(this.lastCommittedSegmentInfos.files(true));
            // cleanup anything not referenced by the latest infos.
            cleanUnreferencedFiles();
            readerManager = buildReaderManager();
            final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                this.lastCommittedSegmentInfos.getUserData().entrySet()
            );
            this.localCheckpointTracker = new LocalCheckpointTracker(commitInfo.maxSeqNo, commitInfo.localCheckpoint);
            this.completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
            this.readerManager = readerManager;
            this.readerManager.addListener(completionStatsCache);
            // NRT Replicas do not have a concept of Internal vs External reader managers.
            // We also do not want to wire up refresh listeners for waitFor & pending refresh location.
            // which are the current external listeners set from IndexShard.
            // Only wire up the internal listeners.
            for (ReferenceManager.RefreshListener listener : engineConfig.getInternalRefreshListener()) {
                this.readerManager.addListener(listener);
            }
            final Map<String, String> userData = this.lastCommittedSegmentInfos.getUserData();
            final String translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));
            translogManagerRef = new WriteOnlyTranslogManager(
                engineConfig.getTranslogConfig(),
                engineConfig.getPrimaryTermSupplier(),
                engineConfig.getGlobalCheckpointSupplier(),
                getTranslogDeletionPolicy(engineConfig),
                shardId,
                readLock,
                this::getLocalCheckpointTracker,
                translogUUID,
                new TranslogEventListener() {
                    @Override
                    public void onFailure(String reason, Exception ex) {
                        failEngine(reason, ex);
                    }

                    @Override
                    public void onAfterTranslogSync() {
                        try {
                            translogManager.trimUnreferencedReaders();
                        } catch (IOException ex) {
                            throw new TranslogException(shardId, "failed to trim unreferenced translog readers", ex);
                        }
                    }
                },
                this,
                engineConfig.getTranslogFactory(),
                engineConfig.getPrimaryModeSupplier()
            );
            this.translogManager = translogManagerRef;
            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(readerManager, translogManagerRef);
                if (isClosed.get() == false) {
                    // failure, we need to dec the store reference
                    store.decRef();
                }
            }
        }
    }

    public void cleanUnreferencedFiles() throws IOException {
        replicaFileTracker.deleteUnreferencedFiles(store.directory().listAll());
    }

    private NRTReplicationReaderManager buildReaderManager() throws IOException {
        return new NRTReplicationReaderManager(
            OpenSearchDirectoryReader.wrap(getDirectoryReader(), shardId),
            replicaFileTracker::incRef,
            replicaFileTracker::decRef
        );
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    public synchronized void updateSegments(final SegmentInfos infos) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            // Update the current infos reference on the Engine's reader.
            ensureOpen();
            final long maxSeqNo = Long.parseLong(infos.userData.get(MAX_SEQ_NO));
            final long incomingGeneration = infos.getGeneration();
            readerManager.updateSegments(infos);
            // Ensure that we commit and clear the local translog if a new commit has been made on the primary.
            // We do not compare against the last local commit gen here because it is possible to receive
            // a lower gen from a newly elected primary shard that is behind this shard's last commit gen.
            // In that case we still commit into the next local generation.
            if (incomingGeneration != this.lastReceivedPrimaryGen) {
                flush(false, true);
                translogManager.getDeletionPolicy().setLocalCheckpointOfSafeCommit(maxSeqNo);
                translogManager.rollTranslogGeneration();
            }
            this.lastReceivedPrimaryGen = incomingGeneration;
            localCheckpointTracker.fastForwardProcessedSeqNo(maxSeqNo);
        }
    }

    /**
     * Persist the latest live SegmentInfos.
     * <p>
     * This method creates a commit point from the latest SegmentInfos.
     *
     * @throws IOException - When there is an IO error committing the SegmentInfos.
     */
    private void commitSegmentInfos(SegmentInfos infos) throws IOException {
        // get a reference to the previous commit files so they can be decref'd once a new commit is made.
        final Collection<String> previousCommitFiles = getLastCommittedSegmentInfos().files(true);
        store.commitSegmentInfos(infos, localCheckpointTracker.getMaxSeqNo(), localCheckpointTracker.getProcessedCheckpoint());
        this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        // incref the latest on-disk commit.
        replicaFileTracker.incRef(this.lastCommittedSegmentInfos.files(true));
        // decref the prev commit.
        replicaFileTracker.decRef(previousCommitFiles);
        translogManager.syncTranslog();
    }

    private void commitSegmentInfos() throws IOException {
        commitSegmentInfos(getLatestSegmentInfos());
    }

    @Override
    public String getHistoryUUID() {
        return loadHistoryUUID(lastCommittedSegmentInfos.userData);
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return completionStatsCache.get(fieldNamePatterns);
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        ensureOpen();
        IndexResult indexResult = new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), false);
        final Translog.Location location = translogManager.add(new Translog.Index(index, indexResult));
        indexResult.setTranslogLocation(location);
        indexResult.setTook(System.nanoTime() - index.startTime());
        indexResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(index.seqNo());
        return indexResult;
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        ensureOpen();
        DeleteResult deleteResult = new DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true);
        final Translog.Location location = translogManager.add(new Translog.Delete(delete, deleteResult));
        deleteResult.setTranslogLocation(location);
        deleteResult.setTook(System.nanoTime() - delete.startTime());
        deleteResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(delete.seqNo());
        return deleteResult;
    }

    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        ensureOpen();
        NoOpResult noOpResult = new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
        final Translog.Location location = translogManager.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
        noOpResult.setTranslogLocation(location);
        noOpResult.setTook(System.nanoTime() - noOp.startTime());
        noOpResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(noOp.seqNo());
        return noOpResult;
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        return getFromSearcher(get, searcherFactory, SearcherScope.EXTERNAL);
    }

    @Override
    protected ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return readerManager;
    }

    /**
     * Refreshing of this engine will only happen internally when a new set of segments is received.  The engine will ignore external
     * refresh attempts so we can return false here.  Further Engine's existing implementation reads DirectoryReader.isCurrent after acquiring a searcher.
     * With this Engine's NRTReplicationReaderManager, This will use StandardDirectoryReader's implementation which determines if the reader is current by
     * comparing the on-disk SegmentInfos version against the one in the reader, which at refresh points will always return isCurrent false and then refreshNeeded true.
     * Even if this method returns refresh as needed, we ignore it and only ever refresh with incoming SegmentInfos.
     */
    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        return 0;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return false;
    }

    @Override
    public long getMinRetainedSeqNo() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return localCheckpointTracker.getPersistedCheckpoint();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return localCheckpointTracker.getStats(globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return translogManager.getLastSyncedGlobalCheckpoint();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return Arrays.asList(getSegmentInfo(getLatestSegmentInfos(), verbose));
    }

    @Override
    public void refresh(String source) throws EngineException {
        // Refresh on this engine should only ever happen in the reader after new segments arrive.
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return false;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {}

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        // readLock is held here to wait/block any concurrent close that acquires the writeLock.
        try (final ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (flushLock.tryLock() == false) {
                if (waitIfOngoing == false) {
                    return;
                }
                flushLock.lock();
            }
            // we are now locked.
            try {
                commitSegmentInfos();
            } catch (IOException e) {
                maybeFailEngine("flush", e);
                throw new FlushFailedEngineException(shardId, e);
            } finally {
                flushLock.unlock();
            }
        }
    }

    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {}

    @Override
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        if (flushFirst) {
            flush(false, true);
        }
        try {
            final IndexCommit indexCommit = Lucene.getIndexCommit(lastCommittedSegmentInfos, store.directory());
            return new GatedCloseable<>(indexCommit, () -> {});
        } catch (IOException e) {
            throw new EngineException(shardId, "Unable to build latest IndexCommit", e);
        }
    }

    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        return acquireLastIndexCommit(false);
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return new SafeCommitInfo(localCheckpointTracker.getProcessedCheckpoint(), lastCommittedSegmentInfos.totalMaxDoc());
    }

    @Override
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                final SegmentInfos latestSegmentInfos = getLatestSegmentInfos();
                /*
                 This is a workaround solution which decreases the chances of conflict on replica nodes when same file is copied
                 from two different primaries during failover. Increasing counter helps in avoiding this conflict as counter is
                 used to generate new segment file names. The ideal solution is to identify the counter from previous primary.
                 This is not required for remote store implementations given on failover the replica re-syncs with the store
                 during promotion.
                 */
                if (engineConfig.getIndexSettings().isRemoteStoreEnabled() == false) {
                    latestSegmentInfos.counter = latestSegmentInfos.counter + SI_COUNTER_INCREMENT;
                    latestSegmentInfos.changed();
                }
                try {
                    commitSegmentInfos(latestSegmentInfos);
                } catch (IOException e) {
                    // mark the store corrupted unless we are closing as result of engine failure.
                    // in this case Engine#failShard will handle store corruption.
                    if (failEngineLock.isHeldByCurrentThread() == false && store.isMarkedCorrupted() == false) {
                        try {
                            store.markStoreCorrupted(e);
                        } catch (IOException ex) {
                            logger.warn("Unable to mark store corrupted", ex);
                        }
                    }
                }
                IOUtils.close(readerManager, translogManager);
            } catch (Exception e) {
                logger.error("failed to close engine", e);
            } finally {
                try {
                    store.decRef();
                    logger.debug("engine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
    }

    @Override
    public void activateThrottling() {}

    @Override
    public void deactivateThrottling() {}

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void maybePruneDeletes() {}

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {}

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return localCheckpointTracker.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {}

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        final TranslogDeletionPolicy translogDeletionPolicy = translogManager.getDeletionPolicy();
        translogDeletionPolicy.setRetentionAgeInMillis(translogRetentionAge.millis());
        translogDeletionPolicy.setRetentionSizeInBytes(translogRetentionSize.getBytes());
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    protected SegmentInfos getLatestSegmentInfos() {
        return readerManager.getSegmentInfos();
    }

    @Override
    public synchronized GatedCloseable<SegmentInfos> getSegmentInfosSnapshot() {
        // get reference to latest infos
        final SegmentInfos latestSegmentInfos = getLatestSegmentInfos();
        // incref all files
        try {
            final Collection<String> files = latestSegmentInfos.files(false);
            replicaFileTracker.incRef(files);
            return new GatedCloseable<>(latestSegmentInfos, () -> { replicaFileTracker.decRef(files); });
        } catch (IOException e) {
            throw new EngineException(shardId, e.getMessage(), e);
        }
    }

    protected LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    private DirectoryReader getDirectoryReader() throws IOException {
        // for segment replication: replicas should create the reader from store, we don't want an open IW on replicas.
        return new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(store.directory()), Lucene.SOFT_DELETES_FIELD);
    }
}
