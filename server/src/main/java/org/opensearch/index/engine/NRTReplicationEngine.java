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
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

import static org.opensearch.index.seqno.SequenceNumbers.MAX_SEQ_NO;

/**
 * This is an {@link Engine} implementation intended for replica shards when Segment Replication
 * is enabled.  This Engine does not create an IndexWriter, rather it refreshes a {@link NRTReplicationReaderManager}
 * with new Segments when received from an external source.
 *
 * @opensearch.internal
 */
public class NRTReplicationEngine extends Engine {

    private volatile SegmentInfos lastCommittedSegmentInfos;
    private final NRTReplicationReaderManager readerManager;
    private final CompletionStatsCache completionStatsCache;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final WriteOnlyTranslogManager translogManager;

    private volatile long lastReceivedGen = SequenceNumbers.NO_OPS_PERFORMED;

    private static final int SI_COUNTER_INCREMENT = 10;

    public NRTReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);
        store.incRef();
        NRTReplicationReaderManager readerManager = null;
        WriteOnlyTranslogManager translogManagerRef = null;
        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            readerManager = new NRTReplicationReaderManager(OpenSearchDirectoryReader.wrap(getDirectoryReader(), shardId));
            final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                this.lastCommittedSegmentInfos.getUserData().entrySet()
            );
            this.localCheckpointTracker = new LocalCheckpointTracker(commitInfo.maxSeqNo, commitInfo.localCheckpoint);
            this.completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
            this.readerManager = readerManager;
            this.readerManager.addListener(completionStatsCache);
            for (ReferenceManager.RefreshListener listener : engineConfig.getExternalRefreshListener()) {
                this.readerManager.addListener(listener);
            }
            for (ReferenceManager.RefreshListener listener : engineConfig.getInternalRefreshListener()) {
                this.readerManager.addListener(listener);
            }
            final Map<String, String> userData = store.readLastCommittedSegmentsInfo().getUserData();
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
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(store::decRef, readerManager, translogManagerRef);
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        }
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    public synchronized void updateSegments(final SegmentInfos infos) throws IOException {
        // Update the current infos reference on the Engine's reader.
        ensureOpen();
        try (ReleasableLock lock = writeLock.acquire()) {
            final long maxSeqNo = Long.parseLong(infos.userData.get(MAX_SEQ_NO));
            final long incomingGeneration = infos.getGeneration();
            readerManager.updateSegments(infos);

            // Commit and roll the translog when we receive a different generation than what was last received.
            // lower/higher gens are possible from a new primary that was just elected.
            if (incomingGeneration != lastReceivedGen) {
                commitSegmentInfos();
                translogManager.getDeletionPolicy().setLocalCheckpointOfSafeCommit(maxSeqNo);
                translogManager.rollTranslogGeneration();
            }
            lastReceivedGen = incomingGeneration;
            localCheckpointTracker.fastForwardProcessedSeqNo(maxSeqNo);
        }
    }

    /**
     * Persist the latest live SegmentInfos.
     *
     * This method creates a commit point from the latest SegmentInfos. It is intended to be used when this shard is about to be promoted as the new primary.
     *
     * TODO: If this method is invoked while the engine is currently updating segments on its reader, wait for that update to complete so the updated segments are used.
     *
     *
     * @throws IOException - When there is an IO error committing the SegmentInfos.
     */
    private void commitSegmentInfos(SegmentInfos infos) throws IOException {
        store.commitSegmentInfos(infos, localCheckpointTracker.getMaxSeqNo(), localCheckpointTracker.getProcessedCheckpoint());
        this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        translogManager.syncTranslog();
    }

    protected void commitSegmentInfos() throws IOException {
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
        maybeRefresh(source);
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        try {
            return readerManager.maybeRefresh();
        } catch (IOException e) {
            try {
                failEngine("refresh failed source[" + source + "]", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {}

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {}

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
                 */
                latestSegmentInfos.counter = latestSegmentInfos.counter + SI_COUNTER_INCREMENT;
                latestSegmentInfos.changed();
                commitSegmentInfos(latestSegmentInfos);
                IOUtils.close(readerManager, translogManager, store::decRef);
            } catch (Exception e) {
                logger.warn("failed to close engine", e);
            } finally {
                logger.debug("engine closed [{}]", reason);
                closedLatch.countDown();
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

    protected LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    private DirectoryReader getDirectoryReader() throws IOException {
        // for segment replication: replicas should create the reader from store, we don't want an open IW on replicas.
        return new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(store.directory()), Lucene.SOFT_DELETES_FIELD);
    }
}
