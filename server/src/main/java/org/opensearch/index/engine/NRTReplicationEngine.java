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
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.Translog;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

/**
 * This is an {@link Engine} implementation intended for replica shards when Segment Replication
 * is enabled.  This Engine does not create an IndexWriter, rather it refreshes a {@link NRTReplicationReaderManager}
 * with new Segments when received from an external source.
 *
 * @opensearch.internal
 */
public class NRTReplicationEngine extends TranslogAwareEngine {

    private volatile SegmentInfos lastCommittedSegmentInfos;
    private final NRTReplicationReaderManager readerManager;
    private final CompletionStatsCache completionStatsCache;
    private final LocalCheckpointTracker localCheckpointTracker;

    public NRTReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);
        store.incRef();
        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            readerManager = new NRTReplicationReaderManager(OpenSearchDirectoryReader.wrap(getDirectoryReader(), shardId));
            final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                this.lastCommittedSegmentInfos.getUserData().entrySet()
            );
            this.localCheckpointTracker = new LocalCheckpointTracker(commitInfo.maxSeqNo, commitInfo.localCheckpoint);
            this.completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
            this.readerManager.addListener(completionStatsCache);
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(store::decRef, translog);
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        }
    }

    public synchronized void updateSegments(final SegmentInfos infos, long seqNo) throws IOException {
        try {
            store.incRef();
            // Update the current infos reference on the Engine's reader.
            readerManager.updateSegments(infos);

            // only update the persistedSeqNo and "lastCommitted" infos reference if the incoming segments have a higher
            // generation. We can still refresh with incoming SegmentInfos that are not part of a commit point.
            if (infos.getGeneration() > lastCommittedSegmentInfos.getGeneration()) {
                this.lastCommittedSegmentInfos = infos;
                localCheckpointTracker.fastForwardPersistedSeqNo(seqNo);
            }
            readerManager.maybeRefresh();
        } finally {
            store.decRef();
        }
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
        IndexResult indexResult = new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), false);
        addIndexOperationToTranslog(index, indexResult);
        indexResult.setTook(System.nanoTime() - index.startTime());
        indexResult.freeze();
        localCheckpointTracker.markSeqNoAsProcessed(indexResult.getSeqNo());
        return indexResult;
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        DeleteResult deleteResult = new DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true);
        addDeleteOperationToTranslog(delete, deleteResult);
        deleteResult.setTook(System.nanoTime() - delete.startTime());
        deleteResult.freeze();
        localCheckpointTracker.markSeqNoAsProcessed(deleteResult.getSeqNo());
        return deleteResult;
    }

    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        NoOpResult noOpResult = new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
        final Translog.Location location = translog.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
        noOpResult.setTranslogLocation(location);
        noOpResult.setTook(System.nanoTime() - noOp.startTime());
        noOpResult.freeze();
        localCheckpointTracker.markSeqNoAsProcessed(noOpResult.getSeqNo());
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

    public long getProcessedLocalCheckpoint() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return localCheckpointTracker.getStats(globalCheckpoint);
    }

    @Override
    protected LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return Arrays.asList(getSegmentInfo(lastCommittedSegmentInfos, verbose));
    }

    @Override
    public void refresh(String source) throws EngineException {}

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
        store.incRef();
        try {
            final IndexCommit indexCommit = Lucene.getIndexCommit(lastCommittedSegmentInfos, store.directory());
            return new GatedCloseable<>(indexCommit, store::decRef);
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
                IOUtils.close(readerManager, translog, store::decRef);
            } catch (Exception e) {
                logger.warn("failed to close engine", e);
            } finally {
                try {
                    logger.debug("engine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
    }

    @Override
    protected void deleteUnusedFiles() {}

    @Override
    public void activateThrottling() {}

    @Override
    public void deactivateThrottling() {}

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public Engine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {
        throw new UnsupportedOperationException("Read only replicas do not have an IndexWriter and cannot recover from a translog.");
    }

    @Override
    public void skipTranslogRecovery() {
        // Do nothing.
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
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    protected SegmentInfos getLatestSegmentInfos() {
        return readerManager.getSegmentInfos();
    }

    private DirectoryReader getDirectoryReader() throws IOException {
        // for segment replication: replicas should create the reader from store, we don't want an open IW on replicas.
        return new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(store.directory()), Lucene.SOFT_DELETES_FIELD);
    }
}
