/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.SegmentInfosCatalogSnapshot;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;

/**
 * An indexer implementation that uses an engine to perform indexing operations.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class EngineBackedIndexer implements Indexer {

    private final Engine engine;

    public EngineBackedIndexer(Engine engine) {
        this.engine = engine;
    }

    @Override
    public EngineConfig config() {
        return engine.config();
    }

    @Override
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        return engine.index(index);
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        return engine.delete(delete);
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        return engine.noOp(noOp);
    }

    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        return engine.countNumberOfHistoryOperations(source, fromSeqNo, toSeqNumber);
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return engine.hasCompleteOperationHistory(reason, startingSeqNo);
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return engine.getIndexBufferRAMBytesUsed();
    }

    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return engine.getMaxSeenAutoIdTimestamp();
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        engine.updateMaxUnsafeAutoIdTimestamp(newTimestamp);
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return engine.getMaxSeqNoOfUpdatesOrDeletes();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        engine.advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOnPrimary);
    }

    @Override
    public long getLastWriteNanos() {
        return engine.getLastWriteNanos();
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return engine.fillSeqNoGaps(primaryTerm);
    }

    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {
        engine.forceMerge(flush, maxNumSegments, onlyExpungeDeletes, upgrade, upgradeOnlyAncientSegments, forceMergeUUID);
    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        engine.onSettingsChanged(translogRetentionAge, translogRetentionSize, softDeletesRetentionOps);
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        engine.writeIndexingBuffer();
    }

    @Override
    public void refresh(String source) throws EngineException {
        engine.refresh(source);
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        engine.flush(force, waitIfOngoing);
    }

    @Override
    public void flush() {
        engine.flush();
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return engine.shouldPeriodicallyFlush();
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return engine.getSafeCommitInfo();
    }

    @Override
    public TranslogManager translogManager() {
        return engine.translogManager();
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return engine.acquireHistoryRetentionLock();
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return engine.newChangesSnapshot(source, fromSeqNo, toSeqNo, requiredFullRange, accurateCount);
    }

    @Override
    public String getHistoryUUID() {
        return engine.getHistoryUUID();
    }

    @Override
    public void flushAndClose() throws IOException {
        engine.flushAndClose();
    }

    @Override
    public void failEngine(String reason, Exception failure) {
        engine.failEngine(reason, failure);
    }

    @Override
    public Engine.Index prepareIndex(
        DocumentMapperForType docMapper,
        SourceToParse source,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long autoGeneratedIdTimestamp,
        boolean isRetry,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        return engine.prepareIndex(
            docMapper,
            source,
            seqNo,
            primaryTerm,
            version,
            versionType,
            origin,
            autoGeneratedIdTimestamp,
            isRetry,
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    @Override
    public Engine.Delete prepareDelete(
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        return engine.prepareDelete(id, seqNo, primaryTerm, version, versionType, origin, ifSeqNo, ifPrimaryTerm);
    }

    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        return engine.acquireSafeIndexCommit();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return engine.getPersistedLocalCheckpoint();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return engine.getProcessedLocalCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return engine.getSeqNoStats(globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return engine.getLastSyncedGlobalCheckpoint();
    }

    @Override
    public long getMinRetainedSeqNo() {
        return engine.getMinRetainedSeqNo();
    }

    @Override
    public CommitStats commitStats() {
        return engine.commitStats();
    }

    @Override
    public DocsStats docStats() {
        return engine.docStats();
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        return engine.segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return engine.completionStats(fieldNamePatterns);
    }

    @Override
    public PollingIngestStats pollingIngestStats() {
        return engine.pollingIngestStats();
    }

    @Override
    public MergeStats getMergeStats() {
        return engine.getMergeStats();
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return engine.getIndexThrottleTimeInMillis();
    }

    @Override
    public boolean isThrottled() {
        return engine.isThrottled();
    }

    @Override
    public void activateThrottling() {
        engine.activateThrottling();
    }

    @Override
    public void deactivateThrottling() {
        engine.deactivateThrottling();
    }

    @Override
    public long getWritingBytes() {
        return engine.getWritingBytes();
    }

    @Override
    public long unreferencedFileCleanUpsPerformed() {
        return engine.unreferencedFileCleanUpsPerformed();
    }

    @Override
    public boolean refreshNeeded() {
        return engine.refreshNeeded();
    }

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        engine.verifyEngineBeforeIndexClosing();
    }

    @Override
    public void maybePruneDeletes() {
        engine.maybePruneDeletes();
    }

    @Override
    public boolean maybeRefresh(String source) {
        return engine.maybeRefresh(source);
    }

    @Override
    public void close() throws IOException {
        engine.close();
    }

    @Override
    public void ensureOpen() {
        engine.ensureOpen();
    }

    @Override
    public long lastRefreshedCheckpoint() {
        if (engine instanceof InternalEngine) {
            return ((InternalEngine) engine).lastRefreshedCheckpoint();
        }
        return Indexer.super.lastRefreshedCheckpoint();
    }

    @Override
    public long currentOngoingRefreshCheckpoint() {
        if (engine instanceof InternalEngine) {
            return ((InternalEngine) engine).currentOngoingRefreshCheckpoint();
        }
        return Indexer.super.currentOngoingRefreshCheckpoint();
    }

    @Override
    public long getNativeBytesUsed() {
        return Indexer.super.getNativeBytesUsed();
    }

    /**
     * Returns a snapshot of the catalog of segments in this engine. This snapshot is
     * guaranteed to be consistent and can be used for recovery purposes.
     */
    @ExperimentalApi
    @Override
    public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
        GatedCloseable<SegmentInfos> segmentInfosRef = engine.getSegmentInfosSnapshot();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfosRef.get());
        return new GatedCloseable<>(snapshot, segmentInfosRef::close);
    }

    @Override
    public GatedCloseable<Reader> acquireReader() throws IOException {
        // TODO: Replace with a reader backed by segment infos catalog snapshot and Lucene's Directory reader.
        // For now we throw an exception as this is not yet implemented
        throw new UnsupportedOperationException("acquireReader is not supported in EngineBackedIndexer");
    }

    public Engine getEngine() {
        return engine;
    }
}
