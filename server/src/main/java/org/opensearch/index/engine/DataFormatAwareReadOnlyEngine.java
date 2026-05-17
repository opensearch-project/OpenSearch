/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CatalogSnapshotLifecycleListener;
import org.opensearch.index.engine.exec.CommitFileManager;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.engine.exec.coord.LuceneVersionConverter;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static org.opensearch.index.engine.exec.coord.CatalogSnapshotManager.createInitialSnapshot;

/**
 * Read-only engine for primaries operating on pluggable data formats.
 *
 * <p>This engine is the flip target when a read-only replica is promoted to primary
 * (via {@code resetToWriteableEngine}). Unlike {@link DataFormatAwareEngine},
 * it has no {@link IndexWriter}, no active translog, and rejects all write
 * operations. Reads route through {@code TieredSubdirectoryAwareDirectory}
 * to remote storage.
 *
 * <p>Replicas continue using {@link DataFormatAwareNRTReplicationEngine}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatAwareReadOnlyEngine implements Indexer {

    private final Logger logger;
    private final EngineConfig engineConfig;
    private final ShardId shardId;
    private final Store store;

    // Core state
    private final CatalogSnapshotManager catalogSnapshotManager;
    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final NoOpTranslogManager translogManager;
    private final Committer committer;

    // Lifecycle
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final SetOnce<Exception> failedEngine = new SetOnce<>();
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    private final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());
    private final ReentrantLock failEngineLock = new ReentrantLock();

    // Metadata
    @Nullable
    private volatile String historyUUID;
    private final List<ReferenceManager.RefreshListener> internalRefreshListeners;

    public DataFormatAwareReadOnlyEngine(EngineConfig engineConfig) {
        this.logger = Loggers.getLogger(DataFormatAwareReadOnlyEngine.class, engineConfig.getShardId());
        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();

        store.incRef();
        Map<DataFormat, EngineReaderManager<?>> readerManagersRef = null;
        CatalogSnapshotManager catalogSnapshotManagerRef = null;
        Committer constructingCommitter = null;
        boolean success = false;
        try {
            // Create committer (same as NRT — replica mode)
            this.committer = constructingCommitter = engineConfig.getCommitterFactory()
                .getCommitter(new CommitterConfig(engineConfig, () -> {}, true));
            Map<String, String> userData = committer.getLastCommittedData();

            // Catalog file deleter
            FileDeleter compositeDeleter = buildFileDeleter();

            List<CatalogSnapshot> committed = committer.listCommittedSnapshots();

            // Build per-format reader managers (same pattern as NRT)
            DataFormatRegistry registry = engineConfig.getDataFormatRegistry();
            Map<String, Supplier<DataFormatDescriptor>> allDescriptors = registry.getFormatDescriptors(engineConfig.getIndexSettings());
            Map<DataFormat, EngineReaderManager<?>> aggregated = new HashMap<>();
            for (String formatName : allDescriptors.keySet()) {
                DataFormat format = registry.format(formatName);
                aggregated.putAll(
                    registry.getReaderManager(
                        new ReaderManagerConfig(
                            Optional.of((IndexStoreProvider) dataFormat -> () -> store),
                            format,
                            registry,
                            store.shardPath(),
                            store.getDataformatAwareStoreHandles()
                        )
                    )
                );
            }
            readerManagersRef = Map.copyOf(aggregated);
            this.readerManagers = readerManagersRef;

            // Register reader managers as catalog snapshot lifecycle listeners so they are notified
            // (via installSnapshot → afterRefresh) BEFORE latestCatalogSnapshot is swapped. This
            // guarantees readers are updated before the snapshot becomes externally visible.
            List<CatalogSnapshotLifecycleListener> snapshotListeners = new ArrayList<>(readerManagersRef.values());

            catalogSnapshotManagerRef = new CatalogSnapshotManager(
                committed,
                new DataFormatAwareNRTReplicationEngine.ReplicaDeletionPolicy(),
                compositeDeleter,
                Map.of(),
                snapshotListeners,
                store.shardPath(),
                committer
            );
            this.catalogSnapshotManager = catalogSnapshotManagerRef;
            this.internalRefreshListeners = new ArrayList<>(engineConfig.getInternalRefreshListener());

            // Initialize sequence number tracking
            final SequenceNumbers.CommitInfo seqNoInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
            this.localCheckpointTracker = new LocalCheckpointTracker(seqNoInfo.maxSeqNo, seqNoInfo.localCheckpoint);

            // Initialize no-op translog
            String translogUUID = userData.get(Translog.TRANSLOG_UUID_KEY);
            this.translogManager = new NoOpTranslogManager(
                shardId,
                readLock,
                this::ensureOpen,
                new TranslogStats(0, 0, 0, 0, 0),
                Translog.EMPTY_TRANSLOG_SNAPSHOT,
                translogUUID != null ? translogUUID : "readonly-" + shardId,
                true
            );

            // History UUID — fabricate if absent (same as NRT engine) for recovery protocol compliance
            this.historyUUID = userData.get(Engine.HISTORY_UUID_KEY);
            if (this.historyUUID == null) {
                this.historyUUID = UUIDs.randomBase64UUID();
            }

            success = true;
            logger.info("Created DataFormatAwareReadOnlyEngine");
        } catch (IOException e) {
            throw new EngineCreationFailureException(shardId, "failed to create read-only engine", e);
        } finally {
            if (success == false) {
                if (readerManagersRef != null) {
                    IOUtils.closeWhileHandlingException(readerManagersRef.values());
                }
                IOUtils.closeWhileHandlingException(catalogSnapshotManagerRef);
                IOUtils.closeWhileHandlingException(constructingCommitter);
                if (isClosed.get() == false) {
                    store.decRef();
                }
            }
        }
    }

    // ---- LifecycleAware ----

    @Override
    public void ensureOpen() {
        if (isClosed.get()) {
            throw new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
        }
    }

    // ---- Indexer core ----

    @Override
    public EngineConfig config() {
        return engineConfig;
    }

    @Override
    public boolean isReplicaIndexer() {
        return false;
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    // ---- IndexReaderProvider (Task 2) ----

    @Override
    public GatedCloseable<Reader> acquireReader() throws IOException {
        ensureOpen();
        GatedCloseable<CatalogSnapshot> snapshotRef = catalogSnapshotManager.acquireSnapshot();
        try {
            CatalogSnapshot catalogSnapshot = snapshotRef.get();
            Map<DataFormat, Object> readers = new HashMap<>();
            for (Map.Entry<DataFormat, EngineReaderManager<?>> entry : readerManagers.entrySet()) {
                Object reader = entry.getValue().getReader(catalogSnapshot);
                if (reader != null) {
                    readers.put(entry.getKey(), reader);
                }
            }
            DataFormatAwareEngine.DataFormatAwareReader reader = new DataFormatAwareEngine.DataFormatAwareReader(snapshotRef, readers);
            return new GatedCloseable<>(reader, reader::close);
        } catch (Exception e) {
            snapshotRef.close();
            throw e;
        }
    }

    // ---- IndexerEngineOperations (Task 3 — write rejection) ----

    @Override
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        throw new UnsupportedOperationException("DataFormatAwareReadOnlyEngine does not support indexing");
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        throw new UnsupportedOperationException("DataFormatAwareReadOnlyEngine does not support deletes");
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        throw new UnsupportedOperationException("DataFormatAwareReadOnlyEngine does not support no-ops");
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
        throw new UnsupportedOperationException("DataFormatAwareReadOnlyEngine does not support indexing");
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
        throw new UnsupportedOperationException("DataFormatAwareReadOnlyEngine does not support deletes");
    }

    // ---- IndexerLifecycleOperations (Tasks 4, 7) ----

    @Override
    public void finalizeReplication(CatalogSnapshot incoming) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            final long maxSeqNo = Long.parseLong(incoming.getUserData().get(SequenceNumbers.MAX_SEQ_NO));

            // Notify refresh listeners before applying snapshot (same as NRT)
            notifyRefreshListenersBefore();
            boolean success = false;
            try {
                catalogSnapshotManager.applyReplicationSnapshot(incoming);
                success = true;
            } finally {
                notifyRefreshListenersAfter(success);
            }

            final String incomingHistoryUUID = incoming.getUserData().get(Engine.HISTORY_UUID_KEY);
            if (incomingHistoryUUID != null) {
                this.historyUUID = incomingHistoryUUID;
            }

            localCheckpointTracker.fastForwardProcessedSeqNo(maxSeqNo);
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        // No-op: read-only engine does not buffer documents
    }

    @Override
    public boolean maybeRefresh(String source) {
        return false;
    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        // No-op: no indexing buffer
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) {
        // No-op: read-only engine does not flush. State is fully remote.
    }

    @Override
    public void flush() {
        // No-op
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
        // No-op: read-only engine does not merge
    }

    @Override
    public void activateThrottling() {
        // No-op
    }

    @Override
    public void deactivateThrottling() {
        // No-op
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        // No-op: no translog deletion policy to update
    }

    @Override
    public void maybePruneDeletes() {
        // No-op: data-format engines do not maintain Lucene-style delete tombstones
    }

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        // No-op
    }

    // ---- IndexerStateManager (Task 8) ----

    @Override
    public long getPersistedLocalCheckpoint() {
        // Segments are immediately visible upon replication; processed == persisted
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public long lastRefreshedCheckpoint() {
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
    public long getMinRetainedSeqNo() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return Long.MIN_VALUE;
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        // No-op for read-only primary
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return localCheckpointTracker.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        // No-op for read-only primary
    }

    @Override
    public long getLastWriteNanos() {
        return System.nanoTime();
    }

    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        return 0;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return startingSeqNo > localCheckpointTracker.getMaxSeqNo();
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    // ---- IndexerStatistics (Task 8) ----

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public long unreferencedFileCleanUpsPerformed() {
        return 0;
    }

    @Override
    public CommitStats commitStats() {
        ensureOpen();
        try {
            return new CommitStats(store.readLastCommittedSegmentsInfo());
        } catch (Exception e) {
            logger.debug("Unable to read last committed SegmentInfos; returning empty CommitStats", e);
            return new CommitStats(new SegmentInfos(org.apache.lucene.util.Version.LATEST.major));
        }
    }

    @Override
    public DocsStats docStats() {
        try (GatedCloseable<CatalogSnapshot> snapshot = catalogSnapshotManager.acquireSnapshot()) {
            long count = snapshot.get().getNumDocs();
            long totalSize = snapshot.get()
                .getSegments()
                .stream()
                .flatMap(segment -> segment.dfGroupedSearchableFiles().values().stream())
                .mapToLong(WriterFileSet::getTotalSize)
                .sum();
            assert count >= 0 : "doc count must be non-negative but was: " + count;
            assert totalSize >= 0 : "total size must be non-negative but was: " + totalSize;
            return new DocsStats.Builder().deleted(0L).count(count).totalSizeInBytes(totalSize).build();
        } catch (IOException ex) {
            throw new OpenSearchException(ex);
        }
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        return new SegmentsStats();
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return new CompletionStats();
    }

    @Override
    public PollingIngestStats pollingIngestStats() {
        return null;
    }

    @Override
    public MergeStats getMergeStats() {
        return new MergeStats();
    }

    // ---- Recovery and Snapshot support (Task 6) ----

    @Override
    public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
        ensureOpen();
        return catalogSnapshotManager.acquireSnapshot();
    }

    @Override
    public byte[] serializeSnapshotToRemoteMetadata(CatalogSnapshot catalogSnapshot) throws IOException {
        throw new UnsupportedOperationException("Read-only primary does not produce upload metadata bytes");
    }

    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        throw new UnsupportedOperationException(
            "acquireSafeIndexCommit is not supported on DataFormatAwareReadOnlyEngine; use acquireSafeCatalogSnapshot"
        );
    }

    @Override
    public GatedCloseable<CatalogSnapshot> acquireSafeCatalogSnapshot() throws EngineException {
        ensureOpen();
        return catalogSnapshotManager.acquireSnapshot();
    }

    @Override
    public GatedCloseable<CatalogSnapshot> acquireLastCommittedSnapshot(boolean flushFirst) throws EngineException {
        ensureOpen();
        // flushFirst is ignored — read-only engine does not flush
        return catalogSnapshotManager.acquireSnapshot();
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        ensureOpen();
        try (GatedCloseable<CatalogSnapshot> snapshot = catalogSnapshotManager.acquireSnapshot()) {
            return new SafeCommitInfo(localCheckpointTracker.getProcessedCheckpoint(), (int) snapshot.get().getNumDocs());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return Translog.EMPTY_TRANSLOG_SNAPSHOT;
    }

    // ---- Lifecycle (Task 7) ----

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) {
            try (ReleasableLock ignored = writeLock.acquire()) {
                closeNoLock("api");
            }
        }
        awaitPendingClose();
    }

    @Override
    public void flushAndClose() throws IOException {
        close();
    }

    @Override
    public void failEngine(String reason, @Nullable Exception failure) {
        if (failEngineLock.tryLock()) {
            try {
                if (failedEngine.get() != null) {
                    logger.warn(() -> new ParameterizedMessage("tried to fail engine but already failed, ignoring. [{}]", reason), failure);
                    return;
                }
                failedEngine.set(failure != null ? failure : new IllegalStateException(reason));
                try {
                    closeNoLock("engine failed on: [" + reason + "]");
                } finally {
                    logger.warn(() -> new ParameterizedMessage("failed engine [{}]", reason), failure);
                    engineConfig.getEventListener().onFailedEngine(reason, failure);
                }
            } catch (Exception inner) {
                if (failure != null) {
                    inner.addSuppressed(failure);
                }
                logger.warn("failEngine threw exception", inner);
            }
        } else {
            logger.debug(() -> new ParameterizedMessage("tried to fail engine but could not acquire lock [{}]", reason), failure);
        }
    }

    // ---- Internal helpers ----

    private void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock must be held or the engine must be currently failing";
            try {
                List<Closeable> closeables = new ArrayList<>(readerManagers.values());
                closeables.add(catalogSnapshotManager);
                IOUtils.close(closeables);
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

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void notifyRefreshListenersBefore() throws IOException {
        for (ReferenceManager.RefreshListener listener : internalRefreshListeners) {
            listener.beforeRefresh();
        }
    }

    private void notifyRefreshListenersAfter(boolean didRefresh) throws IOException {
        for (ReferenceManager.RefreshListener listener : internalRefreshListeners) {
            listener.afterRefresh(didRefresh);
        }
    }

    private FileDeleter buildFileDeleter() {
        return filesByFormat -> {
            Map<String, Collection<String>> failed = new HashMap<>();
            for (Map.Entry<String, Collection<String>> entry : filesByFormat.entrySet()) {
                final String formatName = entry.getKey();
                for (String name : entry.getValue()) {
                    if (committer.isCommitManagedFile(name)) {
                        continue;
                    }
                    try {
                        store.directory().deleteFile(name);
                    } catch (java.nio.file.NoSuchFileException ignored) {
                        // already gone
                    } catch (IOException e) {
                        logger.warn("Failed to delete file [{}] in format [{}]: {}", name, formatName, e.getMessage());
                        failed.computeIfAbsent(formatName, k -> new ArrayList<>()).add(name);
                    }
                }
            }
            return failed;
        };
    }

    // ---- Getters for internal state (used by tests) ----

    CatalogSnapshotManager getCatalogSnapshotManager() {
        return catalogSnapshotManager;
    }

    Map<DataFormat, EngineReaderManager<?>> getReaderManagers() {
        return readerManagers;
    }

    LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }
}
