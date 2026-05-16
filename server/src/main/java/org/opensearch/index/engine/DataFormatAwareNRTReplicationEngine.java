/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.concurrent.GatedConditionalCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.Lucene;
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
import org.opensearch.index.engine.exec.commit.Committer.CommitInput;
import org.opensearch.index.engine.exec.commit.Committer.CommitResult;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * Replica engine for pluggable data-format indices. Operates on {@link CatalogSnapshot}
 * instead of {@code SegmentInfos} and implements {@link Indexer} directly (no IndexWriter).
 * Segments arrive via replication; local writes go only to the translog.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatAwareNRTReplicationEngine implements Indexer {

    private volatile CatalogSnapshot lastCommittedSnapshot;
    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final WriteOnlyTranslogManager translogManager;
    private final Lock flushLock = new ReentrantLock();

    private volatile long lastReceivedPrimaryCommitGen = SequenceNumbers.NO_OPS_PERFORMED;

    private static final int SI_COUNTER_INCREMENT = 100000;

    // Fields that Engine subclasses inherit; Indexer implementations must declare directly.
    private final Logger logger;
    private final EngineConfig engineConfig;
    private final ShardId shardId;
    private final Store store;
    private final CatalogSnapshotManager catalogSnapshotManager;
    private final Committer committer;
    private volatile long lastWriteNanos = System.nanoTime();
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    private final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());
    private final ReentrantLock failEngineLock = new ReentrantLock();
    private final List<ReferenceManager.RefreshListener> internalRefreshListeners;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final SetOnce<Exception> failedEngine = new SetOnce<>();
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    public DataFormatAwareNRTReplicationEngine(EngineConfig engineConfig) {
        this.logger = Loggers.getLogger(DataFormatAwareNRTReplicationEngine.class, engineConfig.getShardId());
        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();

        store.incRef();
        Map<DataFormat, EngineReaderManager<?>> readerManagersRef = null;
        WriteOnlyTranslogManager translogManagerRef = null;
        CatalogSnapshotManager catalogSnapshotManagerRef = null;
        boolean success = false;
        Committer constructingCommitter = null;
        try {
            this.committer = constructingCommitter = engineConfig.getCommitterFactory().getCommitter(new CommitterConfig(engineConfig, () -> {}, true));
            // Bootstrap an empty commit if no segments file exists (fresh replica).
            Map<String, String> userData = committer.getLastCommittedData();

            // Restore CatalogSnapshot from commit data.

            // Catalog file deleter — DataFormatAwareStoreDirectory routes filenames to the
            // correct per-format subdirectory based on extension; no per-format wiring needed.
            FileDeleter compositeDeleter = buildReplicaFileDeleter();

            List<CatalogSnapshot> committed = committer.listCommittedSnapshots();
            assert false == committed.isEmpty() : "At least one commit should be present for replica"; // ????????

            DataFormatRegistry registry = engineConfig.getDataFormatRegistry();

            Map<String, Supplier<DataFormatDescriptor>> allDescriptors = registry.getFormatDescriptors(engineConfig.getIndexSettings());
            Map<DataFormat, EngineReaderManager<?>> aggregated = new HashMap<>();
            for (String formatName : allDescriptors.keySet()) {
                DataFormat format = registry.format(formatName);
                aggregated.putAll(
                    registry.getReaderManager(
                        new ReaderManagerConfig(
                            Optional.of(new IndexStoreProvider() {
                                @Override
                                public FormatStore getStore(DataFormat dataFormat) {
                                    return new FormatStore() {
                                        @Override
                                        public Store store() {
                                            return store;
                                        }
                                    };
                                }
                            }),
                            format,
                            registry,
                            store.shardPath(),
                            store.getDataformatAwareStoreHandles()
                        )
                    )
                );
            }
            readerManagersRef = Map.copyOf(aggregated);

            // Register reader managers as catalog snapshot lifecycle listeners so they are notified
            // (via installSnapshot → afterRefresh) BEFORE latestCatalogSnapshot is swapped. This
            // guarantees readers are updated before the snapshot becomes externally visible.
            List<CatalogSnapshotLifecycleListener> snapshotListeners = new ArrayList<>(readerManagersRef.values());

            catalogSnapshotManagerRef = new CatalogSnapshotManager(
                committed,
                CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY,
                compositeDeleter,
                Map.of(),
                snapshotListeners,
                store.shardPath(),
                committer
            );

            this.catalogSnapshotManager = catalogSnapshotManagerRef;
            this.internalRefreshListeners = new ArrayList<>(engineConfig.getInternalRefreshListener());

            try (GatedCloseable<CatalogSnapshot> snapshotRef = catalogSnapshotManager.acquireSnapshot()) {
                this.lastCommittedSnapshot = snapshotRef.get();
            }

            final SequenceNumbers.CommitInfo seqNoInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
            this.localCheckpointTracker = new LocalCheckpointTracker(seqNoInfo.maxSeqNo, seqNoInfo.localCheckpoint);

            this.readerManagers = readerManagersRef;

            final String translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));

            translogManagerRef = new WriteOnlyTranslogManager(
                engineConfig.getTranslogConfig(),
                engineConfig.getPrimaryTermSupplier(),
                engineConfig.getGlobalCheckpointSupplier(),
                getTranslogDeletionPolicy(),
                shardId,
                readLock,
                () -> localCheckpointTracker,
                translogUUID,
                NRTReplicaTranslogOps.createTranslogEventListener(this::failEngine, this::translogManager, shardId),
                DataFormatAwareNRTReplicationEngine.this,
                engineConfig.getTranslogFactory(),
                engineConfig.getStartedPrimarySupplier(),
                TranslogOperationHelper.create(engineConfig)
            );
            this.translogManager = translogManagerRef;

            success = true;
            logger.trace("created new DataFormatAwareNRTReplicationEngine");
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (success == false) {
                // Close per-format reader managers on failure
                if (readerManagersRef != null) {
                    IOUtils.closeWhileHandlingException(readerManagersRef.values());
                }
                IOUtils.closeWhileHandlingException(catalogSnapshotManagerRef);
                IOUtils.closeWhileHandlingException(translogManagerRef);
                IOUtils.closeWhileHandlingException(constructingCommitter);
                if (isClosed.get() == false) {
                    // failure, we need to dec the store reference
                    store.decRef();
                }
            }
        }
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    /** Updates the catalog snapshot with a new snapshot received from the primary via replication. */
    public void updateCatalogSnapshot(CatalogSnapshot incoming) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            final long maxSeqNo = Long.parseLong(incoming.getUserData().get(SequenceNumbers.MAX_SEQ_NO));
            final long incomingCommitGeneration = incoming.getLastCommitGeneration();

            applyCatalogSnapshot(incoming);

            if (incomingCommitGeneration != lastReceivedPrimaryCommitGen) {
                flush(false, true, false);
                translogManager.getDeletionPolicy().setLocalCheckpointOfSafeCommit(maxSeqNo);
                translogManager.rollTranslogGeneration();
            }
            lastReceivedPrimaryCommitGen = incomingCommitGeneration;
            localCheckpointTracker.fastForwardProcessedSeqNo(maxSeqNo);
        }
    }

    /** Applies the incoming snapshot to the manager. Reader managers are registered as
     *  CatalogSnapshotLifecycleListeners, so they are notified (and refreshed) inside
     *  applyReplicationSnapshot → installSnapshot before the snapshot becomes visible. */
    private void applyCatalogSnapshot(CatalogSnapshot incoming) throws IOException {
        notifyRefreshListenersBefore();
        boolean success = false;
        try {
            catalogSnapshotManager.applyReplicationSnapshot(incoming);
            success = true;
        } finally {
            notifyRefreshListenersAfter(success);
        }
    }

    /** Commits the current catalog snapshot to disk via a synthetic SegmentInfos with serialized CatalogSnapshot in userData. */
    private void commitCatalogSnapshot() throws IOException {
        commitCatalogSnapshot(false);
    }

    private void commitCatalogSnapshot(boolean bumpSICounter) throws IOException {
        try (GatedConditionalCloseable<CatalogSnapshot> snapshotRef = catalogSnapshotManager.acquireSnapshotForCommit()) {
            CatalogSnapshot snapshot = snapshotRef.get();

            Map<String, String> commitData = new HashMap<>();
            commitData.put(CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY, Long.toString(snapshot.getLastWriterGeneration()));
            commitData.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(snapshot.getId()));
            commitData.put(Translog.TRANSLOG_UUID_KEY, translogManager.getTranslogUUID());
            commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpointTracker.getProcessedCheckpoint()));
            commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
            // Required by DataFormatAwareEngine ctor if this replica is later promoted to primary.
            // Replicas don't track auto-id timestamps; -1 matches a fresh primary's startup value.
            commitData.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(-1L));
            // Mirror the commit data onto the snapshot so `lastCommittedSnapshot` reflects the committed userData
            snapshot.setUserData(commitData, true);
            commitData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, snapshot.serializeToString());

            CommitResult commitResult = committer.commit(new CommitInput(commitData.entrySet(), snapshot, bumpSICounter ? SI_COUNTER_INCREMENT : 0));
            if (snapshot instanceof DataformatAwareCatalogSnapshot dfaSnapshot) {
                dfaSnapshot.setLastCommitInfo(commitResult.commitFileName(), commitResult.generation(), commitResult.commitDataFormatVersion());
            }
            snapshotRef.markSuccess();
            lastCommittedSnapshot = snapshot;
        }
        translogManager.syncTranslog();
    }

    // Primary supplies historyUUID via the CATALOG_SNAPSHOT_KEY commit data; we read it from the
    // last-committed snapshot so re-committed / newly-replicated primaries with a fresh UUID
    // propagate correctly. Falls back to the fresh-replica ctor-supplied UUID before the first
    // commit has run.
    @Override
    public String getHistoryUUID() {
        final String fromCommit = lastCommittedSnapshot.getUserData().get(Engine.HISTORY_UUID_KEY);
        if (fromCommit != null) {
            return fromCommit;
        }
        throw new IllegalStateException("commit doesn't contain history uuid");
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        // TODO: Wire up CompletionStatsCache when DFA search integration provides Engine.Searcher support.
        return new CompletionStats();
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
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        ensureOpen();
        return NRTReplicaTranslogOps.index(translogManager, localCheckpointTracker, index);
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        ensureOpen();
        return NRTReplicaTranslogOps.delete(translogManager, localCheckpointTracker, delete);
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        ensureOpen();
        return NRTReplicaTranslogOps.noOp(translogManager, localCheckpointTracker, noOp);
    }

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

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        throw new UnsupportedOperationException("Not supported on DataFormatAwareNRTReplicationEngine");
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        throw new UnsupportedOperationException("Not supported on DataFormatAwareNRTReplicationEngine");
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

    // Replicated segments are immediately visible; processed checkpoint == last refreshed.
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
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public void refresh(String source) throws EngineException {}

    @Override
    public boolean maybeRefresh(String source) {
        return false;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {}

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) {
        flush(force, waitIfOngoing, true);
    }

    public void flush(boolean force, boolean waitIfOngoing, boolean bumpSnapshotGeneration) {
        ensureOpen();
        // Skip flushing for warm indices
        if (engineConfig.getIndexSettings().isWarmIndex()) {
            return;
        }
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (flushLock.tryLock() == false) {
                if (waitIfOngoing == false) {
                    return;
                }
                flushLock.lock();
            }
            try {
                if (bumpSnapshotGeneration) {
                    catalogSnapshotManager.bumpGeneration();
                }
                commitCatalogSnapshot();
            } catch (IOException e) {
                maybeFailEngine("flush", e);
                throw new FlushFailedEngineException(shardId, e);
            } finally {
                flushLock.unlock();
            }
        }
    }

    @Override
    public void flush() {
        flush(false, true);
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

    /** DFA replicas operate on {@link CatalogSnapshot}, not Lucene {@link IndexCommit}.
     *  Use {@link #acquireSafeCatalogSnapshot()} instead. */
    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        throw new UnsupportedOperationException(
            "acquireSafeIndexCommit is not supported on DataFormatAwareNRTReplicationEngine; use acquireSafeCatalogSnapshot"
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
        if (flushFirst) {
            flush(false, true);
        }
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
    public void close() throws IOException {
        if (isClosed.get() == false) {
            try (ReleasableLock ignored = writeLock.acquire()) {
                closeNoLock("api");
            }
        }
        awaitPendingClose();
    }

    private void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock must be held or the engine must be currently failing";
            try {
                if (engineConfig.getIndexSettings().isWarmIndex() == false) {
                    try {
                        // Bump SI counter on final commit to avoid filename collisions on failover.
                        final boolean bumpCounter = engineConfig.getIndexSettings().isRemoteStoreEnabled() == false
                            && engineConfig.getIndexSettings().isAssignedOnRemoteNode() == false;
                        commitCatalogSnapshot(bumpCounter);
                    } catch (IOException e) {
                        // Mark store corrupted unless closing due to engine failure.
                        if (failEngineLock.isHeldByCurrentThread() == false && store.isMarkedCorrupted() == false) {
                            try {
                                store.markStoreCorrupted(e);
                            } catch (IOException ex) {
                                logger.warn("Unable to mark store corrupted", ex);
                            }
                        }
                    }
                }
                List<Closeable> closeables = new ArrayList<>(readerManagers.values());
                closeables.add(catalogSnapshotManager);
                closeables.add(translogManager);
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

    @Override
    public void activateThrottling() {}

    @Override
    public void deactivateThrottling() {}

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void maybePruneDeletes() {
        // No-op: data-format engines do not maintain Lucene-style delete tombstones
    }

    @Override
    public MergeStats getMergeStats() {
        return new MergeStats();
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        // No-op for replica
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return localCheckpointTracker.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        // No-op for replica
    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        final TranslogDeletionPolicy translogDeletionPolicy = translogManager.getDeletionPolicy();
        translogDeletionPolicy.setRetentionAgeInMillis(translogRetentionAge.millis());
        translogDeletionPolicy.setRetentionSizeInBytes(translogRetentionSize.getBytes());
    }

    @Override
    public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
        ensureOpen();
        return catalogSnapshotManager.acquireSnapshot();
    }

    @Override
    public byte[] serializeSnapshotToRemoteMetadata(CatalogSnapshot catalogSnapshot) throws IOException {
        throw new UnsupportedOperationException("Replica engine does not produce upload metadata bytes");
    }

    @Override
    public void ensureOpen() {
        if (isClosed.get()) {
            throw new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
        }
    }

    @Override
    public void finalizeReplication(CatalogSnapshot catalogSnapshot) throws IOException {
        updateCatalogSnapshot(catalogSnapshot);
    }

    // Replica has no indexing engine; prepareIndex is only used for translog replay.
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
        long startTime = System.nanoTime();
        // Skip document parsing — this replica engine only writes to translog.
        // Segments arrive via segment replication, not by indexing parsed documents.
        ParsedDocument doc = new ParsedDocument(null, null, source.id(), null, null, source.source(), source.getMediaType(), null);
        if (docMapper.getMapping() != null) {
            doc.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id()));
        return new Engine.Index(
            uid,
            doc,
            seqNo,
            primaryTerm,
            version,
            versionType,
            origin,
            startTime,
            autoGeneratedIdTimestamp,
            isRetry,
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    // Replica needs Delete for translog replay; the delete is written to translog only.
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
        long startTime = System.nanoTime();
        Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
        return new Engine.Delete(id, uid, seqNo, primaryTerm, version, versionType, origin, startTime, ifSeqNo, ifPrimaryTerm);
    }

    @Override
    public EngineConfig config() {
        return engineConfig;
    }

    /** NRT replica engine — receives segments via replication. */
    @Override
    public boolean isReplicaIndexer() {
        return true;
    }

    @Override
    public CommitStats commitStats() {
        ensureOpen();
        return committer.getCommitStats();
    }

    @Override
    public DocsStats docStats() {
        try (GatedCloseable<CatalogSnapshot> snapshot = catalogSnapshotManager.acquireSnapshot()) {
            long count = snapshot.get().getNumDocs();
            // Total size: sum across all format files — each format contributes distinct bytes on disk.
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
    public PollingIngestStats pollingIngestStats() {
        return null;
    }

    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return Long.MIN_VALUE;
    }

    @Override
    public long getLastWriteNanos() {
        return lastWriteNanos;
    }

    @Override
    public long unreferencedFileCleanUpsPerformed() {
        return 0;
    }

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        // No-op for replica
    }

    @Override
    public void flushAndClose() throws IOException {
        if (isClosed.get() == false) {
            logger.trace("flushAndClose now acquire writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.trace("flushAndClose now acquired writeLock");
                try {
                    logger.debug("flushing shard on close - this might take some time to sync files to disk");
                    try {
                        // TODO we might force a flush in the future since we have the write lock already even though recoveries
                        // are running.
                        flush();
                    } catch (AlreadyClosedException ex) {
                        logger.debug("engine already closed - skipping flushAndClose");
                    }
                } finally {
                    close(); // double close is not a problem
                }
            }
        }
        awaitPendingClose();
    }

    // TODO: check DataFormatAwareImplementation
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

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Fails the engine on corruption, translog tragic events, or already-closed with tragic cause. */
    private void maybeFailEngine(String source, Exception e) {
        if (Lucene.isCorruptionException(e)) {
            failEngine("corrupt file (source: [" + source + "])", e);
        } else if (e instanceof AlreadyClosedException) {
            if (translogManager.getTragicExceptionIfClosed() != null) {
                failEngine("already closed by tragic event on the translog", translogManager.getTragicExceptionIfClosed());
            }
        } else if (e != null && translogManager.getTragicExceptionIfClosed() == e) {
            failEngine(source, e);
        }
    }

    /**
     * Builds filesystem-level {@link FileDeleter}s for every registered data format so that
     * {@link org.opensearch.index.engine.exec.coord.IndexFileDeleter}'s construction-time orphan
     * sweep can physically remove files not referenced by the restored catalog snapshot.
     * <p>
     * Both Lucene and non-default (pluggable) formats are wired. The replica has no
     * {@code IndexWriter}, so Lucene-format secondary files superseded by later replication
     * snapshots must be cleaned through this path too; without it they accumulate forever.
     * The Lucene deleter guards against touching commit-owned files ({@code segments_N},
     * {@code write.lock}) which are managed by {@code Store}/Lucene commit machinery rather
     * than the catalog.
     * <p>
     * Each deleter is a thin wrapper over {@link Files#deleteIfExists(Path)} scoped to the
     * format's on-disk directory — matching the path convention used by
     * {@link org.opensearch.index.engine.exec.coord}.
     */
    // Visible for testing.
    static Map<String, FileDeleter> buildReplicaFileDeleters(ShardPath shardPath, DataFormatRegistry registry, CommitFileManager commitFileManager) {
        Map<String, FileDeleter> deleters = new HashMap<>();
        for (DataFormat format : registry.getRegisteredFormats()) {
            final String formatName = format.name();
            // Match OrphanFileScanner's path resolution: "lucene" files live directly under
            // the shard's index/ dir; every other format has its own subdirectory.
            final Path formatDir = "lucene".equals(formatName) ? shardPath.resolveIndex() : shardPath.getDataPath().resolve(formatName);
            deleters.put(formatName, filesByFormat -> {
                Collection<String> files = filesByFormat.get(formatName);
                if (files == null || files.isEmpty()) {
                    return Map.of();
                }
                Set<String> failed = new HashSet<>();
                for (String name : files) {
                    // Never delete Lucene commit files or write locks via the catalog cleanup
                    // path — these are owned by Store/Lucene commit machinery, not the catalog.
                    // Defence-in-depth against a catalog that wrongly references a commit file.
                    if (commitFileManager.isCommitManagedFile(name)) {
                        continue;
                    }
                    try {
                        Files.deleteIfExists(formatDir.resolve(name));
                    } catch (NoSuchFileException ignored) {
                        // already gone — treat as success
                    } catch (IOException e) {
                        LogManager.getLogger(DataFormatAwareNRTReplicationEngine.class)
                            .warn("Failed to delete file [{}] in format [{}]: {}", name, formatName, e.getMessage());
                        failed.add(name);
                    }
                }
                return failed.isEmpty() ? Map.of() : Map.of(formatName, failed);
            });
        }
        return deleters;
    }

    private TranslogDeletionPolicy getTranslogDeletionPolicy() {
        return NRTReplicaTranslogOps.getTranslogDeletionPolicy(engineConfig);
    }

    /** Notifies internal refresh listeners; called manually since per-format readers lack ReferenceManager hooks. */
    private void notifyRefreshListenersAfter(boolean didRefresh) throws IOException {
        for (ReferenceManager.RefreshListener refreshListener : internalRefreshListeners) {
            refreshListener.afterRefresh(didRefresh);
        }
    }

    private void notifyRefreshListenersBefore() throws IOException {
        for (ReferenceManager.RefreshListener refreshListener : internalRefreshListeners) {
            refreshListener.beforeRefresh();
        }
    }

    /**
     * Catalog-tracked file deleter for the replica. Path resolution is delegated to
     * {@link org.opensearch.index.store.DataFormatAwareStoreDirectory} via {@code store.directory()},
     * which routes each filename to the correct per-format subdirectory based on the file's
     * extension. Commit-managed files ({@code segments_N}, {@code write.lock}) are skipped —
     * those are owned by Store/Lucene commit machinery, not the catalog.
     */
    private FileDeleter buildReplicaFileDeleter() {
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
                    } catch (NoSuchFileException ignored) {
                        // already gone — treat as success
                    } catch (IOException e) {
                        logger.warn("Failed to delete file [{}] in format [{}]: {}", name, formatName, e.getMessage());
                        failed.computeIfAbsent(formatName, k -> new ArrayList<>()).add(name);
                    }
                }
            }
            return failed;
        };
    }
}
