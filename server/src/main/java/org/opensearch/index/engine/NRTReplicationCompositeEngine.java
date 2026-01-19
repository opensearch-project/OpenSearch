/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.engine.exec.coord.CompositeEngineCatalogSnapshot;
import org.opensearch.index.engine.exec.coord.SegmentInfosCatalogSnapshot;
import org.opensearch.index.engine.exec.commit.LuceneCommitEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.plugins.spi.vectorized.DataFormat;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.MAX_SEQ_NO;

/**
 * Engine implementation for replica shards with optimized (multi-format) indices.
 * Combines segment replication behavior (translog-only writes) with multi-format support.
 *
 * Similar to NRTReplicationEngine but for optimized indices using CatalogSnapshot.
 */
public class NRTReplicationCompositeEngine extends CompositeEngine {

    private final ShardId shardId;
    private final Logger logger;
    private final WriteOnlyTranslogManager translogManager;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    private final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());
    private final Lock flushLock = new ReentrantLock();
    private final Map<DataFormat, List<SearchExecEngine<?, ?, ?, ?>>> readEngines = new HashMap<>();

    private volatile long lastReceivedPrimaryGen = SequenceNumbers.NO_OPS_PERFORMED;

    public NRTReplicationCompositeEngine(
        EngineConfig engineConfig,
        MapperService mapperService,
        PluginsService pluginsService,
        IndexSettings indexSettings,
        ShardPath shardPath,
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        TranslogEventListener translogEventListener
    ) {
        super(engineConfig, mapperService, pluginsService, indexSettings, shardPath, localCheckpointTrackerSupplier, translogEventListener);
        this.shardId = engineConfig.getShardId();
        this.logger = Loggers.getLogger(NRTReplicationCompositeEngine.class, shardId);

        store.incRef();
        WriteOnlyTranslogManager translogManagerRef = null;
        CatalogSnapshotManager catalogSnapshotManagerRef = null;
        boolean success = false;

        try {
            // Read last committed segment infos
            final SegmentInfos lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            final Map<String, String> userData = lastCommittedSegmentInfos.getUserData();
            final String translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));

            // Initialize local checkpoint tracker
            final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
            this.localCheckpointTracker = localCheckpointTrackerSupplier.apply(commitInfo.maxSeqNo, commitInfo.localCheckpoint);

            // Register ONLY internal refresh listeners (not external)
            for (ReferenceManager.RefreshListener listener : engineConfig.getInternalRefreshListener()) {
                this.refreshListeners.add(listener);
            }

            // Create write-only translog manager
            TranslogEventListener internalTranslogEventListener = new TranslogEventListener() {
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
            };

            translogManagerRef = new WriteOnlyTranslogManager(
                engineConfig.getTranslogConfig(),
                engineConfig.getPrimaryTermSupplier(),
                engineConfig.getGlobalCheckpointSupplier(),
                new DefaultTranslogDeletionPolicy(
                    engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                    engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
                    engineConfig.getIndexSettings().getTranslogRetentionTotalFiles()
                ),
                shardId,
                readLock,
                this::getLocalCheckpointTracker,
                translogUUID,
                internalTranslogEventListener,
                this,
                engineConfig.getTranslogFactory(),
                engineConfig.getStartedPrimarySupplier(),
                TranslogOperationHelper.create(engineConfig)
            );
            this.translogManager = translogManagerRef;

            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create NRTReplicationCompositeEngine", e);
        } finally {
            if (!success) {
                if (translogManagerRef != null) {
                    try {
                        translogManagerRef.close();
                    } catch (Exception e) {
                        logger.warn("Failed to close translog manager", e);
                    }
                }
                // CatalogSnapshotManager doesn't implement Closeable
                if (isClosed.get() == false) {
                    store.decRef();
                }
            }
        }
    }

    // Translog-only operations (from NRTReplicationEngine)

    @Override
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        ensureOpen();
        Engine.IndexResult indexResult = new Engine.IndexResult(index.version(), index.primaryTerm(), index.seqNo(), false);
        final Translog.Location location = translogManager.add(new Translog.Index(index, indexResult));
        indexResult.setTranslogLocation(location);
        indexResult.setTook(System.nanoTime() - index.startTime());
        indexResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(index.seqNo());
        return indexResult;
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        ensureOpen();
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true);
        final Translog.Location location = translogManager.add(new Translog.Delete(delete, deleteResult));
        deleteResult.setTranslogLocation(location);
        deleteResult.setTook(System.nanoTime() - delete.startTime());
        deleteResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(delete.seqNo());
        return deleteResult;
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        ensureOpen();
        Engine.NoOpResult noOpResult = new Engine.NoOpResult(noOp.primaryTerm(), noOp.seqNo());
        final Translog.Location location = translogManager.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
        noOpResult.setTranslogLocation(location);
        noOpResult.setTook(System.nanoTime() - noOp.startTime());
        noOpResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(noOp.seqNo());
        return noOpResult;
    }

    /**
     * Updates segments from primary using CatalogSnapshot.
     * CRITICAL: Invokes refresh listeners to update replication checkpoint.
     */
    public synchronized void updateSegments(final CatalogSnapshot catalogSnapshot) throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();

            final long maxSeqNo = Long.parseLong(catalogSnapshot.getUserData().get(MAX_SEQ_NO));
            final long incomingGeneration = catalogSnapshot.getGeneration();

            // For replicas, catalog is managed externally - just track the generation
            // The catalog snapshot is already applied by IndexShard.finalizeReplication()

            // Invoke refresh listeners
            invokeRefreshListeners(true);

            // Flush if generation changed
            if (incomingGeneration != this.lastReceivedPrimaryGen) {
                flush(false, true);
                translogManager.getDeletionPolicy().setLocalCheckpointOfSafeCommit(maxSeqNo);
                translogManager.rollTranslogGeneration();
            }

            this.lastReceivedPrimaryGen = incomingGeneration;
            localCheckpointTracker.fastForwardProcessedSeqNo(maxSeqNo);
        }
    }

    public void finalizeReplication(CatalogSnapshot catalogSnapshot, ShardPath shardPath) throws IOException {
        catalogSnapshotManager.applyReplicationChanges(catalogSnapshot, shardPath);

        if (catalogSnapshot != null) {
            long maxGenerationInSnapshot = catalogSnapshot.getLastWriterGeneration();
            engine.updateWriterGenerationIfNeeded(maxGenerationInSnapshot);
        }

        updateSearchEngine();
        updateSegments(catalogSnapshot);
    }

    private void invokeRefreshListeners(boolean didRefresh) {
        // Call beforeRefresh
        refreshListeners.forEach(listener -> {
            try {
                listener.beforeRefresh();
            } catch (IOException e) {
                logger.error("refresh listener beforeRefresh failed", e);
                throw new RuntimeException(e);
            }
        });

        // Call afterRefresh - ReplicationCheckpointUpdater runs here
        refreshListeners.forEach(listener -> {
            try {
                listener.afterRefresh(didRefresh);
            } catch (IOException e) {
                logger.error("refresh listener afterRefresh failed", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void refresh(String source) throws EngineException {
        // No-op for replicas
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        // No-op
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        if (engineConfig.getIndexSettings().isWarmIndex()) {
            return;
        }
        try (final ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (flushLock.tryLock() == false) {
                if (waitIfOngoing == false) {
                    return;
                }
                flushLock.lock();
            }
            try {
                // For replicas, flush is minimal - just update translog deletion policy
                translogManager.getDeletionPolicy().setLocalCheckpointOfSafeCommit(
                    localCheckpointTracker.getProcessedCheckpoint()
                );
            } catch (Exception e) {
                maybeFailEngine("flush", e);
                throw new FlushFailedEngineException(shardId, e);
            } finally {
                flushLock.unlock();
            }
        }
    }

    // Checkpoint methods

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

    // Metadata methods

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    // Throttling methods

    @Override
    public void activateThrottling() {
        // No-op
    }

    @Override
    public void deactivateThrottling() {
        // No-op
    }

    // Unsupported operations for replicas

    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {
        // No-op - replicas don't merge
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return new SafeCommitInfo(localCheckpointTracker.getProcessedCheckpoint(), 0);
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                logger.debug("closing NRTReplicationCompositeEngine, reason: {}", reason);

                // Close translog manager
                if (translogManager != null) {
                    translogManager.close();
                }
                store.decRef();
            } catch (Exception e) {
                logger.error("failed to close NRTReplicationCompositeEngine", e);
            }
        }
        super.closeNoLock(reason, closedLatch);
    }

    public LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        // No-op
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return SequenceNumbers.UNASSIGNED_SEQ_NO;
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long seqNo) {
        // No-op for replicas
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
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
    }
}
