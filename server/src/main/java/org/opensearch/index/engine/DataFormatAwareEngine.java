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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Booleans;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.concurrent.GatedConditionalCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.queue.DefaultLockableHolder;
import org.opensearch.common.queue.LockablePool;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.AppendOnlyIndexOperationRetryException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.RowIdAwareWriter;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.engine.dataformat.merge.DataFormatAwareMergePolicy;
import org.opensearch.index.engine.dataformat.merge.MergeFailedEngineException;
import org.opensearch.index.engine.dataformat.merge.MergeHandler;
import org.opensearch.index.engine.dataformat.merge.MergeScheduler;
import org.opensearch.index.engine.dataformat.merge.OneMerge;
import org.opensearch.index.engine.exec.CatalogSnapshotLifecycleListener;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.DocumentLookupSupport;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.PrimaryTermFieldType;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.RemoteStoreRefreshListener;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.plugins.DocumentLookupProvider;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

import static org.opensearch.index.engine.Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * An {@link Indexer} implementation that delegates to an {@link IndexingExecutionEngine}.
 * This engine manages the full lifecycle of indexing,
 * deleting, and searching documents on a single shard by coordinating between the
 * underlying data format engine and the translog.
 * <p>
 * Mirrors the responsibilities of {@link InternalEngine} but is decoupled from Lucene:
 * <ul>
 *   <li>Sequence number generation and local checkpoint tracking</li>
 *   <li>Translog management for durability and recovery</li>
 *   <li>Refresh to make documents searchable via catalog snapshots</li>
 *   <li>Flush to commit catalog snapshots and trim the translog</li>
 *   <li>Throttling when merges fall behind or indexing buffer is full</li>
 *   <li>Soft-deletes policy for peer-recovery retention</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatAwareEngine implements Indexer {

    private final Logger logger;
    private final EngineConfig engineConfig;
    private final ShardId shardId;
    private final Store store;

    private final IndexingExecutionEngine indexingExecutionEngine;
    private final IndexingStrategyPlanner indexingStrategyPlanner;
    private final DocumentCountTracker documentCountTracker;
    private final AtomicLong pendingRowCount = new AtomicLong();
    private final LockablePool<DefaultLockableHolder<Writer<?>>> writerPool;
    private final AtomicLong writerGenerationCounter;

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;

    private final CatalogSnapshotManager catalogSnapshotManager;
    private final Committer committer;
    private final List<ReferenceManager.RefreshListener> refreshListeners;
    private final CatalogSnapshotStatsCache statsCache;

    // Translog for durability and recovery
    private final TranslogManager translogManager;

    // Sequence number tracking
    private final LocalCheckpointTracker localCheckpointTracker;
    private final AtomicLong maxSeqNoOfUpdatesOrDeletes;

    // Wall-clock time (ms) of the last version-map delete-tombstone prune; used to throttle maybePruneDeletes().
    protected volatile long lastDeleteVersionPruneTimeMSec;

    // Throttling
    private final IndexingThrottler throttle;
    private final AtomicInteger throttleRequestCount = new AtomicInteger();

    @Nullable
    private final DocumentLookupProvider documentLookupProvider;
    private final DocumentMetadataResolver documentMetadataResolver;
    // Shared get-by-id flow (parquet lookup + read version-conflict checks), common to all DataFormatAware* engines.
    private final DocumentLookupSupport documentLookup;

    // Timestamps and seq-no markers
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final AtomicLong maxSeenAutoIdTimestamp = new AtomicLong(-1);
    private volatile long lastWriteNanos = System.nanoTime();

    // Lifecycle
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final SetOnce<Exception> failedEngine = new SetOnce<>();
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    // Concurrency locks
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    private final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());
    private final Lock flushLock = new ReentrantLock();
    private final ReentrantLock refreshLock = new ReentrantLock();
    private final ReentrantLock failEngineLock = new ReentrantLock();

    // Refresh tracker
    private final LastRefreshedCheckpointListener lastRefreshedCheckpointListener;

    // Merge
    private final MergeScheduler mergeScheduler;

    /**
     * When {@code true}, tiering-sensitive operations are blocked: primary index ops, merges,
     * refresh, flush, and merge-result catalog commits (each with documented bypasses below).
     * Set in three places: the constructor (freeze-on-open when a shard opens mid-tiering),
     * {@link #onSettingsChanged} (when INDEX_TIERING_STATE transitions to HOT_TO_WARM),
     * and {@link #freezeForTiering()} (explicitly, from the prepare action). Cleared when the state
     * returns to HOT (tiering cancel or prepare failure).
     */
    private final AtomicBoolean frozenForTiering = new AtomicBoolean(false);

    // TODO Refactor these flush managing activities into FlushManager.

    // Segments flushed inline outside refresh — by indexing threads (preIndex cooperative drain)
    // or by failure-handling retirement (retireWriterIfNeeded). Drained by the next refresh().
    private final ConcurrentLinkedQueue<Segment> pendingSegments = new ConcurrentLinkedQueue<>();

    // Writers that have been flushed inline but not yet closed. Their Lucene temp
    // directories must remain on disk until refresh incorporates them via addIndexes.
    // Closed after refresh completes.
    private final ConcurrentLinkedQueue<Writer<?>> pendingWritersToClose = new ConcurrentLinkedQueue<>();

    // Shared queue of writers pending flush. Populated by refresh (checkoutAll),
    // drained cooperatively by both the refresh thread and write threads (backpressure).
    private final ConcurrentLinkedQueue<Writer<?>> flushQueue = new ConcurrentLinkedQueue<>();

    // Latch for the current refresh cycle — decremented by any thread that flushes a writer.
    // Refresh thread awaits this before proceeding with catalog commit.
    private volatile CountDownLatch activeFlushLatch;
    private final LiveVersionMap versionMap;
    private final CounterMetric numVersionLookups = new CounterMetric();
    private final CounterMetric numIndexVersionsLookups = new CounterMetric();

    /**
     * System property to enable or disable pluggable dataformat merge operations.
     * Set to "true" to enable merges (e.g., {@code -Dopensearch.pluggable.dataformat.merge.enabled=true}).
     * Defaults to "false" (merges disabled) as the merge implementations are not yet complete
     * for all data formats.
     * <p>
     * TODO: Remove this flag once merge implementations are complete for all data formats.
     */
    static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

    @Nullable
    private final String historyUUID;

    /**
     * Constructs a DataFormatBasedEngine.
     *
     * @param engineConfig the engine configuration
     */
    public DataFormatAwareEngine(EngineConfig engineConfig) {
        // DataFormatAwareEngine is the writable primary-side engine. Read-only replicas
        // (segment-rep) and warm-tier shards must use a read-only engine instead — fail
        // fast so a misconfiguration surfaces before any indexing or recovery.
        if (engineConfig.isReadOnlyReplica() || engineConfig.getIndexSettings().isWarmIndex()) {
            throw new IllegalStateException(
                "DataFormatAwareEngine cannot be used on a read-only shard ["
                    + engineConfig.getShardId()
                    + "] (readOnlyReplica="
                    + engineConfig.isReadOnlyReplica()
                    + ", warm="
                    + engineConfig.getIndexSettings().isWarmIndex()
                    + "); use a segment-consuming or read-only engine"
            );
        }
        this.logger = Loggers.getLogger(DataFormatAwareEngine.class, engineConfig.getShardId());
        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();
        this.throttle = new IndexingThrottler();
        this.documentLookupProvider = engineConfig.getDocumentLookupProvider();
        this.documentMetadataResolver = engineConfig.getDocumentMetadataResolver() != null
            ? engineConfig.getDocumentMetadataResolver()
            : DocumentMetadataResolver.NOOP;
        this.documentLookup = new DocumentLookupSupport(shardId, this.documentLookupProvider, this.documentMetadataResolver);
        this.versionMap = new LiveVersionMap();

        List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
        refreshListeners.add(versionMap);
        if (engineConfig.getInternalRefreshListener() != null) {
            refreshListeners.addAll(engineConfig.getInternalRefreshListener());
        }
        // We don't segregate internal/external here since NRT is anyhow invoked on internal refresh which makes
        // data available to read on internal refreshes on replica.
        if (engineConfig.getExternalRefreshListener() != null) {
            refreshListeners.addAll(engineConfig.getExternalRefreshListener());
        }
        this.refreshListeners = refreshListeners;

        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            updateAutoIdTimestamp(Long.MAX_VALUE, true);
        }

        boolean success = false;
        TranslogManager translogManagerRef = null;

        try {
            store.incRef();

            // 1. Create Committer (uses translogPath for safe bootstrap trimming)
            // Encapsulate refreshLock access behind a pre-merge-commit hook: committer-owned
            // writers (e.g. Lucene MergeIndexWriter) invoke the hook on the merge thread
            // immediately before the merged segment becomes visible. When Lucene participates
            // in a merge, its committer wires the hook into a MergedSegmentWarmer that fires
            // between mergeMiddle and commitMerge — the IndexWriter monitor is not held there,
            // so acquiring refreshLock via the hook establishes the same refreshLock → IW
            // monitor ordering that the refresh path uses and avoids lock inversion. Ownership
            // then transfers to applyMergeChanges, which releases the lock after the catalog
            // is updated. For merges that do not invoke the hook — pure Parquet merges, or
            // Lucene merges that skip because the shared writer has no matching segments —
            // applyMergeChanges acquires refreshLock itself. Either way, applyMergeChanges
            // releases the lock before returning.
            this.committer = engineConfig.getCommitterFactory().getCommitter(new CommitterConfig(engineConfig, () -> {
                if (refreshLock.isHeldByCurrentThread() == false) {
                    refreshLock.lock();
                }
            }));

            // 2. Read translogUUID and history UUID from last committed data
            final Map<String, String> userData = committer.getLastCommittedData();
            String translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));
            this.historyUUID = userData.get(Engine.HISTORY_UUID_KEY);
            updateAutoIdTimestamp(Long.parseLong(userData.get(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID)), true);

            // 3. Create TranslogManager
            final TranslogDeletionPolicy translogDeletionPolicy = getTranslogDeletionPolicy();
            final TranslogEventListener translogEventListener = createInternalTranslogEventListener();
            translogManagerRef = createTranslogManager(translogUUID, translogDeletionPolicy, translogEventListener);
            this.translogManager = translogManagerRef;

            // 4. Initialize local checkpoint tracker
            this.localCheckpointTracker = createLocalCheckpointTracker(LocalCheckpointTracker::new);
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
            maxSeqNoOfUpdatesOrDeletes = new AtomicLong(
                SequenceNumbers.max(localCheckpointTracker.getMaxSeqNo(), translogManager.getMaxSeqNo())
            );

            // 5. Create IndexingExecutionEngine and ReaderManagers
            DataFormatRegistry registry = engineConfig.getDataFormatRegistry();
            this.indexingExecutionEngine = registry.getIndexingEngine(
                new IndexingEngineConfig(
                    committer,
                    config().getMapperService(),
                    config().getIndexSettings(),
                    config().getStore(),
                    registry,
                    config().getChecksumStrategies()
                ),
                registry.format(config().getIndexSettings().pluggableDataFormat())
            );

            long maxGenFromCommit = 0L;
            try {
                List<CatalogSnapshot> initSnapshots = committer.listCommittedSnapshots();
                if (initSnapshots.isEmpty() == false) {
                    for (Segment seg : initSnapshots.getLast().getSegments()) {
                        maxGenFromCommit = Math.max(maxGenFromCommit, seg.generation());
                    }
                }
            } catch (IOException e) {
                // Fall back to 0 on error
            }
            this.writerGenerationCounter = new AtomicLong(maxGenFromCommit);
            this.writerPool = new LockablePool<>(() -> {
                long gen = writerGenerationCounter.incrementAndGet();
                assert gen > 0 : "writer generation must be positive but was: " + gen;
                Writer<?> writer = indexingExecutionEngine.createWriter(new WriterConfig(gen));
                return DefaultLockableHolder.of(new RowIdAwareWriter<>(writer));
            }, LinkedList::new, Runtime.getRuntime().availableProcessors());
            // Create Reader managers
            // We will pass IndexStoreProvider to this, which would contain store
            // and any index specific attributes useful for reads.
            this.readerManagers = indexingExecutionEngine.buildReaderManager(
                new ReaderManagerConfig(
                    Optional.ofNullable(indexingExecutionEngine.getProvider()),
                    indexingExecutionEngine.getDataFormat(),
                    registry,
                    store.shardPath(),
                    store.getDataformatAwareStoreHandles(),
                    engineConfig.getIndexSettings()
                )
            );

            // 6. Create CombinedCatalogSnapshotDeletionPolicy
            CombinedCatalogSnapshotDeletionPolicy combinedPolicy = new CombinedCatalogSnapshotDeletionPolicy(
                logger,
                translogDeletionPolicy,
                translogManager::getLastSyncedGlobalCheckpoint
            );

            // 7. Create CatalogSnapshotManager (fully wired)
            FileDeleter fileDeleter = indexingExecutionEngine::deleteFiles;
            Map<String, FilesListener> filesListeners = new HashMap<>();
            List<CatalogSnapshotLifecycleListener> snapshotListeners = new ArrayList<>();
            for (Map.Entry<DataFormat, EngineReaderManager<?>> entry : readerManagers.entrySet()) {
                filesListeners.put(entry.getKey().name(), entry.getValue());
                snapshotListeners.add(entry.getValue());
            }
            List<CatalogSnapshot> committedSnapshots = committer.listCommittedSnapshots();
            this.catalogSnapshotManager = new CatalogSnapshotManager(
                committedSnapshots,
                combinedPolicy,
                fileDeleter,
                filesListeners,
                snapshotListeners,
                store.shardPath(),
                committer
            );
            // Bump catalog generation on engine open so uploads from this primary do not collide
            // with a prior primary's uploads for the same shard. See method Javadoc for rationale.
            this.catalogSnapshotManager.bumpGeneration();

            this.lastRefreshedCheckpointListener = new LastRefreshedCheckpointListener(localCheckpointTracker);
            this.refreshListeners.add(this.lastRefreshedCheckpointListener);

            // Create and register stats cache as refresh listener
            this.statsCache = new CatalogSnapshotStatsCache(catalogSnapshotManager, store, engineConfig, () -> {
                try {
                    return committer.getLastCommittedData();
                } catch (IOException e) {
                    logger.warn("Failed to get last committed data for stats cache", e);
                    return Collections.emptyMap();
                }
            }, logger);
            this.refreshListeners.add(this.statsCache);
            this.documentCountTracker = new DocumentCountTracker(shardId, () -> {
                // First get active writes as active writes are only reduced after catalog snapshot refresh
                // This prevents under-accounting
                long docsIngested = pendingRowCount.get();
                try (var cs = catalogSnapshotManager.acquireSnapshot()) {
                    docsIngested += cs.get().getNumDocs();
                }
                return docsIngested;
            }, indexingExecutionEngine.maxIndexableDocs());
            this.indexingStrategyPlanner = new IndexingStrategyPlanner(
                engineConfig.getIndexSettings(),
                engineConfig.getShardId(),
                this.versionMap,
                maxUnsafeAutoIdTimestamp::get,
                () -> 0L,
                localCheckpointTracker::getProcessedCheckpoint,
                this::hasBeenProcessedBefore,
                op -> OpVsEngineDocStatus.OP_NEWER,
                this::resolveDocVersion,
                this::updateAutoIdTimestamp,
                documentCountTracker::tryAcquireInFlightDocs
            );
            // All critical engine components must be initialized before the engine is considered ready
            assert translogManager != null : "translog manager must be initialized";
            assert localCheckpointTracker != null : "local checkpoint tracker must be initialized";
            assert catalogSnapshotManager != null : "catalog snapshot manager must be initialized";
            assert indexingExecutionEngine != null : "indexing execution engine must be initialized";
            assert committer != null : "committer must be initialized";
            assert writerPool != null : "writer pool must be initialized";

            DataFormatAwareMergePolicy dataFormatAwareMergePolicy = new DataFormatAwareMergePolicy(
                engineConfig.getIndexSettings().getMergePolicy(true),
                shardId
            );

            // Merge
            MergeHandler mergeHandler = new MergeHandler(
                this::acquireSnapshot,
                indexingExecutionEngine.getMerger(),
                shardId,
                dataFormatAwareMergePolicy,
                dataFormatAwareMergePolicy,
                () -> {
                    long gen = writerGenerationCounter.incrementAndGet();
                    assert gen > 0 : "merge generation must be positive but was: " + gen;
                    return gen;
                }
            );

            // Restore version map and checkpoint tracker after recovery.
            restoreVersionMapAndCheckpointTracker();

            // Merge failure cleanup: cleans up unreferenced files and acts as a safety net
            // for refreshLock. The preMergeCommitHook acquires refreshLock on the merge thread;
            // if the merge fails before applyMergeChanges can release it, this callback ensures
            // the lock is not permanently held.
            this.mergeScheduler = new MergeScheduler(mergeHandler, this::applyMergeChanges, () -> {
                try {
                    catalogSnapshotManager.incrementUnreferencedFileCleanUps();
                } finally {
                    if (refreshLock.isHeldByCurrentThread()) {
                        logger.debug("releasing refreshLock held after merge failure (safety-net unlock)");
                        refreshLock.unlock();
                    }
                }
            },
                this::activateThrottling,
                this::deactivateThrottling,
                shardId,
                engineConfig.getIndexSettings(),
                engineConfig.getThreadPool()
            );
            success = true;
            logger.trace("created new DataFormatBasedEngine");
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(translogManagerRef);
                if (isClosed.get() == false) {
                    store.decRef();
                }
            }
        }
    }

    private LocalCheckpointTracker createLocalCheckpointTracker(BiFunction<Long, Long, LocalCheckpointTracker> supplier)
        throws IOException {
        final SequenceNumbers.CommitInfo seqNoStats = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
            store.readLastCommittedSegmentsInfo().getUserData().entrySet()
        );
        logger.trace("recovered max_seq_no [{}] and local_checkpoint [{}]", seqNoStats.maxSeqNo, seqNoStats.localCheckpoint);
        return supplier.apply(seqNoStats.maxSeqNo, seqNoStats.localCheckpoint);
    }

    private TranslogEventListener createInternalTranslogEventListener() {
        return new TranslogEventListener() {
            @Override
            public void onAfterTranslogSync() {
                try {
                    // TODO: Handle file deletion policy
                    translogManager.trimUnreferencedReaders();
                } catch (IOException ex) {
                    throw new TranslogException(shardId, "failed to trim translog on sync", ex);
                }
            }

            @Override
            public void onAfterTranslogRecovery() {
                flush(false, true);
                translogManager.trimUnreferencedTranslogFiles();
            }

            @Override
            public void onFailure(String reason, Exception ex) {
                if (ex instanceof AlreadyClosedException) {
                    failOnTragicEvent((AlreadyClosedException) ex);
                } else {
                    failEngine(reason, ex);
                }
            }
        };
    }

    private TranslogManager createTranslogManager(
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        TranslogEventListener translogEventListener
    ) throws IOException {
        return new InternalTranslogManager(
            engineConfig.getTranslogConfig(),
            engineConfig.getPrimaryTermSupplier(),
            engineConfig.getGlobalCheckpointSupplier(),
            deletionPolicy,
            shardId,
            readLock,
            () -> localCheckpointTracker,
            translogUUID,
            translogEventListener,
            this::ensureOpen,
            engineConfig.getTranslogFactory(),
            engineConfig.getStartedPrimarySupplier(),
            TranslogOperationHelper.create(engineConfig)
        );
    }

    private TranslogDeletionPolicy getTranslogDeletionPolicy() {
        TranslogDeletionPolicy custom = null;
        if (engineConfig.getCustomTranslogDeletionPolicyFactory() != null) {
            custom = engineConfig.getCustomTranslogDeletionPolicyFactory()
                .create(engineConfig.getIndexSettings(), engineConfig.retentionLeasesSupplier());
        }
        return Objects.requireNonNullElseGet(
            custom,
            () -> new DefaultTranslogDeletionPolicy(
                engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
                engineConfig.getIndexSettings().getTranslogRetentionTotalFiles()
            )
        );
    }

    /**
     * Indexes a document into the engine. Handles sequence number assignment for primary
     * operations, throttling, translog recording, and local checkpoint tracking.
     * <p>
     * For primary operations, the indexing strategy planner determines whether to execute
     * the operation or return an early result (e.g., for version conflicts). For replica
     * operations, the sequence number is marked as seen and the operation proceeds directly.
     *
     * @param index the index operation containing the parsed document, version, and origin
     * @return the index result with sequence number, version, and translog location
     * @throws IOException if writing to the engine or translog fails
     */
    @Override
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        if (isFrozenForTiering() && index.origin() == Engine.Operation.Origin.PRIMARY) {
            throw new IllegalStateException("Engine is frozen for tiering — index operations blocked");
        }

        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();
        // DataFormatAwareEngine is the primary-side engine in a segment-replication cluster.
        // Replicas use a separate engine (analogous to NRTReplicationEngine) that consumes segments.
        assert (index.origin() == Engine.Operation.Origin.PRIMARY
            || index.origin() == Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY
            || index.origin() == Engine.Operation.Origin.LOCAL_RESET)
            : "DataFormatAwareEngine only supports PRIMARY, LOCAL_TRANSLOG_RECOVERY, or LOCAL_RESET origins but got: " + index.origin();
        final boolean doThrottle = index.origin().isRecovery() == false;
        int rows = 0;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            try (
                Releasable ignored = versionMap.acquireLock(index.uid().bytes());
                Releasable indexThrottle = doThrottle ? throttle.acquireThrottle() : () -> {}
            ) {
                lastWriteNanos = index.startTime();
                final IndexingStrategy plan;
                if (index.origin() == Engine.Operation.Origin.PRIMARY) {
                    plan = indexingStrategyPlanner.planOperationAsPrimary(index);
                } else {
                    plan = indexingStrategyPlanner.planOperationAsNonPrimary(index);
                }
                rows = plan.reservedDocs;
                final Engine.IndexResult indexResult;
                if (plan.earlyResultOnPreFlightError.isPresent()) {
                    assert index.origin() == Engine.Operation.Origin.PRIMARY : index.origin();
                    indexResult = (Engine.IndexResult) plan.earlyResultOnPreFlightError.get();
                    assert indexResult.getResultType() == Engine.Result.Type.FAILURE : indexResult.getResultType();
                } else {
                    if (index.origin() == Engine.Operation.Origin.PRIMARY) {
                        index = new Engine.Index(
                            index.uid(),
                            index.parsedDoc(),
                            generateSeqNoForOperationOnPrimary(index),
                            index.primaryTerm(),
                            index.version(),
                            index.versionType(),
                            index.origin(),
                            index.startTime(),
                            index.getAutoGeneratedIdTimestamp(),
                            index.isRetry(),
                            index.getIfSeqNo(),
                            index.getIfPrimaryTerm()
                        );
                    } else {
                        markSeqNoAsSeen(index.seqNo());
                        // Replica and recovery operations must arrive with a pre-assigned sequence number
                        assert index.seqNo() >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + index.origin();
                    }

                    assert index.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();

                    if (plan.executeOpOnEngine) {
                        logger.trace(
                            "Indexing doc id=[{}] seqNo=[{}] primaryTerm=[{}] — writing to engine",
                            index.id(),
                            index.seqNo(),
                            index.primaryTerm()
                        );
                        preIndex();
                        indexResult = indexIntoEngine(index, plan);
                    } else {
                        indexResult = new Engine.IndexResult(
                            plan.version,
                            index.primaryTerm(),
                            index.seqNo(),
                            plan.currentNotFoundOrDeleted
                        );
                    }
                }
                return indexResult;
            } finally {
                if (rows > 0) {
                    documentCountTracker.releaseInFlightDocs(rows);
                }
            }
        } catch (RuntimeException | IOException e) {
            maybeFailEngine("index id[" + index.id() + "] origin[" + index.origin() + "]", e);
            throw e;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Engine.IndexResult indexIntoEngine(Engine.Index index, IndexingStrategy plan) throws IOException {
        Engine.IndexResult indexResult;

        assert index.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();
        assert index.primaryTerm() > 0 : "primary term must be positive but was: " + index.primaryTerm();
        DefaultLockableHolder<Writer<?>> lockedWriter = null;
        boolean writerCheckedOut = false;
        long mappingVersion = currentMappingVersion();
        try {
            lockedWriter = writerPool.getAndLock(h -> {
                Writer<?> w = h.get();
                if (w.state() != WriterState.ACTIVE) return false;
                return w.isSchemaMutable() || w.mappingVersion() >= mappingVersion;
            });
            Writer currentWriter = lockedWriter.get();
            currentWriter.updateMappingVersion(mappingVersion);
            // Writer pool must never return null — it creates on demand via the supplier
            assert index.seqNo() >= 0 : "seqNo must be assigned before writing but was: " + index.seqNo();
            assert index.primaryTerm() > 0 : "primaryTerm must be positive but was: " + index.primaryTerm();
            index.parsedDoc().getDocumentInput().addField(engineConfig.getMapperService().fieldType(VersionFieldMapper.NAME), plan.version);
            index.parsedDoc().getDocumentInput().addField(engineConfig.getMapperService().fieldType(SeqNoFieldMapper.NAME), index.seqNo());
            index.parsedDoc().getDocumentInput().addField(PrimaryTermFieldType.INSTANCE, index.primaryTerm());

            WriteResult result = currentWriter.addDoc(index.parsedDoc().getDocumentInput());

            if (result instanceof WriteResult.Success) {
                indexResult = new Engine.IndexResult(plan.version, index.primaryTerm(), index.seqNo(), true);
                assert indexResult.getSeqNo() == index.seqNo() : "IndexResult seq no ["
                    + indexResult.getSeqNo()
                    + "] must match operation seq no ["
                    + index.seqNo()
                    + "]";
                pendingRowCount.incrementAndGet();
            } else {
                WriteResult.Failure f = (WriteResult.Failure) result;
                try {
                    writerCheckedOut = retireWriterIfNeeded(lockedWriter);
                } catch (IllegalStateException bufferLoss) {
                    // Acked docs can't reach the catalog (writer in an unreconcilable state, or
                    // flush-on-retire threw). Fail the engine so recovery replays the translog.
                    writerCheckedOut = true;
                    failEngine("writer retirement could not preserve buffered acked docs", bufferLoss);
                }
                indexResult = new Engine.IndexResult(f.cause(), plan.version, index.primaryTerm(), index.seqNo());
            }
        } catch (Exception e) {
            // A throw from addDoc is unmodeled. Known per-doc rejections must come back as
            // WriteResult.Failure with the writer's state already reconciled (ACTIVE after a
            // self-rollback, or PENDING_ROLLBACK awaiting CompositeWriter rollback). Anything thrown
            // means we don't know the writer's buffer state and cannot safely retire it —
            // fail the engine and let recovery replay the translog.
            failEngine("uncaught exception during indexing id[" + index.id() + "] seq#[" + index.seqNo() + "]", e);
            throw e;
        } finally {
            if (lockedWriter != null) {
                // Invariant: writerCheckedOut ⇒ writer no longer registered in the pool.
                assert writerCheckedOut == false || writerPool.isRegistered(lockedWriter) == false
                    : "writer claimed checked-out but still registered in pool";
                if (writerPool.isRegistered(lockedWriter)) {
                    writerPool.releaseAndUnlock(lockedWriter);
                }
            }
        }

        if (index.origin().isFromTranslog() == false) {
            final Translog.Location location;
            if (indexResult.getResultType() == Engine.Result.Type.SUCCESS) {
                location = translogManager.add(new Translog.Index(index, indexResult));
                versionMap.maybePutIndexUnderLock(
                    index.uid().bytes(),
                    new IndexVersionValue(location, indexResult.getVersion(), index.seqNo(), index.primaryTerm())
                );
            } else if (indexResult.getSeqNo() != UNASSIGNED_SEQ_NO
                && indexResult.getFailure() != null
                && !(indexResult.getFailure() instanceof AppendOnlyIndexOperationRetryException)) {
                    final Engine.NoOp noOp = new Engine.NoOp(
                        indexResult.getSeqNo(),
                        index.primaryTerm(),
                        index.origin(),
                        index.startTime(),
                        indexResult.getFailure().toString()
                    );
                    location = translogManager.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
                } else {
                    location = null;
                }
            indexResult.setTranslogLocation(location);
        }
        // Non-translog-origin successful operations must be recorded in the translog for durability
        assert index.origin().isFromTranslog()
            || indexResult.getResultType() != Engine.Result.Type.SUCCESS
            || indexResult.getTranslogLocation() != null : "successful non-translog-origin op must have a translog location";
        // Translog-origin operations must NOT be written back to the translog (would cause duplicates)
        assert index.origin().isFromTranslog() == false || indexResult.getTranslogLocation() == null
            : "translog-origin op should not have a translog location";

        // Track the sequence number
        assert indexResult.getSeqNo() >= 0 : "indexResult must have assigned seqNo but was: " + indexResult.getSeqNo();
        localCheckpointTracker.markSeqNoAsProcessed(indexResult.getSeqNo());
        if (indexResult.getTranslogLocation() == null) {
            localCheckpointTracker.markSeqNoAsPersisted(indexResult.getSeqNo());
        }

        indexResult.setTook(System.nanoTime() - index.startTime());
        indexResult.freeze();
        return indexResult;
    }

    /**
     * Not supported — delete operations are not implemented for data-format-aware engines.
     *
     * @throws UnsupportedEncodingException always
     */
    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        throw new UnsupportedEncodingException("delete operation not supported.");
    }

    /**
     * Records a no-op for the given seqNo. Used for failed indexing attempts (so the failed
     * seqNo is durably recorded), translog replay, and {@link #fillSeqNoGaps} after a
     * primary-term bump.
     *
     * <p>Unlike {@code InternalEngine.noOp}, this implementation does not write a tombstone
     * document — DataFormatAwareEngine has no soft-delete tracking. The seqNo is marked as
     * seen and processed so the local checkpoint advances, and a {@link Translog.NoOp} is
     * appended when the op originated outside the translog itself.
     */
    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return innerNoOp(noOp);
        }
    }

    /**
     * Lock-free no-op: marks the seqNo as seen + processed and (when not from translog)
     * appends a Translog.NoOp. No tombstone is written — DataFormatAwareEngine has no
     * soft-delete tracking. Caller must hold either the readLock or the writeLock.
     */
    private Engine.NoOpResult innerNoOp(Engine.NoOp noOp) throws IOException {
        assert rwl.getReadHoldCount() > 0 || rwl.isWriteLockedByCurrentThread() : "innerNoOp requires a read or write lock";
        assert noOp.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "noOp must have an assigned seqNo: " + noOp;
        markSeqNoAsSeen(noOp.seqNo());
        Engine.NoOpResult result = new Engine.NoOpResult(noOp.primaryTerm(), noOp.seqNo());
        if (noOp.origin().isFromTranslog() == false) {
            Translog.Location location = translogManager.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
            result.setTranslogLocation(location);
        }
        localCheckpointTracker.markSeqNoAsProcessed(noOp.seqNo());
        if (result.getTranslogLocation() == null) {
            localCheckpointTracker.markSeqNoAsPersisted(noOp.seqNo());
        }
        result.setTook(System.nanoTime() - noOp.startTime());
        result.freeze();
        return result;
    }

    /**
     * Parses the source document using the document mapper and creates an {@link Engine.Index}
     * operation. The parsed document's {@link org.opensearch.index.engine.dataformat.DocumentInput}
     * is created via the indexing execution engine's {@code newDocumentInput()} method.
     *
     * @param docMapper                the document mapper for parsing
     * @param source                   the raw source to parse
     * @param seqNo                    the sequence number ({@code UNASSIGNED_SEQ_NO} for primary)
     * @param primaryTerm              the primary term
     * @param version                  the expected version
     * @param versionType              the version type
     * @param origin                   the operation origin (PRIMARY, REPLICA, etc.)
     * @param autoGeneratedIdTimestamp the auto-generated ID timestamp
     * @param isRetry                  whether this is a retry
     * @param ifSeqNo                  the conditional sequence number
     * @param ifPrimaryTerm            the conditional primary term
     * @return the prepared index operation
     */
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
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source, indexingExecutionEngine.newDocumentInput());
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

    /**
     * Not supported — delete operations are not implemented for data-format-aware engines.
     *
     * @throws UnsupportedOperationException always
     */
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
        throw new UnsupportedOperationException("delete operation not supported.");
    }

    /**
     * Refreshes the engine to make recently indexed documents searchable.
     * <p>
     * Acquires all writers from the pool, flushes each to produce per-format file sets,
     * delegates to the {@link IndexingExecutionEngine#refresh} to incorporate segments,
     * commits a new catalog snapshot, and notifies reader managers so they can open
     * updated readers.
     *
     * @param source a descriptive label for the refresh (e.g., "flush", "write indexing buffer")
     * @throws EngineException if the refresh fails
     */
    @Override
    public void refresh(String source) throws EngineException {
        final long refreshStartNanos = System.nanoTime();
        final long localCheckpointBeforeRefresh = localCheckpointTracker.getProcessedCheckpoint();
        boolean refreshed = false;
        List<Closeable> toClose = new ArrayList<>();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            ensureNoTragicException();
            refreshLock.lock();

            // refresh only if new segments have been created or force param is true
            notifyRefreshListenersBefore();
            try (GatedCloseable<CatalogSnapshot> catalogSnapshot = catalogSnapshotManager.acquireSnapshot()) {
                if (store.tryIncRef()) {
                    try {
                        List<DefaultLockableHolder<Writer<?>>> writers = writerPool.checkoutAll();
                        List<Segment> existingSegments = catalogSnapshot.get().getSegments();
                        List<Segment> newSegments = new ArrayList<>();

                        final long flushAllStartNanos = System.nanoTime();
                        int writerCount = writers.size();
                        long rowsToRelease = 0L;

                        // Add all checked-out writers to the shared flushQueue with a latch
                        // so the refresh thread can wait for ALL writers to be flushed
                        // (by itself + write threads cooperatively).
                        CountDownLatch flushLatch = new CountDownLatch(writerCount);
                        this.activeFlushLatch = flushLatch;
                        for (var lockable : writers) {
                            flushQueue.add(lockable.get());
                        }

                        // Refresh thread drains the queue itself (it's not idle — it does work)
                        Writer<?> writerToFlush;
                        while ((writerToFlush = flushQueue.poll()) != null) {
                            ensureOpen(); // short-circuit if engine has failed/closed concurrently
                            try {
                                final long writerFlushStartNanos = System.nanoTime();
                                FileInfos fileInfos = writerToFlush.flush(FlushInput.EMPTY);
                                final long writerFlushElapsedMs = TimeValue.nsecToMSec(System.nanoTime() - writerFlushStartNanos);

                                Segment.Builder segmentBuilder = Segment.builder(writerToFlush.generation());
                                boolean hasFiles = false;
                                for (Map.Entry<DataFormat, WriterFileSet> entry : fileInfos.writerFilesMap().entrySet()) {
                                    logger.trace(
                                        "Writer gen={} flushed format=[{}] files={}",
                                        writerToFlush.generation(),
                                        entry.getKey().name(),
                                        entry.getValue().files()
                                    );
                                    segmentBuilder.addSearchableFiles(entry.getKey(), entry.getValue());
                                    hasFiles = true;
                                }
                                logger.trace(
                                    "refresh[{}]: writer gen={} flush took [{}ms] hasFiles={}",
                                    source,
                                    writerToFlush.generation(),
                                    writerFlushElapsedMs,
                                    hasFiles
                                );
                                if (hasFiles) {
                                    Segment segment = segmentBuilder.build();
                                    newSegments.add(segment);
                                    rowsToRelease += segment.dfGroupedSearchableFiles().values().stream().findFirst().get().numRows();
                                }
                                refreshed |= hasFiles;
                            } catch (Exception e) {
                                IOUtils.closeWhileHandlingException(writerToFlush);
                                throw e;
                            }
                            toClose.add(writerToFlush);
                            flushLatch.countDown();
                        }

                        // Wait for any writers that write threads picked up to finish.
                        // Use active polling so we detect engine close/failure promptly
                        // instead of blocking indefinitely on the latch.
                        while (flushLatch.getCount() > 0) {
                            if (isClosed.get() || failedEngine.get() != null) {
                                throw new AlreadyClosedException("engine closed during refresh flush");
                            }
                            try {
                                if (flushLatch.await(1, TimeUnit.SECONDS) == false) {
                                    continue; // re-check isClosed / failedEngine
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("Refresh interrupted waiting for flush completion", e);
                            }
                        }
                        this.activeFlushLatch = null;
                        final long flushAllElapsedMs = TimeValue.nsecToMSec(System.nanoTime() - flushAllStartNanos);

                        // Drain any segments flushed by write threads (via preIndex)
                        Segment pendingSeg;
                        while ((pendingSeg = pendingSegments.poll()) != null) {
                            newSegments.add(pendingSeg);
                            rowsToRelease += pendingSeg.dfGroupedSearchableFiles().values().stream().findFirst().get().numRows();
                            refreshed = true;
                        }
                        // Drain pending writers so they get closed after addIndexes incorporates their files
                        Writer<?> pendingWriter;
                        while ((pendingWriter = pendingWritersToClose.poll()) != null) {
                            toClose.add(pendingWriter);
                        }

                        logger.debug(
                            "refresh[{}]: flushed {} writers producing {} new segments in [{}ms]",
                            source,
                            writerCount,
                            newSegments.size(),
                            flushAllElapsedMs
                        );
                        // Every new segment must contain files from at least one data format
                        assert newSegments.stream().allMatch(s -> s.dfGroupedSearchableFiles().isEmpty() == false)
                            : "new segments must have at least one format's files";
                        // No two new segments may share the same generation
                        assert newSegments.stream().map(Segment::generation).distinct().count() == newSegments.size()
                            : "new segments must have unique generations";

                        // New segment generations must not collide with existing segment generations
                        assert newSegments.stream()
                            .noneMatch(ns -> existingSegments.stream().anyMatch(es -> es.generation() == ns.generation()))
                            : "new segment generation collides with an existing segment generation";

                        if (refreshed) {
                            final long engineRefreshStartNanos = System.nanoTime();
                            long nextGen = newSegments.size() > 1 ? writerGenerationCounter.incrementAndGet() : RefreshInput.NO_GENERATION;
                            RefreshInput refreshInput = new RefreshInput(existingSegments, newSegments, nextGen);
                            RefreshResult result = indexingExecutionEngine.refresh(refreshInput);
                            final long engineRefreshElapsedMs = TimeValue.nsecToMSec(System.nanoTime() - engineRefreshStartNanos);
                            logger.debug(
                                "refresh[{}]: indexingExecutionEngine.refresh took [{}ms] "
                                    + "existingSegments={} newSegments={} resultSegments={}",
                                source,
                                engineRefreshElapsedMs,
                                existingSegments.size(),
                                newSegments.size(),
                                result.refreshedSegments().size()
                            );
                            // Refresh result must contain at least as many segments as existed before (existing + new)
                            assert result.refreshedSegments().size() >= existingSegments.size()
                                : "refresh must not lose existing segments; had "
                                    + existingSegments.size()
                                    + " but got "
                                    + result.refreshedSegments().size();

                            final long commitStartNanos = System.nanoTime();
                            catalogSnapshotManager.commitNewSnapshot(result.refreshedSegments());
                            assert rowsToRelease > 0L : "Rows to release from active writes should be greater than 0 but was: "
                                + rowsToRelease
                                + " for shard: "
                                + shardId;
                            pendingRowCount.addAndGet(-rowsToRelease);
                            final long commitElapsedMs = TimeValue.nsecToMSec(System.nanoTime() - commitStartNanos);
                            logger.trace("refresh[{}]: catalogSnapshot commit took [{}ms]", source, commitElapsedMs);
                        } else if ("flush".equals(source)) {
                            catalogSnapshotManager.bumpGeneration();
                        }
                    } finally {
                        store.decRef();
                    }
                    if (refreshed) {
                        lastRefreshedCheckpointListener.updateRefreshedCheckpoint(localCheckpointBeforeRefresh);
                        maybePruneDeletes();
                        triggerPossibleMerges(); // trigger merges
                    }
                }
            } finally {
                notifyRefreshListenersAfter(refreshed);
                versionMap.afterRefresh(refreshed);
                IOUtils.close(toClose);
                refreshLock.unlock();
            }
        } catch (AlreadyClosedException ex) {
            failOnTragicEvent(ex);
            throw ex;
        } catch (Exception ex) {
            try {
                failEngine("refresh failed source[" + source + "]", ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, ex);
        }
        final long totalRefreshElapsedMs = TimeValue.nsecToMSec(System.nanoTime() - refreshStartNanos);
        logger.debug("refresh[{}]: total time to make documents searchable [{}ms] refreshed={}", source, totalRefreshElapsedMs, refreshed);
    }

    private void notifyRefreshListenersBefore() throws IOException {
        for (ReferenceManager.RefreshListener refreshListener : refreshListeners) {
            refreshListener.beforeRefresh();
        }
    }

    private void notifyRefreshListenersAfter(boolean didRefresh) throws IOException {
        for (ReferenceManager.RefreshListener refreshListener : refreshListeners) {
            refreshListener.afterRefresh(didRefresh);
        }
    }

    /**
     * Flushes the engine by refreshing buffered data to segments, persisting the catalog
     * snapshot and commit data (translog UUID, sequence numbers), syncing the translog,
     * and trimming unreferenced translog files.
     *
     * @param force       if {@code true}, forces a flush even if not strictly needed
     * @param waitIfOngoing if {@code true}, waits for an in-progress flush to complete
     * @throws EngineException if the flush fails
     * @throws IllegalArgumentException if {@code force} is true but {@code waitIfOngoing} is false
     */
    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        if (force && waitIfOngoing == false) {
            throw new IllegalArgumentException(
                "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing
            );
        }
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            ensureNoTragicException();
            if (flushLock.tryLock() == false) {
                if (waitIfOngoing == false) {
                    return;
                }
                flushLock.lock();
            }
            try {
                // Hold refreshLock across the whole flush so a concurrent refresh cannot advance
                // latestCatalogSnapshot between commit() and updateLastCommitInfo(). Reentrant.
                refreshLock.lock();
                try {
                    // Capture the processed checkpoint BEFORE refreshing. The refresh persists the
                    // current writer buffer into the catalog snapshot; any operation processed
                    // concurrently DURING this flush lands in a NEW writer that is not part of this
                    // snapshot. Committing the live (post-refresh) processed checkpoint would
                    // over-claim those ops, so a subsequent recovery/relocation would start replay
                    // past them (seeding them as "already processed") and silently drop them.
                    // Capturing here keeps the committed local checkpoint <= what the snapshot
                    // durably contains, mirroring Lucene's InternalEngine.commitIndexWriter (which
                    // captures the checkpoint before IndexWriter.commit flushes). See
                    // DataFormatAwareEngineTests#testFlushMustNotCommitCheckpointAheadOfPersistedSnapshot.
                    final long committedLocalCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
                    // Refresh first to flush buffered data to segments
                    refresh("flush");
                    translogManager.rollTranslogGeneration();
                    // Persist the latest catalog snapshot so it survives restart
                    try (GatedConditionalCloseable<CatalogSnapshot> snapshotRef = catalogSnapshotManager.acquireSnapshotForCommit()) {
                        CatalogSnapshot snapshot = snapshotRef.get();
                        Map<String, String> lastCommitData = committer.getLastCommittedData();
                        String lastCommittedSnapshotId = lastCommitData.get(CatalogSnapshot.CATALOG_SNAPSHOT_ID);
                        // commit only if last committed CS id is different from the one we are about to commit or if force param is true
                        if (force || lastCommittedSnapshotId == null || snapshot.getId() != Long.parseLong(lastCommittedSnapshotId)) {
                            // Sync translog before commit so the global checkpoint is persisted
                            // and available to the deletion policy when onCommit is triggered.
                            translogManager.ensureCanFlush();
                            translogManager.syncTranslog();
                            Map<String, String> commitData = new HashMap<>();
                            commitData.put(
                                CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY,
                                Long.toString(snapshot.getLastWriterGeneration())
                            );
                            commitData.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(snapshot.getId()));
                            commitData.put(Translog.TRANSLOG_UUID_KEY, translogManager.getTranslogUUID());
                            commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(committedLocalCheckpoint));
                            commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
                            commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
                            commitData.put(Engine.HISTORY_UUID_KEY, historyUUID);

                            // Update snapshot userData so deletion policy can read max_seq_no
                            snapshot.setUserData(commitData, true);

                            // Now add snapshot to commit data so it has latest snapshot
                            commitData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, snapshot.serializeToString());

                            // Commit data must contain all keys required for recovery
                            assert commitData.containsKey(CatalogSnapshot.CATALOG_SNAPSHOT_KEY) : "commit data missing catalog snapshot";
                            assert commitData.containsKey(Translog.TRANSLOG_UUID_KEY) : "commit data missing translog UUID";
                            assert commitData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) : "commit data missing local checkpoint";
                            assert commitData.containsKey(SequenceNumbers.MAX_SEQ_NO) : "commit data missing max seq no";
                            assert commitData.containsKey(Engine.HISTORY_UUID_KEY) : "commit data missing history UUID";
                            assert snapshot.getId() >= 0 : "snapshot ID must be non-negative but was: " + snapshot.getId();
                            assert Long.parseLong(commitData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) >= -1
                                : "local checkpoint in commit data must be >= -1";
                            assert Long.parseLong(commitData.get(SequenceNumbers.MAX_SEQ_NO)) >= -1
                                : "max seq no in commit data must be >= -1";

                            // We do an additional commit on engine start due to no catalog snapshot present in earlier commit during empty
                            // recovery
                            Committer.CommitResult commitResult = committer.commit(
                                new Committer.CommitInput(commitData.entrySet(), snapshot, 0)
                            );

                            if (commitResult != null && snapshot instanceof DataformatAwareCatalogSnapshot dfaSnapshot) {
                                // If the catalog snapshot changed during the flush, this will ensure the latest one
                                // has the commit format.
                                // Any new snapshots created post this should track this commit info.
                                catalogSnapshotManager.updateLastCommitInfo(commitResult);
                            }
                            snapshotRef.markSuccess();
                            translogManager.trimUnreferencedReaders();
                        }
                    }
                    logger.trace("flush completed");

                    // Notify stats cache that flush completed to refresh committed state
                    statsCache.onFlushCompleted();
                } finally {
                    refreshLock.unlock();
                }
            } catch (AlreadyClosedException e) {
                failOnTragicEvent(e);
                throw e;
            } catch (Exception e) {
                maybeFailEngine("flush", e);
                throw new FlushFailedEngineException(shardId, e);
            } finally {
                flushLock.unlock();
            }
        }

        if (engineConfig.isEnableGcDeletes()) {
            pruneDeletedTombstones();
        }
    }

    /** Flushes the engine with default parameters (non-forced, wait if ongoing). */
    @Override
    public void flush() {
        flush(false, true);
    }

    /**
     * Determines whether a periodic flush is needed based on translog size relative
     * to the configured flush threshold.
     *
     * @return {@code true} if the translog exceeds the flush threshold
     */
    @Override
    public boolean shouldPeriodicallyFlush() {
        ensureOpen();
        try {
            Map<String, String> lastCommitData = committer.getLastCommittedData();
            final long localCheckpointOfLastCommit = Long.parseLong(
                lastCommitData.getOrDefault(SequenceNumbers.LOCAL_CHECKPOINT_KEY, "-1")
            );
            return translogManager.shouldPeriodicallyFlush(
                localCheckpointOfLastCommit,
                engineConfig.getIndexSettings().getFlushThresholdSize().getBytes()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Triggers a refresh to flush the indexing buffer to segments. */
    @Override
    public void writeIndexingBuffer() throws EngineException {
        refresh("write indexing buffer");
    }

    /**
     * Forces a merge to reduce the number of segments to at most {@code maxNumSegments}.
     * <p>
     * This method is a no-op for {@code upgrade}, {@code upgradeOnlyAncientSegments}, and
     * {@code onlyExpungeDeletes} operations since those are Lucene-specific concepts not
     * applicable to the data format engine.
     * <p>
     * When {@code flush} is true, a full flush is performed first to ensure all in-flight
     * indexing data (pending VSR writes, buffered segments) is committed before merge
     * candidate selection runs. This guarantees force merge operates on the complete
     * set of segments.
     * <p>
     * Runs synchronously on the calling {@code FORCE_MERGE} thread.
     */
    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {
        if (upgrade || upgradeOnlyAncientSegments || onlyExpungeDeletes) {
            return;
        }
        if (isFrozenForTiering()) {
            logger.debug("forceMerge blocked — engine is frozen for tiering");
            return;
        }
        mergeScheduler.forceMerge(maxNumSegments);
        if (flush) {
            flush(true, true);
        }
    }

    /** {@inheritDoc} Returns the heap RAM bytes used by the indexing execution engine. */
    @Override
    public long getHeapBytesUsed() {
        return indexingExecutionEngine.getHeapBytesUsed();
    }

    @Override
    public long getNativeBytesUsed() {
        return indexingExecutionEngine.getNativeBytesUsed();
    }

    /** {@inheritDoc} Activates write throttling when merge pressure increases. */
    @Override
    public void activateThrottling() {
        int count = throttleRequestCount.incrementAndGet();
        assert count >= 1 : "invalid post-increment throttleRequestCount=" + count;
        if (count == 1) {
            throttle.activate();
        }
    }

    /** {@inheritDoc} Deactivates write throttling when merge pressure subsides. */
    @Override
    public void deactivateThrottling() {
        int count = throttleRequestCount.decrementAndGet();
        assert count >= 0 : "invalid post-decrement throttleRequestCount=" + count;
        if (count == 0) {
            throttle.deactivate();
        }
    }

    @Override
    public boolean isThrottled() {
        return throttle.isThrottled();
    }

    /**
     * Updates the retention settings for the translog deletion policy.
     * Also resets the auto-ID timestamp optimization if disabled.
     *
     * @param translogRetentionAge   the maximum age for translog files
     * @param translogRetentionSize  the maximum total size for translog files
     * @param softDeletesRetentionOps unused — soft deletes are not supported
     */
    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        maybePruneDeletes();
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            updateAutoIdTimestamp(Long.MAX_VALUE, true);
        }
        final TranslogDeletionPolicy translogDeletionPolicy = translogManager.getDeletionPolicy();
        translogDeletionPolicy.setRetentionAgeInMillis(translogRetentionAge.millis());
        translogDeletionPolicy.setRetentionSizeInBytes(translogRetentionSize.getBytes());

        // This checks if the settings related to merge are changed and based on that updates the local variables in the class
        mergeScheduler.refreshConfig();
        updateTieringFreezeState();
    }

    /**
     * Detects tiering state transitions and freezes/unfreezes the engine accordingly.
     * Only freezes for HOT_TO_WARM (actual data migration in progress).
     * The prepare action explicitly calls freezeForTiering() while the index is still
     * HOT, so there is no separate transient state to freeze on here.
     * Unfreeze logic covers both cancel (HOT_TO_WARM → HOT) and prepare failure
     * (frozen-while-HOT → HOT) to ensure merge scheduler resumes.
     */
    private void updateTieringFreezeState() {
        String tieringStateStr = engineConfig.getIndexSettings()
            .getSettings()
            .get(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.name());
        boolean shouldFreeze;
        try {
            IndexModule.TieringState tieringState = IndexModule.TieringState.valueOf(tieringStateStr);
            shouldFreeze = tieringState == IndexModule.TieringState.HOT_TO_WARM;
        } catch (IllegalArgumentException e) {
            // Unrecognized tiering-state value — treat as not frozen, but surface the misconfiguration.
            logger.warn(
                "Unrecognized {} value [{}]; treating engine as not frozen for tiering",
                IndexModule.INDEX_TIERING_STATE.getKey(),
                tieringStateStr
            );
            shouldFreeze = false;
        }
        if (shouldFreeze) {
            if (frozenForTiering.compareAndSet(false, true)) {
                logger.info("Freezing engine for tiering — blocking merges, refresh, flush, and catalog commits");
                mergeScheduler.freeze();
            }
        } else {
            if (frozenForTiering.compareAndSet(true, false)) {
                logger.info("Unfreezing engine — tiering cancelled, resuming normal operations");
                mergeScheduler.unfreeze();
            }
        }
    }

    /** {@inheritDoc} Always returns {@code true} — a refresh is always considered needed. */
    @Override
    public boolean refreshNeeded() {
        // A refresh is needed if there are operations since the last refresh
        return true;
    }

    /**
     * Returns true if the engine is frozen for tiering and the operation should be blocked.
     * <p>
     * Prefers the cached volatile flag, which is set by the constructor on open, by
     * {@link #onSettingsChanged} on a tiering transition, and by {@link #freezeForTiering()}. The
     * live-settings fallback also returns true for a shard opened mid-tiering (restart/relocation) —
     * though the constructor already covers that — and, more importantly, closes the apply-ordering
     * gap on a live engine where the index settings already reflect HOT_TO_WARM but
     * {@link #onSettingsChanged} has not yet flipped the flag, so a request arriving in that window
     * is still blocked. (Requests already past this guard are drained separately by the prepare
     * action acquiring all primary permits.)
     */
    private boolean isFrozenForTiering() {
        if (frozenForTiering.get()) {
            return true;
        }
        // Fallback: read live settings to close the apply-ordering gap described above.
        String state = engineConfig.getIndexSettings()
            .getSettings()
            .get(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.name());
        return IndexModule.TieringState.HOT_TO_WARM.name().equals(state);
    }

    /**
     * Registers a listener that fires when all in-flight merges have completed.
     * If merges are already drained, fires the listener immediately inline.
     * Otherwise, the listener will fire on the merge thread when
     * the last merge finishes.
     *
     * @param listener the callback to fire when merges are drained
     */
    @Override
    public void onMergesDrained(Runnable listener) {
        mergeScheduler.onDrained(listener);
    }

    /**
     * Returns the number of currently active (in-flight) merge tasks.
     *
     * @return the active merge count, or 0 if no merges are running
     */
    @Override
    public int getActiveMergeCount() {
        return mergeScheduler.getActiveMergeCount();
    }

    /**
     * Returns whether any merges are queued but not yet started. DFA primary delegates
     * directly to its {@link MergeScheduler} (orthogonal to active count).
     */
    @Override
    public boolean hasPendingMerges() {
        return mergeScheduler.hasPendingMerges();
    }

    /** {@inheritDoc} Delegates to {@link #refresh(String)} and always returns {@code true}. */
    @Override
    public boolean maybeRefresh(String source) {
        refresh(source);
        return true;
    }

    /** No-op — data-format engines do not maintain Lucene-style delete tombstones. */
    @Override
    public void maybePruneDeletes() {
        // Pruning walks the deletes map taking a per-uid lock, so it is throttled to once per 1/4 of gcDeletes,
        // mirroring InternalEngine. The version map carries delete tombstones used by the get-by-id/version path.
        if (engineConfig.isEnableGcDeletes()
            && engineConfig.getThreadPool().relativeTimeInMillis() - lastDeleteVersionPruneTimeMSec > getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
    }

    /**
     * Prunes delete tombstones from the version map: those older than one GC-delete cycle whose sequence number
     * is at most the processed checkpoint (the single trimming strategy used by {@code InternalEngine}, correct
     * on both primary and replica). Updates {@link #lastDeleteVersionPruneTimeMSec}.
     */
    protected void pruneDeletedTombstones() {
        final long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
        final long maxTimestampToPrune = timeMSec - engineConfig.getIndexSettings().getGcDeletesInMillis();
        versionMap.pruneTombstones(maxTimestampToPrune, localCheckpointTracker.getProcessedCheckpoint());
        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    /**
     * Verifies that the global checkpoint matches the maximum sequence number before
     * closing the index. Throws if they diverge, indicating uncommitted operations.
     *
     * @throws IllegalStateException if global checkpoint does not match max seq no
     */
    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        final long globalCheckpoint = engineConfig.getGlobalCheckpointSupplier().getAsLong();
        final long maxSeqNo = getSeqNoStats(globalCheckpoint).getMaxSeqNo();
        if (globalCheckpoint != maxSeqNo) {
            throw new IllegalStateException(
                "Global checkpoint [" + globalCheckpoint + "] mismatches maximum sequence number [" + maxSeqNo + "]"
            );
        }
    }

    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return maxSeenAutoIdTimestamp.get();
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        updateAutoIdTimestamp(newTimestamp, true);
    }

    private void updateAutoIdTimestamp(long newTimestamp, boolean unsafe) {
        assert newTimestamp >= -1 : "invalid timestamp [" + newTimestamp + "]";
        maxSeenAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        if (unsafe) {
            maxUnsafeAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        }
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes.get();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        this.maxSeqNoOfUpdatesOrDeletes.updateAndGet(curr -> Math.max(curr, maxSeqNoOfUpdatesOnPrimary));
    }

    @Override
    public long getLastWriteNanos() {
        return lastWriteNanos;
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
    public long lastRefreshedCheckpoint() {
        return lastRefreshedCheckpointListener.lastRefreshedCheckpoint();
    }

    /** Local checkpoint captured just before the ongoing refresh; parallels {@code InternalEngine}. */
    @Override
    public long currentOngoingRefreshCheckpoint() {
        return lastRefreshedCheckpointListener.pendingCheckpoint();
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
        // Soft deletes are not supported yet.
        return 0L;
    }

    /**
     * Counts the number of translog operations between the given sequence numbers.
     *
     * @param source      a descriptive label for the caller
     * @param fromSeqNo   the starting sequence number (inclusive)
     * @param toSeqNumber the ending sequence number (inclusive)
     * @return the number of operations in the range
     * @throws IOException if reading the translog fails
     */
    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        ensureOpen();
        try (Translog.Snapshot snapshot = translogManager.newChangesSnapshot(fromSeqNo, toSeqNumber, false)) {
            int count = 0;
            while (snapshot.next() != null) {
                count++;
            }
            return count;
        }
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return false;
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            return SeqNoGapFiller.fillGaps(localCheckpointTracker, translogManager, primaryTerm, noOp -> innerNoOp(noOp));
        }
    }

    @Override
    public CommitStats commitStats() {
        return committer.getCommitStats();
    }

    @Override
    public DocsStats docStats() {
        // Fast path - return precomputed stats from cache
        return statsCache.getDocsStats();
    }

    @Override
    public List<org.opensearch.index.engine.Segment> segments(boolean verbose) {
        ensureOpen();
        // Fast path - return precomputed segments from cache
        // verbose is Lucene-specific (RAM tree breakdown per segment) — not applicable to DFAE, ignored.
        return statsCache.getSegments();
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        ensureOpen();
        // includeUnloadedSegments is a Lucene concept (segments on disk not yet loaded into a SegmentReader).
        // In DFAE, all segments are tracked in the catalog snapshot regardless of load state — no distinction exists.

        if (includeSegmentFileSizes) {
            // When file sizes are requested, compute on-demand with current snapshot
            try (GatedCloseable<CatalogSnapshot> snapshotRef = catalogSnapshotManager.acquireSnapshot()) {
                CatalogSnapshot snapshot = snapshotRef.get();

                return statsCache.buildSegmentsStats(
                    indexingExecutionEngine.getNativeBytesUsed(),
                    maxUnsafeAutoIdTimestamp.get(),
                    snapshot
                );
            } catch (Exception e) {
                logger.warn("Failed to compute segments stats with file sizes, falling back to cached stats", e);
                return statsCache.getSegmentsStats();
            }
        } else {
            // Fast path - return precomputed stats from cache (without file sizes)
            SegmentsStats cachedStats = statsCache.getSegmentsStats();
            if (cachedStats.getFileSizes() != null && !cachedStats.getFileSizes().isEmpty()) {
                // Create a copy without file sizes when includeSegmentFileSizes=false
                SegmentsStats statsWithoutFileSizes = new SegmentsStats();
                statsWithoutFileSizes.add(cachedStats.getCount());
                statsWithoutFileSizes.addIndexWriterMemoryInBytes(cachedStats.getIndexWriterMemoryInBytes());
                statsWithoutFileSizes.addVersionMapMemoryInBytes(cachedStats.getVersionMapMemoryInBytes());
                statsWithoutFileSizes.updateMaxUnsafeAutoIdTimestamp(cachedStats.getMaxUnsafeAutoIdTimestamp());
                return statsWithoutFileSizes;
            }
            return cachedStats;
        }
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
        return mergeScheduler.stats();
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return throttle.getThrottleTimeInMillis();
    }

    /**
     * Returns 0 because DataFormatAwareEngine has no in-flight write state visible to the IMC.
     * Unlike InternalEngine where IndexWriter stays open after flushNextBuffer() (keeping bytes
     * reported via ramBytesUsed while they're being written to disk), here refresh closes and
     * destroys all writers — memory reporting drops to 0 immediately on close, so there are
     * never bytes that are "reported as used but already being freed."
     */
    @Override
    public long getWritingBytes() {
        return 0L;
    }

    @Override
    public long unreferencedFileCleanUpsPerformed() {
        return catalogSnapshotManager.getUnreferencedFileCleanUpsPerformed();
    }

    @Override
    public EngineConfig config() {
        return engineConfig;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return catalogSnapshotManager.getSafeCommitInfo();
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return translogManager.acquireHistoryRetentionLock();
    }

    /**
     * Returns a translog snapshot for the given sequence number range.
     *
     * @param source            a descriptive label for the caller
     * @param fromSeqNo         the starting sequence number (inclusive)
     * @param toSeqNo           the ending sequence number (inclusive)
     * @param requiredFullRange whether the full range must be present
     * @param accurateCount     unused
     * @return a translog snapshot
     * @throws IOException if reading the translog fails
     */
    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return translogManager.newChangesSnapshot(fromSeqNo, toSeqNo, requiredFullRange);
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    /**
     * Flushes the engine and then closes it. If the engine is already closed, the flush
     * is skipped. Waits for any pending close operations to complete.
     *
     * @throws IOException if flush or close fails
     */
    @Override
    public void flushAndClose() throws IOException {
        if (isClosed.get() == false) {
            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    flush(false, true);
                } catch (AlreadyClosedException ex) {
                    logger.debug("engine already closed - skipping flushAndClose");
                } finally {
                    close();
                }
            }
        }
        awaitPendingClose();
    }

    /**
     * Fails the engine with the given reason and optional exception. Acquires the fail
     * engine lock to ensure only one failure is recorded. Closes the engine and notifies
     * the event listener.
     *
     * @param reason  a human-readable reason for the failure
     * @param failure the exception that caused the failure, or {@code null}
     */
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
                    // After failEngine, the engine must be in a closed state
                    assert isClosed.get() : "engine must be closed after failEngine";
                } finally {
                    logger.warn(() -> new ParameterizedMessage("failed engine [{}]", reason), failure);
                    // Delegate corruption marking to the committer (same as Engine.java's intent)
                    if (failure != null && Lucene.isCorruptionException(failure)) {
                        committer.markStoreCorrupted(
                            new IOException(
                                "failed engine (reason: [" + reason + "])",
                                org.opensearch.ExceptionsHelper.unwrapCorruption(failure)
                            )
                        );
                    }
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

    /**
     * Retires the writer if its {@link WriterState} is non-ACTIVE: ACTIVE returns {@code false}
     * (writer stays in the pool); RETIRED_FLUSHABLE checks the writer out, flushes the buffered
     * N-1 docs into {@link #pendingSegments}, and closes it; any other state — or a flush failure
     * — throws {@link IllegalStateException} so the caller can {@link #failEngine} and let
     * recovery replay the translog. The writer is checked out of the pool before any work that
     * could throw, so on throw the caller can still safely flag it as checked-out.
     */

    private boolean retireWriterIfNeeded(DefaultLockableHolder<Writer<?>> lockedWriter) {
        Writer<?> writer = lockedWriter.get();
        WriterState postState = writer.state();
        if (postState == WriterState.ACTIVE) {
            return false;
        }
        try (Releasable ignored = writerPool.checkout(lockedWriter)) {
            if (postState != WriterState.RETIRED_FLUSHABLE) {
                throw new IllegalStateException(
                    "writer generation [" + writer.generation() + "] retired with inconsistent buffer (state=" + postState + ")"
                );
            }

            // RETIRED_FLUSHABLE: flush the buffered N-1 docs. A throw here loses those acked
            // docs; surface as IllegalStateException so the caller fails the engine for recovery.
            FileInfos retiredFileInfos;
            try {
                retiredFileInfos = writer.flush(FlushInput.EMPTY);
            } catch (Exception flushEx) {
                throw new IllegalStateException(
                    "flush failed retiring writer generation [" + writer.generation() + "]; buffered acked docs lost",
                    flushEx
                );
            }
            Segment.Builder segBuilder = Segment.builder(writer.generation());
            boolean hasFiles = false;
            for (Map.Entry<DataFormat, WriterFileSet> entry : retiredFileInfos.writerFilesMap().entrySet()) {
                segBuilder.addSearchableFiles(entry.getKey(), entry.getValue());
                hasFiles = true;
            }
            if (hasFiles) {
                Segment retiredSegment = segBuilder.build();
                assert retiredSegment.generation() == writer.generation() : "retired segment generation must match writer generation";
                assert assertRetiredSegmentInvariants(writer, retiredFileInfos);
                pendingSegments.add(retiredSegment);
            }
        } finally {
            IOUtils.closeWhileHandlingException(writer);
        }
        assert writerPool.isRegistered(lockedWriter) == false : "retired writer must not be in pool";
        return true;
    }

    /**
     * Verifies cross-format row-count parity on a retired writer's flushed segment.
     * Always called inside an {@code assert} so the map walk is stripped when assertions are disabled.
     */
    private static boolean assertRetiredSegmentInvariants(Writer<?> writer, FileInfos retiredFileInfos) {
        Map<DataFormat, WriterFileSet> filesMap = retiredFileInfos.writerFilesMap();
        long expectedRows = filesMap.values().iterator().next().numRows();
        for (Map.Entry<DataFormat, WriterFileSet> entry : filesMap.entrySet()) {
            long rows = entry.getValue().numRows();
            if (rows != expectedRows) {
                throw new AssertionError(
                    "retired segment row counts diverge across formats: format ["
                        + entry.getKey().name()
                        + "] has "
                        + rows
                        + " rows, expected "
                        + expectedRows
                );
            }
        }
        return true;
    }

    /**
     * Acquires a reference to the current catalog snapshot for reading segment metadata.
     *
     * @return a gated closeable wrapping the catalog snapshot
     */
    @Override
    public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
        return catalogSnapshotManager.acquireSnapshot();
    }

    @Override
    public byte[] serializeSnapshotToRemoteMetadata(CatalogSnapshot catalogSnapshot) throws IOException {
        return catalogSnapshotManager.serializeToCommitFormat(catalogSnapshot);
    }

    /**
     * Delegates get-by-id to the installed {@link DocumentLookupProvider}, acquiring a
     * per-format reader on the current snapshot and passing it to the plugin.
     * Throws {@link UnsupportedOperationException} if no plugin is wired.
     */
    @Override
    public Engine.GetResult getById(Engine.Get get, BiFunction<String, Engine.SearcherScope, Engine.Searcher> searcherFactory)
        throws IOException {
        if (documentLookup.isSupported() == false) {
            throw new UnsupportedOperationException("getById not supported: no DocumentLookupProvider installed");
        }
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            if (get.realtime()) {
                VersionValue versionValue;
                try (Releasable ignore = versionMap.acquireLock(get.uid().bytes())) {
                    versionValue = getVersionFromMap(get.uid().bytes());
                }
                if (versionValue != null) {
                    if (versionValue.isDelete()) {
                        return Engine.GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version, get.version())) {
                        throw new VersionConflictEngineException(
                            shardId,
                            get.id(),
                            get.versionType().explainConflictForReads(versionValue.version, get.version())
                        );
                    }
                    if (get.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
                        && (get.getIfSeqNo() != versionValue.seqNo || get.getIfPrimaryTerm() != versionValue.term)) {
                        throw new VersionConflictEngineException(
                            shardId,
                            get.id(),
                            get.getIfSeqNo(),
                            get.getIfPrimaryTerm(),
                            versionValue.seqNo,
                            versionValue.term
                        );
                    }
                    if (get.isReadFromTranslog() && versionValue.getLocation() != null) {
                        try {
                            Translog.Operation operation = translogManager.readOperation(versionValue.getLocation());
                            if (operation != null) {
                                Translog.Index index = (Translog.Index) operation;
                                return new DocumentLookupResult(
                                    get.id(),
                                    index.version(),
                                    true,
                                    index.source(),
                                    index.seqNo(),
                                    index.primaryTerm(),
                                    Map.of(),
                                    Map.of()
                                ).toGetResult();
                            }
                        } catch (IOException e) {
                            throw new EngineException(shardId, "failed to read operation from translog", e);
                        }
                    }
                    assert versionValue.seqNo >= 0 : versionValue;
                }
            }

            // Fall through: read from parquet
            try (GatedCloseable<Reader> readerRef = acquireReader()) {
                DocumentLookupResult result = documentLookup.lookupFromReader(get, readerRef.get());
                return result.exists() ? result.toGetResult() : Engine.GetResult.NOT_EXISTS;
            }
        } // readLock
    }

    /**
     * DFA callers MUST use {@link #acquireSafeCatalogSnapshot()} — that API avoids the extra
     * {@code segments_N} disk read required to materialize a Lucene {@link IndexCommit}, and
     * carries the richer {@link CatalogSnapshot} that describes multi-format segments.
     * This method is retained only to satisfy the {@link Engine} contract; calling it on DFA
     * is a programming error.
     */
    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        throw new UnsupportedOperationException(
            "acquireSafeIndexCommit is not supported on DataFormatAwareEngine; use acquireSafeCatalogSnapshot instead"
        );
    }

    @Override
    public GatedCloseable<CatalogSnapshot> acquireSafeCatalogSnapshot() throws EngineException {
        ensureOpen();
        return catalogSnapshotManager.acquireCommittedSnapshot(true);
    }

    @Override
    public GatedCloseable<CatalogSnapshot> acquireLastCommittedSnapshot(boolean flushFirst) throws EngineException {
        ensureOpen();
        if (flushFirst) {
            flush(false, true);
        }
        return catalogSnapshotManager.acquireCommittedSnapshot(false);
    }

    /**
     * Acquires a {@link DataFormatAwareReader} on the latest catalog snapshot.
     * The caller must close the returned reader when done, which releases the
     * snapshot reference.
     *
     * @return a gated closeable wrapping the reader
     * @throws IOException if reader acquisition fails
     */
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
            DataFormatAwareReader reader = new DataFormatAwareReader(snapshotRef, readers);
            return new GatedCloseable<>(reader, reader::close);
        } catch (Exception e) {
            snapshotRef.close();
            throw e;
        }
    }

    @Override
    public void ensureOpen() {
        if (isClosed.get()) {
            throw new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
        }
    }

    /**
     * Closes the engine, releasing all resources including the indexing execution engine,
     * translog manager, reader managers, and store reference.
     *
     * @throws IOException if closing any resource fails
     */
    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) {
            try (ReleasableLock lock = writeLock.acquire()) {
                closeNoLock("api");
            }
        }
        awaitPendingClose();
    }

    private void applyMergeChanges(MergeResult mergeResult, OneMerge oneMerge) {
        assert mergeResult != null : "merge result must not be null";
        assert oneMerge != null : "oneMerge must not be null";
        assert oneMerge.getSegmentsToMerge().isEmpty() == false : "merged segments list must not be empty";
        // refreshLock may already be held by the merge thread when Lucene participated in the
        // merge: the Lucene committer's MergedSegmentWarmer acquires it between mergeMiddle and
        // commitMerge to coordinate with refreshes. When Lucene is not a participant (pure-Parquet
        // merges, or Lucene merges that skip because the shared writer has no matching segments),
        // the warmer never fires and the lock is not held on entry; acquire it locally to
        // serialise the catalog update against concurrent refreshes. Always release on exit.
        final boolean acquiredHere = refreshLock.isHeldByCurrentThread() == false;
        if (acquiredHere) {
            refreshLock.lock();
        }
        try (GatedCloseable<CatalogSnapshot> oldSnapshotRef = catalogSnapshotManager.acquireSnapshot()) {
            // A merge only swaps segments in the catalog; it does not advance the checkpoint, so
            // checkpoint-publishing listeners must not be notified. We invoke only the
            // RemoteStoreRefreshListener so the merged segments get uploaded to the remote store.
            for (ReferenceManager.RefreshListener refreshListener : refreshListeners) {
                if (refreshListener instanceof RemoteStoreRefreshListener) {
                    refreshListener.beforeRefresh();
                }
            }
            catalogSnapshotManager.applyMergeResults(mergeResult, oneMerge);
            for (ReferenceManager.RefreshListener refreshListener : refreshListeners) {
                if (refreshListener instanceof RemoteStoreRefreshListener) {
                    refreshListener.afterRefresh(true);
                }
            }
        } catch (Exception ex) {
            try {
                logger.error(() -> new ParameterizedMessage("Merge failed while registering merged files in Snapshot"), ex);
                failEngine("Merge failed while registering merged files in Snapshot", ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw new MergeFailedEngineException(shardId, ex);
        } finally {
            refreshLock.unlock();
        }
    }

    private void triggerPossibleMerges() {
        if (Booleans.parseBoolean(System.getProperty(MERGE_ENABLED_PROPERTY, Boolean.TRUE.toString())) == false) {
            logger.debug("Pluggable dataformat merge is disabled via system property [{}], skipping merge", MERGE_ENABLED_PROPERTY);
            return;
        }
        if (isFrozenForTiering()) {
            return;
        }
        mergeScheduler.triggerMerges();
    }

    private void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock must be held or the engine must be currently failing";
            try {
                this.versionMap.clear();
                // Stop accepting new merges immediately
                mergeScheduler.shutdown();
                // Discard any pending segments not yet picked up by refresh
                pendingSegments.clear();
                // Close any writers queued for deferred close (their files won't reach the catalog)
                Writer<?> pendingWriter;
                while ((pendingWriter = pendingWritersToClose.poll()) != null) {
                    IOUtils.closeWhileHandlingException(pendingWriter);
                }
                // Close writers still in the flush queue (checked out of pool, not yet flushed).
                // Without this, their LuceneWriter IndexWriter locks remain in LOCK_HELD.
                Writer<?> queuedWriter;
                while ((queuedWriter = flushQueue.poll()) != null) {
                    IOUtils.closeWhileHandlingException(queuedWriter);
                }
                // Close all writers still in the pool (unflushed writers from the current cycle)
                for (var holder : writerPool.checkoutAll()) {
                    IOUtils.closeWhileHandlingException(holder.get());
                }
                IOUtils.close(indexingExecutionEngine, committer, translogManager);
                closeReaders();
            } catch (Exception e) {
                logger.warn("failed to close engine resources", e);
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

    private void closeReaders() throws IOException {
        List<Exception> exceptions = new ArrayList<>();
        for (EngineReaderManager<?> rm : readerManagers.values()) {
            try {
                rm.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        if (exceptions.isEmpty() == false) {
            IOException ioException = new IOException("Failed to close DataFormatAwareEngine resources");
            for (Exception e : exceptions) {
                ioException.addSuppressed(e);
            }
            throw ioException;
        }
    }

    protected VersionValue resolveDocVersion(final Engine.Operation op, boolean loadSeqNo) throws IOException {
        assert incrementVersionLookup();
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        if (versionValue == null) {
            if (documentLookupProvider == null) {
                return null;
            }
            assert incrementIndexVersionLookup();
            DocumentLookupResult lookupResult;
            try (GatedCloseable<Reader> readerRef = acquireReader()) {
                Reader reader = readerRef.get();
                if (reader.catalogSnapshot().getSegments().isEmpty()) {
                    return null;
                }
                lookupResult = documentLookupProvider.getVersionMetadata(op.id(), reader, shardId.getIndex(), documentMetadataResolver);
            }
            if (lookupResult.version() != Versions.NOT_FOUND) {
                versionValue = new IndexVersionValue(null, lookupResult.version(), lookupResult.seqNo(), lookupResult.primaryTerm());
            }
        } else {
            if (engineConfig.isEnableGcDeletes()
                && versionValue.isDelete()
                && (engineConfig.getThreadPool().relativeTimeInMillis()
                    - ((DeleteVersionValue) versionValue).time) > getGcDeletesInMillis()) {
                versionValue = null;
            }
        }
        return versionValue;
    }

    long getGcDeletesInMillis() {
        return engineConfig.getIndexSettings().getGcDeletesInMillis();
    }

    private boolean incrementVersionLookup() { // only used by asserts
        numVersionLookups.inc();
        return true;
    }

    private boolean incrementIndexVersionLookup() {
        numIndexVersionsLookups.inc();
        return true;
    }

    private static OpVsEngineDocStatus compareOpToVersionMapOnSeqNo(String id, long seqNo, long primaryTerm, VersionValue versionValue) {
        Objects.requireNonNull(versionValue);
        if (seqNo > versionValue.seqNo) {
            return OpVsEngineDocStatus.OP_NEWER;
        } else if (seqNo == versionValue.seqNo) {
            assert versionValue.term == primaryTerm : "primary term not matched; id="
                + id
                + " seq_no="
                + seqNo
                + " op_term="
                + primaryTerm
                + " existing_term="
                + versionValue.term;
            return OpVsEngineDocStatus.OP_STALE_OR_EQUAL;
        } else {
            return OpVsEngineDocStatus.OP_STALE_OR_EQUAL;
        }
    }

    private void restoreVersionMapAndCheckpointTracker() {
        try {
            final long persistedCheckpoint = localCheckpointTracker.getPersistedCheckpoint();
            if (documentLookupProvider != null) {
                try (GatedCloseable<Reader> readerRef = acquireReader()) {
                    List<DocumentLookupResult> docs = documentLookupProvider.getDocsAboveSeqNo(
                        persistedCheckpoint,
                        readerRef.get(),
                        shardId.getIndex(),
                        documentMetadataResolver
                    );
                    for (DocumentLookupResult doc : docs) {
                        localCheckpointTracker.markSeqNoAsProcessed(doc.seqNo());
                        localCheckpointTracker.markSeqNoAsPersisted(doc.seqNo());
                        final BytesRef uid = new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())).bytes();
                        try (Releasable ignored = versionMap.acquireLock(uid)) {
                            final VersionValue curr = versionMap.getUnderLock(uid);
                            if (curr == null
                                || compareOpToVersionMapOnSeqNo(
                                    doc.id(),
                                    doc.seqNo(),
                                    doc.primaryTerm(),
                                    curr
                                ) == OpVsEngineDocStatus.OP_NEWER) {
                                versionMap.putIndexUnderLock(
                                    uid,
                                    new IndexVersionValue(null, doc.version(), doc.seqNo(), doc.primaryTerm())
                                );
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new EngineCreationFailureException(
                config().getShardId(),
                "failed to restore version map and local checkpoint tracker",
                e
            );
        }
    }

    private VersionValue getVersionFromMap(BytesRef id) {
        if (versionMap.isUnsafe()) {
            synchronized (versionMap) {
                // we are switching from an unsafe map to a safe map. This might happen concurrently
                // but we only need to do this once since the last operation per ID is to add to the version
                // map so once we pass this point we can safely lookup from the version map.
                if (versionMap.isUnsafe()) {
                    refresh("unsafe_version_map");
                }
                versionMap.enforceSafeAccess();
            }
        }
        return versionMap.getUnderLock(id);
    }

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private long currentMappingVersion() {
        return engineConfig.getMapperService().documentMapper().getVersion();
    }

    /**
     * Called before each index operation on the write thread. If the shared flush queue
     * has pending writers (put there by the refresh thread), this thread picks one up
     * and flushes it — providing natural backpressure on indexing while helping drain
     * the flush work cooperatively.
     *
     * <p>Gated by {@code index.check_pending_flush.enabled} setting.
     */
    private void preIndex() {
        if (engineConfig.getIndexSettings().isCheckPendingFlushEnabled() == false) {
            logger.trace("preIndex: check pending flush disabled, skipping");
            return;
        }
        Writer<?> writerToFlush = flushQueue.poll();
        if (writerToFlush == null) {
            return;
        }
        try {
            final long flushStartNanos = System.nanoTime();
            FileInfos fileInfos = writerToFlush.flush(FlushInput.EMPTY);
            final long flushElapsedMs = TimeValue.nsecToMSec(System.nanoTime() - flushStartNanos);

            if (fileInfos.writerFilesMap().isEmpty() == false) {
                Segment.Builder segmentBuilder = Segment.builder(writerToFlush.generation());
                for (Map.Entry<DataFormat, WriterFileSet> entry : fileInfos.writerFilesMap().entrySet()) {
                    logger.trace(
                        "Writer gen={} flushed format=[{}] files={}",
                        writerToFlush.generation(),
                        entry.getKey().name(),
                        entry.getValue().files()
                    );
                    segmentBuilder.addSearchableFiles(entry.getKey(), entry.getValue());
                }
                pendingSegments.add(segmentBuilder.build());
            }
            // Queue writer for deferred close (temp dirs must survive until refresh does addIndexes)
            pendingWritersToClose.add(writerToFlush);

            logger.debug("preIndex: write thread flushed writer gen={} in [{}ms]", writerToFlush.generation(), flushElapsedMs);
        } catch (Exception e) {
            logger.warn(
                () -> new ParameterizedMessage("preIndex: flush failed for writer gen={}, failing engine", writerToFlush.generation()),
                e
            );
            IOUtils.closeWhileHandlingException(writerToFlush);
            failEngine("flush failed during preIndex for writer gen=" + writerToFlush.generation(), e);
        } finally {
            // Signal the refresh thread that one more writer has been flushed
            CountDownLatch latch = activeFlushLatch;
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        // Safety net for AlreadyClosedException caught mid-refresh / mid-flush — e.g., a
        // background merge in a format's underlying writer turns tragic between an entry-time
        // ensureNoTragicException check and the actual addIndexes / commit call. Consult the
        // indexing engine (multiplexes across formats) and the translog: either can be the
        // source of an ACE reaching this catch.
        Exception engineTragic = indexingExecutionEngine.getTragicException();
        if (engineTragic != null) {
            failEngine("already closed by tragic event on indexing engine", engineTragic);
            return true;
        }
        if (translogManager.getTragicExceptionIfClosed() != null) {
            failEngine("already closed by tragic event on the translog", translogManager.getTragicExceptionIfClosed());
            return true;
        } else if (failedEngine.get() == null && isClosed.get() == false) {
            throw new AssertionError("Unexpected AlreadyClosedException", ex);
        }
        return false;
    }

    /**
     * If any per-format indexing engine has recorded a tragic exception (e.g., an
     * unrecoverable IndexWriter failure), fail the engine. Called at refresh and flush entry
     * points so a tragic event surfaces on the next maintenance op rather than on a doc
     * write — keeping the indexing hot path free of cross-component state checks.
     */
    private void ensureNoTragicException() {
        Exception engineTragic = indexingExecutionEngine.getTragicException();
        if (engineTragic != null) {
            failEngine("tragic event on indexing engine", engineTragic);
            throw new AlreadyClosedException(shardId + " engine is closed", engineTragic);
        }
    }

    private boolean maybeFailEngine(String source, Exception e) {
        if (Lucene.isCorruptionException(e)) {
            failEngine("corrupt file (source: [" + source + "])", e);
            return true;
        } else if (e instanceof AlreadyClosedException) {
            return failOnTragicEvent((AlreadyClosedException) e);
        } else if (e != null && translogManager.getTragicExceptionIfClosed() == e) {
            failEngine(source, e);
            return true;
        }
        return false;
    }

    boolean assertPrimaryIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        // sequence number should not be set when operation origin is primary
        assert seqNo == UNASSIGNED_SEQ_NO : "primary operations must never have an assigned sequence number but was [" + seqNo + "]";
        return true;
    }

    private boolean hasBeenProcessedBefore(Engine.Operation op) {
        assert op.seqNo() != UNASSIGNED_SEQ_NO : "operation is not assigned seq_no";
        return localCheckpointTracker.hasProcessed(op.seqNo());
    }

    private long generateSeqNoForOperationOnPrimary(final Engine.Operation operation) {
        assert operation.origin() == Engine.Operation.Origin.PRIMARY;
        assert operation.seqNo() == UNASSIGNED_SEQ_NO : "ops should not have an assigned seq no. but was: " + operation.seqNo();
        return doGenerateSeqNoForOperation(operation);
    }

    private long doGenerateSeqNoForOperation(final Engine.Operation operation) {
        return localCheckpointTracker.generateSeqNo();
    }

    private void markSeqNoAsSeen(long seqNo) {
        localCheckpointTracker.advanceMaxSeqNo(seqNo);
    }

    /**
     * A catalog-snapshot-backed data-format aware reader providing per-format reader access.
     * Closing this reader releases the catalog snapshot reference.
     */
    @ExperimentalApi
    public static class DataFormatAwareReader implements IndexReaderProvider.Reader {
        private final GatedCloseable<CatalogSnapshot> snapshotRef;
        private final Map<DataFormat, Object> readers;

        public DataFormatAwareReader(GatedCloseable<CatalogSnapshot> snapshotRef, Map<DataFormat, Object> readers) {
            this.snapshotRef = snapshotRef;
            this.readers = readers;
        }

        @Override
        public Object reader(DataFormat format) {
            return readers.get(format);
        }

        /**
         * Returns the reader for the given format, validated against the expected type.
         *
         * @param format the data format
         * @param readerType the expected reader class
         * @param <R> the reader type
         * @return the typed reader, or {@code null} if no reader exists for the format
         * @throws IllegalArgumentException if the reader exists but is not of the expected type
         */
        @SuppressWarnings("unchecked")
        public <R> R getReader(DataFormat format, Class<R> readerType) {
            Object reader = readers.get(format);
            if (reader == null) {
                return null;
            }
            if (readerType.isInstance(reader) == false) {
                throw new IllegalArgumentException(
                    "Reader for format [" + format.name() + "] is " + reader.getClass().getName() + ", expected " + readerType.getName()
                );
            }
            return (R) reader;
        }

        @Override
        public CatalogSnapshot catalogSnapshot() {
            return snapshotRef.get();
        }

        @Override
        public void close() {
            try {
                snapshotRef.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to release catalog snapshot reference", e);
            }
        }
    }

    // -- Visible for testing --

    /** Returns the writers in the pool. Visible for testing only. */
    public Iterable<Writer<?>> getWriterPool() {
        List<Writer<?>> writers = new ArrayList<>();
        for (DefaultLockableHolder<Writer<?>> holder : writerPool) {
            writers.add(holder.get());
        }
        return writers;
    }

    /** Returns whether the writer is registered in the pool. Visible for testing only. */
    boolean isWriterRegistered(Writer<?> writer) {
        for (DefaultLockableHolder<Writer<?>> holder : writerPool) {
            if (holder.get() == writer) {
                return true;
            }
        }
        return false;
    }

    /** Returns the indexing execution engine. Visible for testing only. */
    IndexingExecutionEngine<?, ?> getIndexingExecutionEngine() {
        return indexingExecutionEngine;
    }

    /** Returns the failed engine exception, or null. Visible for testing only. */
    Exception getFailedEngine() {
        return failedEngine.get();
    }

    /** Returns the store. Visible for testing only. */
    Store getStore() {
        return store;
    }

    /**
     * Explicitly freezes the engine for tiering. Called by {@code TransportPrepareTieringAction}
     * after flush and remote sync are complete, just before the state transitions to HOT_TO_WARM.
     * Idempotent and thread-safe — a no-op if already frozen. The atomic {@code compareAndSet}
     * ensures only one caller wins the freeze transition under parallel tier/cancel.
     * <p>
     * After this call:
     * <ul>
     *   <li>No new merges will be registered (existing in-flight and pending merges drain to completion)</li>
     *   <li>Primary indexing operations are rejected with "frozen for tiering"</li>
     *   <li>{@link #triggerPossibleMerges()} becomes a no-op</li>
     * </ul>
     */
    @Override
    public void freezeForTiering() {
        if (frozenForTiering.compareAndSet(false, true)) {
            mergeScheduler.freeze();
            logger.info("Engine explicitly frozen for tiering by prepare action");
        }
    }
}
