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
import org.opensearch.OpenSearchException;
import org.opensearch.common.Booleans;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.concurrent.GatedConditionalCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.queue.DefaultLockableHolder;
import org.opensearch.common.queue.LockablePool;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.AppendOnlyIndexOperationRetryException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.FileInfos;
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
import org.opensearch.index.engine.dataformat.merge.DataFormatAwareMergePolicy;
import org.opensearch.index.engine.dataformat.merge.MergeFailedEngineException;
import org.opensearch.index.engine.dataformat.merge.MergeHandler;
import org.opensearch.index.engine.dataformat.merge.MergeScheduler;
import org.opensearch.index.engine.dataformat.merge.OneMerge;
import org.opensearch.index.engine.exec.CatalogSnapshotLifecycleListener;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
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
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
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
    private final LockablePool<DefaultLockableHolder<Writer<?>>> writerPool;
    private final AtomicLong writerGenerationCounter;

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;

    private final CatalogSnapshotManager catalogSnapshotManager;
    private final Committer committer;
    private final List<ReferenceManager.RefreshListener> refreshListeners;

    // Translog for durability and recovery
    private final TranslogManager translogManager;

    // Sequence number tracking
    private final LocalCheckpointTracker localCheckpointTracker;
    private final AtomicLong maxSeqNoOfUpdatesOrDeletes;

    // Throttling
    private final IndexingThrottler throttle;
    private final AtomicInteger throttleRequestCount = new AtomicInteger();

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
        this.logger = Loggers.getLogger(DataFormatAwareEngine.class, engineConfig.getShardId());
        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();
        this.throttle = new IndexingThrottler();

        List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
        if (engineConfig.getInternalRefreshListener() != null) {
            refreshListeners.addAll(engineConfig.getInternalRefreshListener());
        }
        // We don't segregate internal/external here since NRT is anyhow invoked on internal refresh which makes
        // data available to read on internal refreshes on replica.
        if (engineConfig.getExternalRefreshListener() != null) {
            refreshListeners.addAll(engineConfig.getExternalRefreshListener());
        }
        this.refreshListeners = List.copyOf(refreshListeners);

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
            this.committer = engineConfig.getCommitterFactory().getCommitter(new CommitterConfig(engineConfig, refreshLock::lock));

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
                return DefaultLockableHolder.of(new RowIdAwareWriter<>(indexingExecutionEngine.createWriter(new WriterConfig(gen))));
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
                    store.getDataformatAwareStoreHandles()
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
            if (committedSnapshots.isEmpty()) {
                committedSnapshots = List.of(CatalogSnapshotManager.createInitialSnapshot(0L, 0L, 0L, List.of(), -1L, userData));
            }
            this.catalogSnapshotManager = new CatalogSnapshotManager(
                committedSnapshots,
                combinedPolicy,
                fileDeleter,
                filesListeners,
                snapshotListeners,
                store.shardPath(),
                committer
            );

            this.lastRefreshedCheckpointListener = new LastRefreshedCheckpointListener(localCheckpointTracker);
            this.indexingStrategyPlanner = new IndexingStrategyPlanner(
                engineConfig.getIndexSettings(),
                engineConfig.getShardId(),
                new LiveVersionMap(),
                maxUnsafeAutoIdTimestamp::get,
                () -> 0L,
                localCheckpointTracker::getProcessedCheckpoint,
                this::hasBeenProcessedBefore,
                op -> OpVsEngineDocStatus.OP_NEWER,
                (a, b) -> null,
                this::updateAutoIdTimestamp,
                (a, b) -> null
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
            this.mergeScheduler = new MergeScheduler(
                mergeHandler,
                this::applyMergeChanges,
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
        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();
        assert (index.origin() == Engine.Operation.Origin.PRIMARY || index.origin() == Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY)
            : "DataFormatAwareEngine only supports PRIMARY origin but got: " + index.origin();
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            try (Releasable indexThrottle = doThrottle ? throttle.acquireThrottle() : () -> {}) {
                lastWriteNanos = index.startTime();
                final IndexingStrategy plan;
                if (index.origin() == Engine.Operation.Origin.PRIMARY) {
                    plan = indexingStrategyPlanner.planOperationAsPrimary(index);
                } else {
                    plan = indexingStrategyPlanner.planOperationAsNonPrimary(index);
                }
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
                        logger.debug(
                            "Indexing doc id=[{}] seqNo=[{}] primaryTerm=[{}] — writing to engine",
                            index.id(),
                            index.seqNo(),
                            index.primaryTerm()
                        );
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
        // Primary term must be positive — it identifies the current primary shard
        assert index.primaryTerm() > 0 : "primary term must be positive but was: " + index.primaryTerm();

        // Convert ParsedDocument to DocumentInput and write via the execution engine's writer
        Writer currentWriter = null;
        long mappingVersion = currentMappingVersion();
        DefaultLockableHolder<Writer<?>> lockedWriter = writerPool.getAndLock(
            h -> h.get().isSchemaMutable() || h.get().mappingVersion() >= mappingVersion
        );
        try {
            currentWriter = lockedWriter.get();
            currentWriter.updateMappingVersion(mappingVersion);
            // Writer pool must never return null — it creates on demand via the supplier
            assert currentWriter != null : "writer pool returned null writer";
            assert index.seqNo() >= 0 : "seqNo must be assigned before writing but was: " + index.seqNo();
            assert index.primaryTerm() > 0 : "primaryTerm must be positive but was: " + index.primaryTerm();
            WriteResult result = currentWriter.addDoc(index.parsedDoc().getDocumentInput());

            if (result instanceof WriteResult.Success) {
                indexResult = new Engine.IndexResult(plan.version, index.primaryTerm(), index.seqNo(), true);
                // The result must carry the same seq no that was assigned to the operation
                assert indexResult.getSeqNo() == index.seqNo() : "IndexResult seq no ["
                    + indexResult.getSeqNo()
                    + "] must match operation seq no ["
                    + index.seqNo()
                    + "]";
            } else {
                WriteResult.Failure f = (WriteResult.Failure) result;
                indexResult = new Engine.IndexResult(f.cause(), plan.version, index.primaryTerm(), index.seqNo());
            }
        } catch (Exception e) {
            indexResult = new Engine.IndexResult(e, plan.version, index.primaryTerm(), index.seqNo());
        } finally {
            if (currentWriter != null) {
                writerPool.releaseAndUnlock(lockedWriter);
            }
        }

        if (index.origin().isFromTranslog() == false) {
            final Translog.Location location;
            if (indexResult.getResultType() == Engine.Result.Type.SUCCESS) {
                location = translogManager.add(new Translog.Index(index, indexResult));
            } else if (indexResult.getSeqNo() != UNASSIGNED_SEQ_NO
                && indexResult.getFailure() != null
                && !(indexResult.getFailure() instanceof AppendOnlyIndexOperationRetryException)) {
                    throw new UnsupportedOperationException(
                        "recording document failure as a no-op in translog is not " + "supported for Data format engine"
                    );
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
     * Not supported — no-op operations are not implemented for data-format-aware engines.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        throw new UnsupportedOperationException("no_op operation not supported.");
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
        final long localCheckpointBeforeRefresh = localCheckpointTracker.getProcessedCheckpoint();
        boolean refreshed = false;
        List<Closeable> toClose = new ArrayList<>();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            refreshLock.lock();
            try (GatedCloseable<CatalogSnapshot> catalogSnapshot = catalogSnapshotManager.acquireSnapshot()) {
                if (store.tryIncRef()) {
                    try {
                        List<DefaultLockableHolder<Writer<?>>> writers = writerPool.checkoutAll();
                        List<Segment> existingSegments = catalogSnapshot.get().getSegments();
                        List<Segment> newSegments = new ArrayList<>();

                        for (var lockable : writers) {
                            Writer<?> writer = lockable.get();
                            FileInfos fileInfos = writer.flush();
                            Segment.Builder segmentBuilder = Segment.builder(writer.generation());
                            boolean hasFiles = false;
                            for (Map.Entry<DataFormat, WriterFileSet> entry : fileInfos.writerFilesMap().entrySet()) {
                                logger.debug(
                                    "Writer gen={} flushed format=[{}] files={}",
                                    writer.generation(),
                                    entry.getKey().name(),
                                    entry.getValue().files()
                                );
                                segmentBuilder.addSearchableFiles(entry.getKey(), entry.getValue());
                                hasFiles = true;
                            }
                            toClose.add(writer);
                            if (hasFiles) {
                                newSegments.add(segmentBuilder.build());
                            }
                            refreshed |= hasFiles;
                        }
                        logger.debug("Produced {} new segments from flush", newSegments.size());
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

                        // refresh only if new segments have been created or force param is true
                        notifyRefreshListenersBefore();
                        if (refreshed) {
                            RefreshInput refreshInput = new RefreshInput(existingSegments, newSegments);
                            RefreshResult result = indexingExecutionEngine.refresh(refreshInput);
                            // Refresh result must contain at least as many segments as existed before (existing + new)
                            assert result.refreshedSegments().size() >= existingSegments.size()
                                : "refresh must not lose existing segments; had "
                                    + existingSegments.size()
                                    + " but got "
                                    + result.refreshedSegments().size();

                            catalogSnapshotManager.commitNewSnapshot(result.refreshedSegments());
                        }
                        notifyRefreshListenersAfter(refreshed);
                    } finally {
                        store.decRef();
                    }
                    if (refreshed) {
                        lastRefreshedCheckpointListener.updateRefreshedCheckpoint(localCheckpointBeforeRefresh);
                        triggerPossibleMerges(); // trigger merges
                    }
                }
            } finally {
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
            if (flushLock.tryLock() == false) {
                if (waitIfOngoing == false) {
                    return;
                }
                flushLock.lock();
            }
            try {
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
                        commitData.put(CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY, Long.toString(snapshot.getLastWriterGeneration()));
                        commitData.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(snapshot.getId()));
                        commitData.put(Translog.TRANSLOG_UUID_KEY, translogManager.getTranslogUUID());
                        commitData.put(
                            SequenceNumbers.LOCAL_CHECKPOINT_KEY,
                            Long.toString(localCheckpointTracker.getProcessedCheckpoint())
                        );
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
                        assert Long.parseLong(commitData.get(SequenceNumbers.MAX_SEQ_NO)) >= -1 : "max seq no in commit data must be >= -1";
                        committer.commit(commitData);
                        snapshotRef.markSuccess();
                        translogManager.trimUnreferencedReaders();
                    }
                }
                logger.trace("flush completed");
            } catch (AlreadyClosedException e) {
                failOnTragicEvent(e);
                throw e;
            } catch (Exception e) {
                throw new FlushFailedEngineException(shardId, e);
            } finally {
                flushLock.unlock();
            }
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

    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {
        mergeScheduler.forceMerge(1);
    }

    /** {@inheritDoc} Returns the RAM bytes used by the indexing execution engine. */
    @Override
    public long getIndexBufferRAMBytesUsed() {
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
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            updateAutoIdTimestamp(Long.MAX_VALUE, true);
        }
        final TranslogDeletionPolicy translogDeletionPolicy = translogManager.getDeletionPolicy();
        translogDeletionPolicy.setRetentionAgeInMillis(translogRetentionAge.millis());
        translogDeletionPolicy.setRetentionSizeInBytes(translogRetentionSize.getBytes());

        // This checks if the settings related to merge are changed and based on that updates the local variables in the class
        mergeScheduler.refreshConfig();
    }

    /** {@inheritDoc} Always returns {@code true} — a refresh is always considered needed. */
    @Override
    public boolean refreshNeeded() {
        // A refresh is needed if there are operations since the last refresh
        return true;
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
        // No-op: data-format engines do not maintain Lucene-style delete tombstones
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
        throw new UnsupportedOperationException("updates/deletes not supported");
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
        // No-op: data-format engines do not maintain Lucene-style gaps
        return 0;
    }

    @Override
    public CommitStats commitStats() {
        return committer.getCommitStats();
    }

    @Override
    public DocsStats docStats() {
        try (GatedCloseable<CatalogSnapshot> snapshot = acquireSnapshot()) {
            long count = snapshot.get()
                .getSegments()
                .stream()
                .flatMap(segment -> segment.dfGroupedSearchableFiles().values().stream())
                .mapToLong(WriterFileSet::numRows)
                .sum();
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
        SegmentsStats stats = new SegmentsStats();
        throw new UnsupportedOperationException("Unsupported operation");
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

    @Override
    public long getWritingBytes() {
        return 0L;
    }

    @Override
    public long unreferencedFileCleanUpsPerformed() {
        return 0;
    }

    @Override
    public long getNativeBytesUsed() {
        return indexingExecutionEngine.getNativeBytesUsed();
    }

    @Override
    public EngineConfig config() {
        return engineConfig;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return committer.getSafeCommitInfo();
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
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
     * Acquires a reference to the current catalog snapshot for reading segment metadata.
     *
     * @return a gated closeable wrapping the catalog snapshot
     */
    @Override
    public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
        return catalogSnapshotManager.acquireSnapshot();
    }

    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        // TODO: Implement when commit coordination is added
        throw new UnsupportedOperationException("acquireSafeIndexCommit not yet implemented for DataFormatAwareEngine");
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
            notifyRefreshListenersBefore();
            catalogSnapshotManager.applyMergeResults(mergeResult, oneMerge);
            notifyRefreshListenersAfter(true);
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
        if (Booleans.parseBoolean(System.getProperty(MERGE_ENABLED_PROPERTY, Boolean.FALSE.toString())) == false) {
            logger.debug("Pluggable dataformat merge is disabled via system property [{}], skipping merge", MERGE_ENABLED_PROPERTY);
            return;
        }
        mergeScheduler.triggerMerges();
    }

    private void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock must be held or the engine must be currently failing";
            try {
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

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private long currentMappingVersion() {
        return engineConfig.getMapperService().getIndexSettings().getIndexMetadata().getMappingVersion();
    }

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        if (translogManager.getTragicExceptionIfClosed() != null) {
            failEngine("already closed by tragic event on the translog", translogManager.getTragicExceptionIfClosed());
            return true;
        } else if (failedEngine.get() == null && isClosed.get() == false) {
            throw new AssertionError("Unexpected AlreadyClosedException", ex);
        }
        return false;
    }

    private boolean maybeFailEngine(String source, Exception e) {
        if (e instanceof AlreadyClosedException) {
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

}
