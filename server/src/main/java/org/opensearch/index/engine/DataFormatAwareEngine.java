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
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
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
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.EngineReaderManager;
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
    private final LockablePool<Writer<?>> writerPool;
    private final AtomicLong writerGenerationCounter;

    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;

    private final CatalogSnapshotManager catalogSnapshotManager;
    private final Committer committer;

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

        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            updateAutoIdTimestamp(Long.MAX_VALUE, true);
        }

        boolean success = false;
        TranslogManager translogManagerRef = null;

        try {
            store.incRef();

            this.committer = engineConfig.getCommitterFactory().getCommitter(new CommitterConfig(engineConfig));
            this.catalogSnapshotManager = new CatalogSnapshotManager(0, 0, 0, List.of(), -1, Map.of());

            // Read history UUID and translog UUID from last commit
            final Map<String, String> userData = committer.getLastCommittedData();
            String translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));

            // Initialize translog
            // TODO: Once file deleter is merged, we will add relevant listeners
            final TranslogEventListener translogEventListener = createInternalTranslogEventListener();
            translogManagerRef = createTranslogManager(translogUUID, translogEventListener);
            this.translogManager = translogManagerRef;

            // Initialize local checkpoint tracker from last committed segment infos
            this.localCheckpointTracker = createLocalCheckpointTracker(LocalCheckpointTracker::new);
            maxSeqNoOfUpdatesOrDeletes = new AtomicLong(
                SequenceNumbers.max(localCheckpointTracker.getMaxSeqNo(), translogManager.getMaxSeqNo())
            );

            // Initialize from commit data
            this.historyUUID = userData.get(Engine.HISTORY_UUID_KEY);
            updateAutoIdTimestamp(Long.parseLong(userData.get(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID)), true);

            // Move to data format aware writers and readers.
            DataFormatRegistry registry = engineConfig.getDataFormatRegistry();
            // Create indexing engine
            // Pass committer here as well.
            this.indexingExecutionEngine = registry.getIndexingEngine(
                new IndexingEngineConfig(
                    committer,
                    config().getMapperService(),
                    config().getIndexSettings(),
                    config().getStore(),
                    registry
                ),
                registry.format(config().getIndexSettings().pluggableDataFormat())
            );
            this.writerGenerationCounter = new AtomicLong(1L);// committer.getCommitStats().getGeneration());
            this.writerPool = new LockablePool<>(
                () -> indexingExecutionEngine.createWriter(writerGenerationCounter.getAndIncrement()),
                LinkedList::new,
                Runtime.getRuntime().availableProcessors()
            );
            // Create Reader managers
            // We will pass IndexViewProvider to this, which would contain store
            // and any index specific attributes useful for reads.
            this.readerManagers = registry.getReaderManagers(
                Optional.ofNullable(indexingExecutionEngine.getProvider()),
                engineConfig.getMapperService(),
                engineConfig.getIndexSettings(),
                store.shardPath()
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

    private TranslogManager createTranslogManager(String translogUUID, TranslogEventListener translogEventListener) throws IOException {
        TranslogDeletionPolicy deletionPolicy = getTranslogDeletionPolicy();
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

    @Override
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();
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
                    }

                    assert index.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();

                    if (plan.executeOpOnEngine) {
                        logger.debug(
                            "Indexing doc id=[{}] seqNo=[{}] primaryTerm=[{}] — writing to engine",
                            index.id(),
                            index.seqNo(),
                            index.primaryTerm()
                        );
                        indexResult = indexIntoEngine(index);
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
    private Engine.IndexResult indexIntoEngine(Engine.Index index) throws IOException {
        Engine.IndexResult indexResult;

        assert index.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();

        // Convert ParsedDocument to DocumentInput and write via the execution engine's writer
        Writer currentWriter = null;
        try {
            currentWriter = writerPool.getAndLock();

            WriteResult result = currentWriter.addDoc(index.parsedDoc().getDocumentInput());

            if (result instanceof WriteResult.Success) {
                indexResult = new Engine.IndexResult(index.version(), index.primaryTerm(), index.seqNo(), true);
            } else {
                WriteResult.Failure f = (WriteResult.Failure) result;
                indexResult = new Engine.IndexResult(f.cause(), index.version(), index.primaryTerm(), index.seqNo());
            }
        } catch (Exception e) {
            indexResult = new Engine.IndexResult(e, index.version(), index.primaryTerm(), index.seqNo());
        } finally {
            if (currentWriter != null) {
                writerPool.releaseAndUnlock(currentWriter);
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

        // Track the sequence number
        localCheckpointTracker.markSeqNoAsProcessed(indexResult.getSeqNo());
        if (indexResult.getTranslogLocation() == null) {
            localCheckpointTracker.markSeqNoAsPersisted(indexResult.getSeqNo());
        }

        indexResult.setTook(System.nanoTime() - index.startTime());
        indexResult.freeze();
        return indexResult;
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        throw new UnsupportedEncodingException("delete operation not supported.");
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        throw new UnsupportedOperationException("no_op operation not supported.");
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

    @Override
    public void refresh(String source) throws EngineException {
        final long localCheckpointBeforeRefresh = localCheckpointTracker.getProcessedCheckpoint();
        boolean refreshed = false;
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            refreshLock.lock();
            try (GatedCloseable<CatalogSnapshot> catalogSnapshot = catalogSnapshotManager.acquireSnapshot()) {
                if (store.tryIncRef()) {
                    try {
                        List<Writer<?>> writers = writerPool.checkoutAll();
                        List<Segment> existingSegments = catalogSnapshot.get().getSegments();
                        List<Segment> newSegments = new ArrayList<>();

                        for (Writer<?> writer : writers) {
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
                            writer.close();
                            if (hasFiles) {
                                newSegments.add(segmentBuilder.build());
                            }
                            refreshed |= hasFiles;
                        }
                        logger.debug("Produced {} new segments from flush", newSegments.size());

                        RefreshInput refreshInput = new RefreshInput(existingSegments, newSegments);
                        RefreshResult result = indexingExecutionEngine.refresh(refreshInput);
                        catalogSnapshotManager.commitNewSnapshot(result.refreshedSegments());

                        // TODO: Add other Refresh listeners
                        // Notify reader managers so they can create readers for the new snapshot
                        try (GatedCloseable<CatalogSnapshot> newSnapshotRef = catalogSnapshotManager.acquireSnapshot()) {
                            CatalogSnapshot newSnapshot = newSnapshotRef.get();
                            for (EngineReaderManager<?> rm : readerManagers.values()) {
                                rm.afterRefresh(refreshed, newSnapshot);
                            }
                        }

                        refreshed = true;
                    } finally {
                        store.decRef();
                    }
                    if (refreshed) {
                        lastRefreshedCheckpointListener.updateRefreshedCheckpoint(localCheckpointBeforeRefresh);
                    }
                }
            } finally {
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
                // Persist the latest catalog snapshot so it survives restart
                try (GatedCloseable<CatalogSnapshot> snapshotRef = catalogSnapshotManager.acquireSnapshot()) {
                    CatalogSnapshot snapshot = snapshotRef.get();
                    Map<String, String> commitData = new HashMap<>();
                    commitData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, snapshot.serializeToString());
                    commitData.put(CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY, Long.toString(snapshot.getLastWriterGeneration()));
                    commitData.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(snapshot.getId()));
                    commitData.put(Translog.TRANSLOG_UUID_KEY, translogManager.getTranslogUUID());
                    commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpointTracker.getProcessedCheckpoint()));
                    commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
                    committer.commit(commitData);
                }
                translogManager.ensureCanFlush();
                translogManager.syncTranslog();
                translogManager.rollTranslogGeneration();
                translogManager.trimUnreferencedReaders();
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

    @Override
    public void flush() {
        flush(false, true);
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        ensureOpen();
        final long localCheckpointOfLastCommit = localCheckpointTracker.getPersistedCheckpoint();
        return translogManager.shouldPeriodicallyFlush(
            localCheckpointOfLastCommit,
            engineConfig.getIndexSettings().getFlushThresholdSize().getBytes()
        );
    }

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
        // TODO: Delegate to IndexingExecutionEngine's Merger when merge scheduling is implemented
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return indexingExecutionEngine.getNativeBytesUsed();
    }

    @Override
    public void activateThrottling() {
        int count = throttleRequestCount.incrementAndGet();
        assert count >= 1 : "invalid post-increment throttleRequestCount=" + count;
        if (count == 1) {
            throttle.activate();
        }
    }

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

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            updateAutoIdTimestamp(Long.MAX_VALUE, true);
        }
        final TranslogDeletionPolicy translogDeletionPolicy = translogManager.getDeletionPolicy();
        translogDeletionPolicy.setRetentionAgeInMillis(translogRetentionAge.millis());
        translogDeletionPolicy.setRetentionSizeInBytes(translogRetentionSize.getBytes());
    }

    @Override
    public boolean refreshNeeded() {
        // A refresh is needed if there are operations since the last refresh
        return true;
    }

    @Override
    public boolean maybeRefresh(String source) {
        refresh(source);
        return true;
    }

    @Override
    public void maybePruneDeletes() {
        // No-op: data-format engines do not maintain Lucene-style delete tombstones
    }

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
        // TODO: Derive from catalog snapshot segment metadata or reader. Pending discussion to finalize this.
        return new DocsStats(0, 0, 0);
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        throw new UnsupportedOperationException("CompletionStats not supported");
    }

    @Override
    public PollingIngestStats pollingIngestStats() {
        return null;
    }

    @Override
    public MergeStats getMergeStats() {
        // TODO: MergeHandler to provide this.
        return new MergeStats();
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
     * Acquires a DataFormatAwareReader on the latest catalog snapshot.
     * The caller MUST close the returned {@link DataFormatAwareReader} when done,
     * which releases the snapshot reference.
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

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) {
            try (ReleasableLock lock = writeLock.acquire()) {
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
                IOUtils.close(indexingExecutionEngine, translogManager);
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
