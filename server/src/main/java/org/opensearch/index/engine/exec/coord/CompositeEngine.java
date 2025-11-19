/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.bridge.CheckpointState;
import org.opensearch.index.engine.exec.bridge.Indexer;
import org.opensearch.index.engine.exec.bridge.IndexingThrottler;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.LuceneCommitEngine;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.merge.MergeHandler;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.MergeScheduler;
import org.opensearch.index.engine.exec.merge.OneMerge;
import org.opensearch.index.engine.exec.merge.CompositeMergeHandler;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.listener.CompositeTranslogEventListener;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.plugins.spi.vectorized.DataFormat;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.opensearch.index.engine.Engine.HISTORY_UUID_KEY;
import static org.opensearch.index.engine.Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID;
import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.CATALOG_SNAPSHOT_KEY;
import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY;

@ExperimentalApi
public class CompositeEngine implements LifecycleAware, Closeable, Indexer, CheckpointState, IndexingThrottler {

    private static final Consumer<ReferenceManager.RefreshListener> PRE_REFRESH_LISTENER_CONSUMER = refreshListener -> {
        try {
            refreshListener.beforeRefresh();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };
    private static final Consumer<ReferenceManager.RefreshListener> POST_REFRESH_LISTENER_CONSUMER = refreshListener -> {
        try {
            refreshListener.afterRefresh(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };
    private static final BiConsumer<ReleasableRef<CatalogSnapshot>, CatalogSnapshotAwareRefreshListener>
        POST_REFRESH_CATALOG_SNAPSHOT_AWARE_LISTENER_CONSUMER = (catalogSnapshot, catalogSnapshotAwareRefreshListener) -> {
        try {
            catalogSnapshotAwareRefreshListener.afterRefresh(true, catalogSnapshot);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    private final ShardId shardId;
    private final CompositeIndexingExecutionEngine engine;
    private final EngineConfig engineConfig;
    private final Store store;
    private final Logger logger;
    private final Committer compositeEngineCommitter;
    private final TranslogManager translogManager;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final SetOnce<Exception> failedEngine = new SetOnce<>();
    private final List<ReferenceManager.RefreshListener> refreshListeners = new ArrayList<>();
    private final List<CatalogSnapshotAwareRefreshListener> catalogSnapshotAwareRefreshListeners = new ArrayList<>();
    private final Map<String, List<FileDeletionListener>> fileDeletionListeners = new HashMap<>();
    private final Map<DataFormat, List<SearchExecEngine<?, ?, ?, ?>>> readEngines =
        new HashMap<>();
    private final MergeScheduler mergeScheduler;
    private final MergeHandler mergeHandler;

    @Nullable
    protected final String historyUUID;

        // TODO : how to extend this for Lucene ? where engine is a r/w engine
        // Create read specific engines for each format which is associated with shard
        InitalizeSearchEngine(searchEnginePlugins, shardPath);

    }

    public void InitalizeSearchEngine(List<SearchEnginePlugin> searchEnginePlugins, ShardPath shardPath) throws IOException
    {
        for (SearchEnginePlugin searchEnginePlugin : searchEnginePlugins) {
            for (org.opensearch.vectorized.execution.search.DataFormat dataFormat : searchEnginePlugin.getSupportedFormats()) {
                List<SearchExecEngine<?, ?, ?, ?>> currentSearchEngines = readEngines.getOrDefault(dataFormat, new ArrayList<>());
                SearchExecEngine<?, ?, ?, ?> newSearchEngine = searchEnginePlugin.createEngine(dataFormat,
                    Collections.emptyList(),
                    shardPath);

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
            CompositeTranslogEventListener compositeTranslogEventListener =
                new CompositeTranslogEventListener(Arrays.asList(internalTranslogEventListener, translogEventListener), shardId);
            translogManagerRef = createTranslogManager(translogUUID, translogDeletionPolicy, compositeTranslogEventListener);
            this.translogManager = translogManagerRef;

            // initialize committer and composite indexing execution engine
            committerRef = new LuceneCommitEngine(store, translogDeletionPolicy, translogManager::getLastSyncedGlobalCheckpoint);
            this.compositeEngineCommitter = committerRef;
            final AtomicLong lastCommittedWriterGeneration = new AtomicLong(-1);
            Map<String, String> lastCommittedData = this.compositeEngineCommitter.getLastCommittedData();
            if (lastCommittedData.containsKey(LAST_COMPOSITE_WRITER_GEN_KEY)) {
                lastCommittedWriterGeneration.set(Long.parseLong(lastCommittedData.get(CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY)));
            }

            System.out.println("While initialising Composite Engine - lst commit generation : " + lastCommittedWriterGeneration.get());

            // How to bring the Dataformat here? Currently, this means only Text and LuceneFormat can be used
            this.engine = new CompositeIndexingExecutionEngine(
                mapperService,
                pluginsService,
                shardPath,
                lastCommittedWriterGeneration.incrementAndGet()
            );
            //Initialize CatalogSnapshotManager before loadWriterFiles to ensure stale files are cleaned up before loading
            this.catalogSnapshotManager = new CatalogSnapshotManager(this, committerRef, shardPath);
            try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = catalogSnapshotManager.acquireSnapshot()) {
                this.engine.loadWriterFiles(catalogSnapshotReleasableRef.getRef());
            } catch (Exception e) {
                failEngine("unable to close releasable catalog snapshot while bootstrapping composite engine", e);
            }

            this.maxSeqNoOfUpdatesOrDeletes =
                new AtomicLong(SequenceNumbers.max(localCheckpointTracker.getMaxSeqNo(), translogManager.getMaxSeqNo()));

            this.indexingStrategyPlanner = new IndexingStrategyPlanner(
                engineConfig,
                engineConfig.getShardId(),
                new LiveVersionMap(),
                maxUnsafeAutoIdTimestamp::get,
                maxSeqNoOfUpdatesOrDeletes::get,
                localCheckpointTracker::getProcessedCheckpoint,
                this::hasBeenProcessedBefore,
                this::compareOpToDocBasedOnSeqNo,
                this::resolveDocVersion,
                this::updateAutoIdTimestamp,
                this::tryAcquireInFlightDocs
            );
            this.throttle = new IndexThrottle();
            this.historyUUID = loadHistoryUUID(userData);
            this.mergeHandler = new CompositeMergeHandler(this, this.engine, this.engine.getDataFormat(), indexSettings, shardId);
            this.mergeScheduler = new MergeScheduler(this.mergeHandler, this, shardId, indexSettings);

            // Refresh here so that catalog snapshot gets initialized
            // TODO : any better way to do this ?
            refresh("start");
            // TODO : how to extend this for Lucene ? where engine is a r/w engine
            // Create read specific engines for each format which is associated with shard
            List<SearchEnginePlugin> searchEnginePlugins = pluginsService.filterPlugins(SearchEnginePlugin.class);
            for (SearchEnginePlugin searchEnginePlugin : searchEnginePlugins) {
                for (DataFormat dataFormat : searchEnginePlugin.getSupportedFormats()) {
                    List<SearchExecEngine<?, ?, ?, ?>> currentSearchEngines = readEngines.getOrDefault(dataFormat, new ArrayList<>());
                    SearchExecEngine<?, ?, ?, ?> newSearchEngine =
                        searchEnginePlugin.createEngine(dataFormat, Collections.emptyList(), shardPath);

                    currentSearchEngines.add(newSearchEngine);
                    readEngines.put(dataFormat, currentSearchEngines);

                    // TODO : figure out how to do internal and external refresh listeners
                    // Maybe external refresh should be managed in opensearch core and plugins should always give
                    // internal refresh managers
                    // 60s as refresh interval -> ExternalReaderManager acquires a view every 60 seconds
                    // InternalReaderManager -> IndexingMemoryController , it keeps on refreshing internal maanger
                    //
                    if (newSearchEngine.getRefreshListener(Engine.SearcherScope.INTERNAL) != null) {
                        catalogSnapshotAwareRefreshListeners.add(newSearchEngine.getRefreshListener(Engine.SearcherScope.INTERNAL));
                    }

                    if (newSearchEngine.getFileDeletionListener(Engine.SearcherScope.INTERNAL) != null) {
                        fileDeletionListeners.computeIfAbsent(dataFormat.getName(), k -> new ArrayList<>())
                            .add(newSearchEngine.getFileDeletionListener(Engine.SearcherScope.INTERNAL));
                    }
                }
            }
            catalogSnapshotAwareRefreshListeners.forEach(refreshListener -> POST_REFRESH_CATALOG_SNAPSHOT_AWARE_LISTENER_CONSUMER.accept(
                acquireSnapshot(),
                refreshListener
            ));
            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(committerRef, translogManagerRef);
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new CompositeEngine");

        initializeRefreshListeners(engineConfig);
    }

    private LocalCheckpointTracker createLocalCheckpointTracker(
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier
    ) throws IOException {
        final long maxSeqNo;
        final long localCheckpoint;
        final SequenceNumbers.CommitInfo seqNoStats =
            SequenceNumbers.loadSeqNoInfoFromLuceneCommit(store.readLastCommittedSegmentsInfo().getUserData().entrySet());
        maxSeqNo = seqNoStats.maxSeqNo;
        localCheckpoint = seqNoStats.localCheckpoint;
        logger.trace("recovered maximum sequence number [{}] and local checkpoint [{}]", maxSeqNo, localCheckpoint);
        return localCheckpointTrackerSupplier.apply(maxSeqNo, localCheckpoint);
    }

    protected TranslogDeletionPolicy getTranslogDeletionPolicy(EngineConfig engineConfig) {
        TranslogDeletionPolicy customTranslogDeletionPolicy = null;
        if (engineConfig.getCustomTranslogDeletionPolicyFactory() != null) {
            customTranslogDeletionPolicy = engineConfig.getCustomTranslogDeletionPolicyFactory()
                .create(engineConfig.getIndexSettings(), engineConfig.retentionLeasesSupplier());
        }
        return Objects.requireNonNullElseGet(
            customTranslogDeletionPolicy, () -> new DefaultTranslogDeletionPolicy(
                engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
                engineConfig.getIndexSettings().getTranslogRetentionTotalFiles()
            )
        );
    }

    protected TranslogManager createTranslogManager(
        String translogUUID,
        TranslogDeletionPolicy translogDeletionPolicy,
        CompositeTranslogEventListener translogEventListener
    ) throws IOException {
        return new InternalTranslogManager(
            engineConfig.getTranslogConfig(),
            engineConfig.getPrimaryTermSupplier(),
            engineConfig.getGlobalCheckpointSupplier(),
            translogDeletionPolicy,
            shardId,
            readLock,
            this::getLocalCheckpointTracker,
            translogUUID,
            translogEventListener,
            this::ensureOpen,
            engineConfig.getTranslogFactory(),
            engineConfig.getStartedPrimarySupplier(),
            TranslogOperationHelper.create(engineConfig)
        );
    }

    @Override
    public void ensureOpen() {

    }

    LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    public void updateSearchEngine() throws IOException {
        catalogSnapshotAwareRefreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true, catalogSnapshot);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Initialize refresh listeners from EngineConfig after all dependencies are ready.
     * This method should be called after remote store stats trackers have been created.
     * ToDo: Added as part of upload flow test, Need to discuss.
     */
    public void initializeRefreshListeners(EngineConfig engineConfig) {
        // Add EngineConfig refresh listeners to catalogSnapshotAwareRefreshListeners
        if (engineConfig.getInternalRefreshListener() != null) {
            for (ReferenceManager.RefreshListener listener : engineConfig.getInternalRefreshListener()) {
                if (listener instanceof CatalogSnapshotAwareRefreshListener) {
                    catalogSnapshotAwareRefreshListeners.add((CatalogSnapshotAwareRefreshListener) listener);
                }
            }
        }

        // Also check external refresh listeners
        if (engineConfig.getExternalRefreshListener() != null) {
            for (ReferenceManager.RefreshListener listener : engineConfig.getExternalRefreshListener()) {
                if (listener instanceof CatalogSnapshotAwareRefreshListener) {
                    catalogSnapshotAwareRefreshListeners.add((CatalogSnapshotAwareRefreshListener) listener);
                }
            }
        }

        logger.trace("CompositeEngine initialized with {} catalog snapshot aware refresh listeners", catalogSnapshotAwareRefreshListeners.size());
    }

    public SearchExecEngine<?, ?, ?, ?> getReadEngine(DataFormat dataFormat) {
        return readEngines.getOrDefault(dataFormat, new ArrayList<>()).getFirst();
    }

    public SearchExecEngine<?, ?, ?, ?> getPrimaryReadEngine() {
        // Return the first available ReadEngine as primary
        return readEngines.values().stream().filter(list -> !list.isEmpty()).findFirst().map(List::getFirst).orElse(null);
    }

    @Override
    public CompositeDataFormatWriter.CompositeDocumentInput documentInput() {
        return engine.createCompositeWriter().newDocumentInput();
    }

    public Engine.IndexResult index(Engine.Index index) throws IOException {
        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
            try (Releasable indexThrottle = doThrottle ? throttle.acquireThrottle() : () -> {}) {
                lastWriteNanos = index.startTime();
                final IndexingStrategy plan = indexingStrategyForOperation(index);
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

                    if (plan.executeOpOnEngine || plan.optimizeAppendOnly) {
                        index.documentInput.setSeqNo(index.seqNo());
                        index.documentInput.setPrimaryTerm(SeqNoFieldMapper.PRIMARY_TERM_NAME, index.primaryTerm());
                        index.documentInput.setVersion(1); // we are not supporting update in parquet
                        WriteResult writeResult = index.documentInput.addToWriter();
                        indexResult =
                            new Engine.IndexResult(writeResult.version(), index.primaryTerm(), index.seqNo(), writeResult.success());
                    } else {
                        indexResult =
                            new Engine.IndexResult(plan.version, index.primaryTerm(), index.seqNo(), plan.currentNotFoundOrDeleted);
                    }
                }

                if (index.origin().isFromTranslog() == false) {
                    final Translog.Location location;
                    if (indexResult.getResultType() == Engine.Result.Type.SUCCESS) {
                        location = translogManager.add(new Translog.Index(index, indexResult));
                    } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && indexResult.getFailure() != null
                        && !(indexResult.getFailure() instanceof AppendOnlyIndexOperationRetryException)) {
                        throw new UnsupportedOperationException("recording document failure as a no-op in translog is not supported");
                    } else {
                        location = null;
                    }
                    indexResult.setTranslogLocation(location);
                }
                localCheckpointTracker.markSeqNoAsProcessed(indexResult.getSeqNo());
                if (indexResult.getTranslogLocation() == null && !(indexResult.getFailure() != null
                    && (indexResult.getFailure() instanceof AppendOnlyIndexOperationRetryException))) {
                    // the op is coming from the translog (and is hence persisted already) or it does not have a sequence number
                    assert index.origin().isFromTranslog() || indexResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
                    localCheckpointTracker.markSeqNoAsPersisted(indexResult.getSeqNo());
                }
                indexResult.setTook(System.nanoTime() - index.startTime());
                indexResult.freeze();
                return indexResult;
            }
        } catch (RuntimeException | IOException e) {
            try {
                if (e instanceof AlreadyClosedException == false && treatDocumentFailureAsTragicError(index)) {
                    failEngine("index id[" + index.id() + "] origin[" + index.origin() + "] seq#[" + index.seqNo() + "]", e);
                } else {
                    maybeFailEngine("index id[" + index.id() + "] origin[" + index.origin() + "] seq#[" + index.seqNo() + "]", e);
                }
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    private IndexingStrategy indexingStrategyForOperation(final Engine.Index index) throws IOException {
        if (index.origin() == Engine.Operation.Origin.PRIMARY) {
            return indexingStrategyPlanner.planOperationAsPrimary(index);
        } else {
            // non-primary mode (i.e., replica or recovery)
            return indexingStrategyPlanner.planOperationAsNonPrimary(index);
        }
    }

    private OpVsEngineDocStatus compareOpToDocBasedOnSeqNo(final Engine.Operation op) {
        return OpVsEngineDocStatus.OP_NEWER;
    }

    /** resolves the current version of the document, returning null if not found */
    private VersionValue resolveDocVersion(final Engine.Operation op, boolean loadSeqNo) {
        return null;
    }

    /**
     * Checks if the given operation has been processed in this engine or not.
     * @return true if the given operation was processed; otherwise false.
     */
    private boolean hasBeenProcessedBefore(Engine.Operation op) {
        assert !Assertions.ENABLED || op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "operation is not assigned seq_no";
        return localCheckpointTracker.hasProcessed(op.seqNo());
    }

    private long generateSeqNoForOperationOnPrimary(final Engine.Operation operation) {
        assert operation.origin() == Engine.Operation.Origin.PRIMARY;
        assert
            operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO :
            "ops should not have an assigned seq no. but was: " + operation.seqNo();
        return doGenerateSeqNoForOperation(operation);
    }

    /**
     * Generate the sequence number for the specified operation.
     *
     * @param operation the operation
     * @return the sequence number
     */
    public long doGenerateSeqNoForOperation(final Engine.Operation operation) {
        return localCheckpointTracker.generateSeqNo();
    }

    private Exception tryAcquireInFlightDocs(Engine.Operation operation, Integer integer) {
        // TODO - in flight document handling
        return null;
    }

    /**
     * Marks the given seq_no as seen and advances the max_seq_no of this engine to at least that value.
     */
    protected final void markSeqNoAsSeen(long seqNo) {
        localCheckpointTracker.advanceMaxSeqNo(seqNo);
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
    public long getMinRetainedSeqNo() {
        return -1;
    }

    @Override
    public final long getMaxSeenAutoIdTimestamp() {
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
        assert maxUnsafeAutoIdTimestamp.get() <= maxSeenAutoIdTimestamp.get();
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes.get();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        // Noop since we're not supporting updates or deletes yet.
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return throttle.getThrottleTimeInMillis();
    }

    @Override
    public boolean isThrottled() {
        return throttle.isThrottled();
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

    public synchronized void refresh(String source) throws EngineException {
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = catalogSnapshotManager.acquireSnapshot()) {
            refreshListeners.forEach(PRE_REFRESH_LISTENER_CONSUMER);

    public synchronized void refresh(String source, Map<DataFormat, RefreshInput> refreshInputs) throws EngineException {
        refreshListeners.forEach(ref -> {
            try {
                ref.beforeRefresh();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        long id = 0L;
        long version = 0L;
        if (catalogSnapshot != null) {
            id = catalogSnapshot.getId();
            version = catalogSnapshot.getVersion();

        }
        CatalogSnapshot newCatSnap;
        try {
            RefreshResult refreshResult;
            if(source.equals("merge")) {
                refreshResult = engine.refresh(refreshInputs);
            } else {
                refreshResult = engine.refresh(new RefreshInput());
                if (refreshResult == null) {
                    return;
                }
            }
            newCatSnap = new CatalogSnapshot(refreshResult, id + 1L, version + 1L);
            System.out.println("CATALOG SNAPSHOT: " + newCatSnap);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        newCatSnap.incRef();
        if (catalogSnapshot != null) {
            catalogSnapshot.decRef();
        }
        catalogSnapshot = newCatSnap;
        compositeEngineCommitter.addLuceneIndexes(catalogSnapshot);

        catalogSnapshotAwareRefreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true, catalogSnapshot);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        refreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // trigger merges
        triggerPossibleMerges();
    }

    public void applyMergeChanges(MergeResult mergeResult, OneMerge oneMerge) {
        Map<DataFormat, RefreshInput> refreshInputs = new HashMap<>();

        Map<DataFormat, Collection<FileMetadata>> mergedFileMetadata = mergeResult.getMergedFileMetadata();
        for(DataFormat dataFormat : mergedFileMetadata.keySet()) {
            Collection<FileMetadata> mergedFiles = mergedFileMetadata.get(dataFormat);
            RefreshInput refreshInput = new RefreshInput();
            refreshInput.setExistingSegments(catalogSnapshotReleasableRef.getRef().getSegments());
            RefreshResult refreshResult = engine.refresh(refreshInput);
            if (refreshResult == null) {
                return;
            }
            catalogSnapshotManager.applyRefreshResult(refreshResult);
            catalogSnapshotAwareRefreshListeners.forEach(refreshListener -> POST_REFRESH_CATALOG_SNAPSHOT_AWARE_LISTENER_CONSUMER.accept(
                acquireSnapshot(),
                refreshListener
            ));

            refreshListeners.forEach(POST_REFRESH_LISTENER_CONSUMER);
            triggerPossibleMerges(); // trigger merges
        } catch (Exception ex) {
            try {
                failEngine("refresh failed source[" + source + "]", ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, ex);
        }
    }

    public synchronized void applyMergeChanges(MergeResult mergeResult, OneMerge oneMerge) {
        catalogSnapshotManager.applyMergeResults(mergeResult, oneMerge);
    }

    public void triggerPossibleMerges() {
        try {
            mergeScheduler.triggerMerges();
        } catch (IOException e) {
            System.out.println("ERROR in MERGE : " + e.getMessage());
            e.printStackTrace();
        }
    }


    public synchronized void setCatalogSnapshot(CatalogSnapshot catalogSnapshot, ShardPath shardPath) {
        CatalogSnapshot oldSnapshot = this.catalogSnapshot;

        if (catalogSnapshot != null) {
            catalogSnapshot.incRef();
            this.catalogSnapshot = catalogSnapshot.remapPaths(shardPath.getDataPath());
        } else {
            this.catalogSnapshot = null;
        }

        if (oldSnapshot != null) {
            oldSnapshot.decRef();
        }
    }

    // This should get wired into searcher acquireSnapshot for initializing reader context later
    // this now becomes equivalent of the reader
    // Each search side specific impl can decide on how to init specific reader instances using this pit snapshot provided by writers
    public ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        if (catalogSnapshot == null) {
            return new ReleasableRef<CatalogSnapshot>(null) {
                @Override
                public void close() {
                    // No-op for null
                }
            };
        }

        final CatalogSnapshot snapshot = catalogSnapshot;
        snapshot.incRef();
        return new ReleasableRef<>(snapshot) {
            @Override
            public void close() {
                snapshot.decRef();
            }
        }
    }

    @ExperimentalApi
    public static abstract class ReleasableRef<T> implements AutoCloseable {

        private final T t;

        public ReleasableRef(T t) {
            this.t = t;
        }

        public T getRef() {
            return t;
        }
    }

    public long getNativeBytesUsed() {
        return engine.getNativeBytesUsed();
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        return null;
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        return null;
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
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return List.of();
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
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
        mergeScheduler.forceMerge(maxNumSegments);
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        refresh("write indexing buffer");
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        if (catalogSnapshot != null) {
            catalogSnapshot.changed();
        }
        compositeEngineCommitter.commit(catalogSnapshot);
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return compositeEngineCommitter.getSafeCommitInfo();
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return null;
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return null;
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    /**
     * Flush the engine (committing segments to disk and truncating the translog) and close it.
     */
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
                        flush(false, true);
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

    private boolean shouldFlush() {
        long currentSnapshotIdToFlush = -1, lastCommitedSnapshotId = -1;
        try (ReleasableRef<CatalogSnapshot> catalogSnapshotToFlushRef = catalogSnapshotManager.acquireSnapshot()) {
            if (catalogSnapshotToFlushRef != null && catalogSnapshotToFlushRef.getRef() != null)
                currentSnapshotIdToFlush = catalogSnapshotToFlushRef.getRef().getId();
            if (lastCommitedCatalogSnapshotRef != null && lastCommitedCatalogSnapshotRef.getRef() != null)
                lastCommitedSnapshotId = lastCommitedCatalogSnapshotRef.getRef().getId();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return (currentSnapshotIdToFlush != -1) && (currentSnapshotIdToFlush != lastCommitedSnapshotId);
    }

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        if (translogManager.getTragicExceptionIfClosed() != null) {
            failEngine("already closed by tragic event on the translog", translogManager.getTragicExceptionIfClosed());
            engineFailed = true;
        } else if (failedEngine.get() == null && isClosed.get() == false) {
            // this smells like a bug - we only expect ACE if we are in a fatal case ie. translog is closed by
            // a tragic event or has closed itself. if that is not the case we are in a buggy state and raise an assertion error
            throw new AssertionError("Unexpected AlreadyClosedException", ex);
        } else {
            engineFailed = false;
        }
        return engineFailed;
    }

    private boolean maybeFailEngine(String source, Exception e) {
        // Check for AlreadyClosedException -- ACE is a very special
        // exception that should only be thrown in a tragic event. we pass on the checks to failOnTragicEvent which will
        // throw and AssertionError if the tragic event condition is not met.
        if (e instanceof AlreadyClosedException) {
            return failOnTragicEvent((AlreadyClosedException) e);
        } else if (e != null && (translogManager.getTragicExceptionIfClosed() == e || e instanceof UnsupportedOperationException)) {
            // this spot on - we are handling the tragic event exception here so we have to fail the engine right away
            failEngine(source, e);
            return true;
        }
        return false;
    }

    @Override
    public void failEngine(String reason, @Nullable Exception failure) {
        if (failure != null) {
            maybeDie(logger, reason, failure);
        }
        if (failEngineLock.tryLock()) {
            try {
                if (failedEngine.get() != null) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "tried to fail composite engine but it is already failed. ignoring. [{}]",
                            reason
                        ),
                        failure
                    );
                    return;
                }
                // this must happen before we close translog such that we can check this state to opt out of failing the engine
                // again on any caught AlreadyClosedException
                failedEngine.set((failure != null) ? failure : new IllegalStateException(reason));
                try {
                    closeNoLock("composite engine failed on: [" + reason + "]", closedLatch);
                } finally {
                    logger.warn(() -> new ParameterizedMessage("failed composite engine [{}]", reason), failure);
                    eventListener.onFailedEngine(reason, failure);
                }
            } catch (Exception inner) {
                if (failure != null) inner.addSuppressed(failure);
                logger.warn("failEngine threw exception", inner); // don't bubble up these exceptions up
            }
        } else {
            logger.debug(
                () -> new ParameterizedMessage(
                    "tried to fail composite engine but could not acquire lock - composite engine should " + "be failed by now [{}]",
                    reason
                ), failure
            );
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) { // don't acquire the write lock if we are already closed
            logger.debug("close now acquiring writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.debug("close acquired writeLock");
                closeNoLock("api", closedLatch);
            }
        }
        awaitPendingClose();
    }

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the engine without acquiring the write lock. This should only be
     * called while the write lock is hold or in a disaster condition ie. if the engine
     * is failed.
     */
    private void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread()
                || failEngineLock.isHeldByCurrentThread() : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                try {
                    IOUtils.close(engine, translogManager, compositeEngineCommitter);
                } catch (Exception e) {
                    logger.warn("Failed to close translog", e);
                }
            } catch (Exception e) {
                logger.warn("failed to close translog manager", e);
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

    /**
     * Acquires the most recent safe index commit snapshot from the currently running engine.
     * All index files referenced by this commit won't be freed until the commit/snapshot is closed.
     * This method is required for replica recovery operations.
     */
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        ensureOpen();
        if (compositeEngineCommitter instanceof LuceneCommitEngine) {
            LuceneCommitEngine luceneCommitEngine = (LuceneCommitEngine) compositeEngineCommitter;
            // Delegate to the LuceneCommitEngine's acquireSafeIndexCommit method
            return luceneCommitEngine.acquireSafeIndexCommit();
        } else {
            throw new EngineException(shardId, "CompositeEngine committer is not a LuceneCommitEngine");
        }
    }
}
