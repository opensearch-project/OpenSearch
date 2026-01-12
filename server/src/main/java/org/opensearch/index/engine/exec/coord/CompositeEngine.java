/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.Assertions;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.AppendOnlyIndexOperationRetryException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.FileDeletionListener;
import org.opensearch.index.engine.FlushFailedEngineException;
import org.opensearch.index.engine.IndexThrottle;
import org.opensearch.index.engine.IndexingStrategy;
import org.opensearch.index.engine.IndexingStrategyPlanner;
import org.opensearch.index.engine.LifecycleAware;
import org.opensearch.index.engine.LiveVersionMap;
import org.opensearch.index.engine.MergeFailedEngineException;
import org.opensearch.index.engine.RefreshFailedEngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.engine.VersionValue;
import org.opensearch.index.engine.*;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FileStats;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.bridge.CheckpointState;
import org.opensearch.index.engine.exec.bridge.Indexer;
import org.opensearch.index.engine.exec.bridge.Indexer.OpVsEngineDocStatus;
import org.opensearch.index.engine.exec.bridge.IndexingThrottler;
import org.opensearch.index.engine.exec.bridge.StatsHolder;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.LuceneCommitEngine;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.CompositeEngine.ReleasableRef;
import org.opensearch.index.engine.exec.merge.CompositeMergeHandler;
import org.opensearch.index.engine.exec.merge.MergeHandler;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.MergeScheduler;
import org.opensearch.index.engine.exec.merge.OneMerge;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Checkpoint;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogHeader;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.listener.CompositeTranslogEventListener;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.plugins.spi.vectorized.DataFormat;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.function.Supplier;

import static org.opensearch.index.engine.Engine.HISTORY_UUID_KEY;
import static org.opensearch.index.engine.Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID;
import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.CATALOG_SNAPSHOT_KEY;
import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY;
import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.*;

@ExperimentalApi
public class CompositeEngine implements LifecycleAware, Closeable, Indexer, CheckpointState, IndexingThrottler, StatsHolder {

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
    private static final BiConsumer<Supplier<ReleasableRef<CatalogSnapshot>>, CatalogSnapshotAwareRefreshListener>
        POST_REFRESH_CATALOG_SNAPSHOT_AWARE_LISTENER_CONSUMER = (catalogSnapshot, catalogSnapshotAwareRefreshListener) -> {
        try {
            catalogSnapshotAwareRefreshListener.afterRefresh(true, catalogSnapshot);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };
    private static final Function<String, String> extractSegmentName = name -> name.substring(name.lastIndexOf('_'), name.lastIndexOf('.'));

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
    private final Map<DataFormat, List<SearchExecEngine<?, ?, ?, ?>>> readEngines = new HashMap<>();
    private final MergeScheduler mergeScheduler;
    private final MergeHandler mergeHandler;

    @Nullable
    protected final String historyUUID;

    private final LocalCheckpointTracker localCheckpointTracker;
    private final LastRefreshedCheckpointListener lastRefreshedCheckpointListener;
    private final ReentrantLock failEngineLock = new ReentrantLock();
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    private final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());
    private final Lock flushLock = new ReentrantLock();
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    private final IndexThrottle throttle;
    // How many callers are currently requesting index throttling. Currently, there are only two situations where we do this: when merges
    // are falling behind and when writing indexing buffer to disk is too slow. When this is 0, there is no throttling, else we throttling
    // incoming indexing ops to a single thread:
    private final AtomicInteger throttleRequestCount = new AtomicInteger();
    /*
     * on {@code lastWriteNanos} we use System.nanoTime() to initialize this since:
     *  - we use the value for figuring out if the shard / engine is active so if we startup and no write has happened yet we still
     *    consider it active for the duration of the configured active to inactive period. If we initialize to 0 or Long.MAX_VALUE we
     *    either immediately or never mark it inactive if no writes at all happen to the shard.
     *  - we also use this to flush big-ass merges on an inactive engine / shard but if we we initialize 0 or Long.MAX_VALUE we either
     *    immediately or never commit merges even though we shouldn't from a user perspective (this can also have funky side effects in
     *    tests when we open indices with lots of segments and suddenly merges kick in.
     *  NOTE: don't use this value for anything accurate it's a best effort for freeing up diskspace after merges and on a shard level to
     *  reduce index buffer sizes on inactive shards.
     */
    private volatile long lastWriteNanos = System.nanoTime();
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final AtomicLong maxSeenAutoIdTimestamp = new AtomicLong(-1);
    // max_seq_no_of_updates_or_deletes tracks the max seq_no of update or delete operations that have been processed in this engine.
    // An index request is considered as an update if it overwrites existing documents with the same docId in the Lucene index.
    // The value of this marker never goes backwards, and is tracked/updated differently on primary and replica.
    private final AtomicLong maxSeqNoOfUpdatesOrDeletes;
    private final IndexingStrategyPlanner indexingStrategyPlanner;
    private final CatalogSnapshotManager catalogSnapshotManager;
    private ReleasableRef<CatalogSnapshot> lastCommitedCatalogSnapshotRef;
    private final EventListener eventListener;

    public CompositeEngine(
        EngineConfig engineConfig,
        MapperService mapperService,
        PluginsService pluginsService,
        IndexSettings indexSettings,
        ShardPath shardPath,
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        TranslogEventListener translogEventListener
    ) {
        this.logger = Loggers.getLogger(CompositeEngine.class, engineConfig.getShardId());
        this.engineConfig = engineConfig;
        this.eventListener = engineConfig.getEventListener();
        this.store = engineConfig.getStore();
        this.shardId = engineConfig.getShardId();
        final TranslogDeletionPolicy translogDeletionPolicy = getTranslogDeletionPolicy(engineConfig);
        Committer committerRef = null;
        TranslogManager translogManagerRef = null;
        boolean success = false;
        try {
            this.store.incRef();
            if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
                updateAutoIdTimestamp(Long.MAX_VALUE, true);
            }
            // initialize local checkpoint tracker and translog manager
            this.localCheckpointTracker = createLocalCheckpointTracker(localCheckpointTrackerSupplier);
            this.lastRefreshedCheckpointListener = new LastRefreshedCheckpointListener(localCheckpointTracker);
            refreshListeners.add(lastRefreshedCheckpointListener);
            Map<String, String> userData;
            String translogUUID;
            // Note: lastRefreshedCheckpointListener is initialized later after localCheckpointTracker is ready
            try {
                final SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
                userData = segmentInfos.getUserData();
                logger.info("[COMPOSITE ENGINE STARTUP] Read userData from Lucene commit: keys={}", userData.keySet());
                logger.info("[COMPOSITE ENGINE STARTUP] CATALOG_SNAPSHOT_KEY present={}, LAST_COMPOSITE_WRITER_GEN_KEY present={}",
                           userData.containsKey(CATALOG_SNAPSHOT_KEY), userData.containsKey(LAST_COMPOSITE_WRITER_GEN_KEY));
                if (userData.containsKey(LAST_COMPOSITE_WRITER_GEN_KEY)) {
                    logger.info("[COMPOSITE ENGINE STARTUP] LAST_COMPOSITE_WRITER_GEN_KEY value={}",
                               userData.get(LAST_COMPOSITE_WRITER_GEN_KEY));
                }
                translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));
            } catch (java.io.FileNotFoundException e) {
                // Local store is empty (remote store recovery scenario)
                logger.debug("Local store is empty, reading translog UUID from translog header and creating initial commit");
                final Path translogPath = engineConfig.getTranslogConfig().getTranslogPath();
                final Checkpoint checkpoint = Checkpoint.read(translogPath.resolve(Translog.CHECKPOINT_FILE_NAME));
                final Path translogFile = translogPath.resolve(Translog.getFilename(checkpoint.getGeneration()));
                try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(translogFile, java.nio.file.StandardOpenOption.READ)) {
                    final TranslogHeader translogHeader = TranslogHeader.read(translogFile, channel);
                    translogUUID = translogHeader.getTranslogUUID();

                    // Create initial empty commit for LuceneCommitEngine
                    store.createEmpty(engineConfig.getIndexSettings().getIndexVersionCreated().luceneVersion, translogUUID);

                    // Now read the userData from the newly created commit
                    userData = store.readLastCommittedSegmentsInfo().getUserData();
                    logger.debug("Created initial empty commit with translog UUID: {}", translogUUID);
                }
            }
            TranslogEventListener internalTranslogEventListener = new TranslogEventListener() {
                @Override
                public void onAfterTranslogSync() {
                    try {
                        translogManager.trimUnreferencedReaders();
                    } catch (IOException ex) {
                        throw new TranslogException(shardId, "Failed to trim unreferenced translog generations on translog synced", ex);
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
                lastCommittedWriterGeneration.set(Long.parseLong(lastCommittedData.get(LAST_COMPOSITE_WRITER_GEN_KEY)));
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
                CatalogSnapshot loadedSnapshot = catalogSnapshotReleasableRef.getRef();
                this.engine.loadWriterFiles(loadedSnapshot);

                if (loadedSnapshot != null) {
                    long snapshotLastWriterGen = loadedSnapshot.getLastWriterGeneration();
                    engine.updateWriterGenerationIfNeeded(snapshotLastWriterGen);
                }
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
            initializeRefreshListeners(engineConfig);
            refresh("start");
            // TODO : how to extend this for Lucene ? where engine is a r/w engine
            // Create read specific engines for each format which is associated with shard
            List<SearchEnginePlugin> searchEnginePlugins = pluginsService.filterPlugins(SearchEnginePlugin.class);
            for (SearchEnginePlugin searchEnginePlugin : searchEnginePlugins) {
                for (DataFormat dataFormat : searchEnginePlugin.getSupportedFormats()) {
                    List<SearchExecEngine<?, ?, ?, ?>> currentSearchEngines = readEngines.getOrDefault(dataFormat, new ArrayList<>());

                    // Get FileMetadata filtered by data format from current catalog snapshot
                    Collection<FileMetadata> formatFiles;
                    try (ReleasableRef<CatalogSnapshot> snapshotRef = acquireSnapshot()) {
                        CatalogSnapshot snapshot = snapshotRef.getRef();
                        formatFiles = snapshot.getFileMetadataList().stream()
                            .filter(fm -> fm.dataFormat().equals(dataFormat.getName()))
                            .collect(Collectors.toList());
                    } catch (Exception e) {
                        throw new EngineCreationFailureException(shardId, "failed to acquire catalog snapshot for read engine creation", e);
                    }

                    SearchExecEngine<?, ?, ?, ?> newSearchEngine =
                        searchEnginePlugin.createEngine(dataFormat, formatFiles, shardPath);

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
                this::acquireSnapshot,
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
    }

    private LocalCheckpointTracker createLocalCheckpointTracker(
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier
    ) throws IOException {
        final long maxSeqNo;
        final long localCheckpoint;

        try {
            final SequenceNumbers.CommitInfo seqNoStats =
                SequenceNumbers.loadSeqNoInfoFromLuceneCommit(store.readLastCommittedSegmentsInfo().getUserData().entrySet());
            maxSeqNo = seqNoStats.maxSeqNo;
            localCheckpoint = seqNoStats.localCheckpoint;
            logger.trace("recovered maximum sequence number [{}] and local checkpoint [{}]", maxSeqNo, localCheckpoint);
        } catch (org.apache.lucene.index.IndexNotFoundException e) {
            // Local store is empty (remote store recovery scenario)
            // Initialize with NO_OPS_PERFORMED (-1) - checkpoint will be restored from CatalogSnapshot during first flush
            logger.debug(
                "Local store is empty during engine initialization, initializing checkpoint tracker with NO_OPS_PERFORMED. "
                + "This is expected during remote store recovery where local store has not been initialized yet."
            );
            return localCheckpointTrackerSupplier.apply(
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED
            );
        }

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

    public final EngineConfig config()
    {
        return engineConfig;
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
            this,
            engineConfig.getTranslogFactory(),
            engineConfig.getStartedPrimarySupplier(),
            TranslogOperationHelper.create(engineConfig)
        );
    }

    @Override
    public void ensureOpen() {
        if (isClosed.get()) {
            throw new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
        }
    }

    public LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    public void updateSearchEngine() throws IOException {
            catalogSnapshotAwareRefreshListeners.forEach(ref -> {
            try {
                ref.afterRefresh(true, catalogSnapshotManager::acquireSnapshot);
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

        logger.trace(
            "CompositeEngine initialized with {} catalog snapshot aware refresh listeners",
            catalogSnapshotAwareRefreshListeners.size()
        );
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
        final long localCheckpointBeforeRefresh = localCheckpointTracker.getProcessedCheckpoint();
        boolean refreshed = false;
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = catalogSnapshotManager.acquireSnapshot()) {
            refreshListeners.forEach(PRE_REFRESH_LISTENER_CONSUMER);

            // Call checkpoint listener's beforeRefresh to capture pending checkpoint
            lastRefreshedCheckpointListener.beforeRefresh();

            RefreshInput refreshInput = new RefreshInput();
            refreshInput.setExistingSegments(new ArrayList<>(catalogSnapshotReleasableRef.getRef().getSegments()));
            RefreshResult refreshResult = engine.refresh(refreshInput);
            if (refreshResult == null) {
                return;
            }
            catalogSnapshotManager.applyRefreshResult(refreshResult);
            refreshed = true;

            catalogSnapshotAwareRefreshListeners.forEach(refreshListener -> POST_REFRESH_CATALOG_SNAPSHOT_AWARE_LISTENER_CONSUMER.accept(
                this::acquireSnapshot,
                refreshListener
            ));

            refreshListeners.forEach(POST_REFRESH_LISTENER_CONSUMER);

            // Call checkpoint listener's afterRefresh to update refreshed checkpoint
            if (refreshed) {
                lastRefreshedCheckpointListener.afterRefresh(true);
            }

            triggerPossibleMerges(); // trigger merges
        } catch (Exception ex) {
            try {
                failEngine("refresh failed source[" + source + "]", ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, ex);
        }

        assert refreshed == false || lastRefreshedCheckpoint() >= localCheckpointBeforeRefresh : "refresh checkpoint was not advanced; "
            + "local_checkpoint="
            + localCheckpointBeforeRefresh
            + " refresh_checkpoint="
            + lastRefreshedCheckpoint();
    }

    public synchronized void applyMergeChanges(MergeResult mergeResult, OneMerge oneMerge) {
        try {
            catalogSnapshotManager.applyMergeResults(mergeResult, oneMerge);
        } catch (Exception ex) {
            try {
                logger.error(
                    () -> new ParameterizedMessage(
                        "Merge failed while registering merged files in Snapshot"
                    ),
                    ex
                );
                failEngine("Merge failed while registering merged files in Snapshot", ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw new MergeFailedEngineException(shardId, ex);
        }
    }

    public void triggerPossibleMerges() {
        mergeScheduler.triggerMerges();
    }

    public void finalizeReplication(CatalogSnapshot catalogSnapshot, ShardPath shardPath) throws IOException {
        catalogSnapshotManager.applyReplicationChanges(catalogSnapshot, shardPath);

        if (catalogSnapshot != null) {
            long maxGenerationInSnapshot = catalogSnapshot.getLastWriterGeneration();
            engine.updateWriterGenerationIfNeeded(maxGenerationInSnapshot);
        }

        updateSearchEngine();
    }

    // This should get wired into searcher acquireSnapshot for initializing reader context later
    // this now becomes equivalent of the reader
    // Each search side specific impl can decide on how to init specific reader instances using this pit snapshot provided by writers
    public ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        return this.catalogSnapshotManager.acquireSnapshot();
    }

    // Notifies composite execution engine to delete dataformat specific files
    public void notifyDelete(Map<String, Collection<String>> dfFilesToDelete) throws IOException {
        // notify engine to delete all files
        engine.deleteFiles(dfFilesToDelete);
        // trigger postDelete hooks for fileDeletionListeners
        for (String dataFormat : dfFilesToDelete.keySet()) {
            if (fileDeletionListeners.get(dataFormat) == null) continue;
            for (FileDeletionListener fileDeletionListener : fileDeletionListeners.get(dataFormat)) {
                fileDeletionListener.onFileDeleted(dfFilesToDelete.get(dataFormat));
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
        try {
            List<Segment> segments = new ArrayList<>();
            Set<Long> committedSegments = new HashSet<>();
            if (lastCommitedCatalogSnapshotRef != null && lastCommitedCatalogSnapshotRef.getRef() != null) {
                lastCommitedCatalogSnapshotRef.getRef()
                    .getSegments()
                    .stream()
                    .map(org.opensearch.index.engine.exec.coord.Segment::getGeneration)
                    .collect(Collectors.toCollection(() -> committedSegments));
            }
            Map<String, FileStats> segmentStats = getPrimaryReadEngine().fetchSegmentStats();
            segmentStats.forEach((name, fileStats) -> {
                Segment segment = new Segment(extractSegmentName.apply(name));
                segment.docCount = Math.toIntExact(fileStats.getDocCount());
                segment.sizeInBytes = fileStats.getSize();
                segment.search = true;
                segment.committed = committedSegments.contains(segment.getGeneration());
                segment.version = null; // not implemented since it refers lucene version
                segment.delDocCount = 0; // deletion not supported yet
                segments.add(segment);
            });
            return List.copyOf(segments);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public long lastRefreshedCheckpoint() {
        return lastRefreshedCheckpointListener.getRefreshedCheckpoint();
    }

    @Override
    public long currentOngoingRefreshCheckpoint() {
        return lastRefreshedCheckpointListener.getPendingCheckpoint();
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
        ensureOpen();
        if (force && waitIfOngoing == false) {
            assert false : "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing;
            throw new IllegalArgumentException(
                "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing);
        }
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (flushLock.tryLock() == false) {
                // if we can't get the lock right away we block if needed otherwise barf
                if (waitIfOngoing == false) {
                    return;
                }
                logger.trace("waiting for in-flight flush to finish");
                flushLock.lock();
                logger.trace("acquired flush lock after blocking");
            } else {
                logger.trace("acquired flush lock immediately");
            }
            try {
                boolean shouldPeriodicallyFlush = shouldPeriodicallyFlush();
                if (force || shouldFlush() || shouldPeriodicallyFlush || getProcessedLocalCheckpoint() > Long.parseLong(
                    readLastCommittedData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))) {

                    logger.info(
                        "[COMPOSITE ENGINE FLUSH] Starting flush. force={}, shouldFlush={}, shouldPeriodicallyFlush={}, " +
                        "processedLocalCheckpoint={}, lastCommittedCheckpoint={}",
                        force, shouldFlush(), shouldPeriodicallyFlush,
                        getProcessedLocalCheckpoint(),
                        readLastCommittedData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)
                    );

                    translogManager.ensureCanFlush();

                    try {
                        logger.info("[COMPOSITE ENGINE FLUSH] About to roll translog generation");
                        translogManager.rollTranslogGeneration();
                        logger.info("[COMPOSITE ENGINE FLUSH] Successfully rolled translog generation");
                        logger.trace("starting commit for flush; commitTranslog=true");
                        CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotToFlushRef = catalogSnapshotManager.acquireSnapshot();
                        final CatalogSnapshot catalogSnapshotToFlush = catalogSnapshotToFlushRef.getRef();
                        System.out.println("FLUSH called, current snapshot to commit : " + catalogSnapshotToFlush.getId()
                            + ", previous commited snapshot : " + ((lastCommitedCatalogSnapshotRef != null)
                                                                   ? lastCommitedCatalogSnapshotRef.getRef().getId()
                                                                   : -1));

                        // FIX: Use MAX of engine's current counter and snapshot's lastWriterGeneration
                        // to ensure we never reuse a generation after restart.
                        // Engine counter - 1 = last assigned generation (counter points to NEXT generation)
                        final long engineLastAssignedGen = engine.getCurrentWriterGeneration() - 1;
                        final long snapshotLastWriterGen = catalogSnapshotToFlush.getLastWriterGeneration();
                        final long lastWriterGeneration = Math.max(engineLastAssignedGen, snapshotLastWriterGen);

                        logger.info("[COMPOSITE ENGINE FLUSH] Computing lastWriterGeneration: engineCounter={}, " +
                                   "engineLastAssignedGen={}, snapshotLastWriterGen={}, result={}",
                                   engine.getCurrentWriterGeneration(), engineLastAssignedGen,
                                   snapshotLastWriterGen, lastWriterGeneration);

                        final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();

                        // Create commitData with checkpoint information BEFORE serializing CatalogSnapshot
                        // This ensures CatalogSnapshot.userData contains the correct checkpoint values
                        final Map<String, String> commitData = new HashMap<>(7);
                        commitData.put(Translog.TRANSLOG_UUID_KEY, translogManager.getTranslogUUID());
                        commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
                        commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
                        commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
                        commitData.put(HISTORY_UUID_KEY, historyUUID);
                        commitData.put(LAST_COMPOSITE_WRITER_GEN_KEY, Long.toString(lastWriterGeneration));

                        // Copy checkpoint data to CatalogSnapshot.userData BEFORE serialization
                        // This preserves checkpoint state for recovery scenarios (e.g., replica promotion)
                        catalogSnapshotToFlush.setUserData(commitData, false);

                        // Now serialize CatalogSnapshot with checkpoint data in userData
                        final String serializedCatalogSnapshot = catalogSnapshotToFlush.serializeToString();
                        commitData.put(CATALOG_SNAPSHOT_KEY, serializedCatalogSnapshot);

                        compositeEngineCommitter.commit(
                            () -> commitData.entrySet().iterator(),
                            catalogSnapshotToFlush
                        );
                        logger.trace("finished commit for flush");
                        if (lastCommitedCatalogSnapshotRef != null && lastCommitedCatalogSnapshotRef.getRef() != null)
                            lastCommitedCatalogSnapshotRef.close();
                        lastCommitedCatalogSnapshotRef = catalogSnapshotToFlushRef;
                        translogManager.trimUnreferencedReaders();
                    } catch (AlreadyClosedException e) {
                        failOnTragicEvent(e);
                        throw e;
                    } catch (Exception e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                }
            } catch (FlushFailedEngineException ex) {
                maybeFailEngine("flush", ex);
                throw ex;
            } finally {
                flushLock.unlock();
            }
        }
        // We don't have to do this here; we do it defensively to make sure that even if wall clock time is misbehaving
        // (e.g., moves backwards) we will at least still sometimes prune deleted tombstones:
        if (engineConfig.isEnableGcDeletes()) {
            // TODO - pruneDeletedTombstones();
        }

    }

    @Override
    public CommitStats commitStats() {
        return compositeEngineCommitter.getCommitStats();
    }

    @Override
    public DocsStats docStats() {
        try {
            Map<String, FileStats> segmentStats = getPrimaryReadEngine().fetchSegmentStats();
            long docCount = segmentStats.values().stream().mapToLong(FileStats::getDocCount).sum();
            long size = segmentStats.values().stream().mapToLong(FileStats::getSize).sum();
            return new DocsStats(docCount, 0, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        ensureOpen();
        try {
            Map<String, FileStats> segmentStats = getPrimaryReadEngine().fetchSegmentStats();
            SegmentsStats stats = new SegmentsStats();
            segmentStats.forEach((key, value) -> {
                stats.add(1);
                if (includeSegmentFileSizes) {
                    stats.addFileSizes(segmentStats.entrySet()
                        .stream()
                        .collect(Collectors.toMap(e -> extractSegmentName.apply(e.getKey()), e -> e.getValue().getSize())));
                }
            });
            stats.addVersionMapMemoryInBytes(0);
            stats.addIndexWriterMemoryInBytes(0);
            return stats;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return null;
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
    public long getLastWriteNanos() {
        return lastWriteNanos;
    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        mergeScheduler.refreshConfig();
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        ensureOpen();
        final long localCheckpointOfLastCommit = Long.parseLong(readLastCommittedData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        return translogManager.shouldPeriodicallyFlush(
            localCheckpointOfLastCommit,
            this.engineConfig.getIndexSettings().getFlushThresholdSize().getBytes()
        );
    }

    private Map<String, String> readLastCommittedData() {
        return this.compositeEngineCommitter.getLastCommittedData();
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
        return translogManager.newChangesSnapshot(fromSeqNo, toSeqNo, requiredFullRange);
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
                    IOUtils.close(engine, translogManager, compositeEngineCommitter);
                } catch (Exception e) {
                    logger.warn("Failed to close translog", e);
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
    @Override
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
