/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.InfoStream;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.Booleans;
import org.opensearch.common.Nullable;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.LoggerInfoStream;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.merge.OnGoingMerge;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.shard.OpenSearchMergePolicy;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.pollingingest.DefaultStreamPoller;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static org.opensearch.index.translog.Translog.EMPTY_TRANSLOG_SNAPSHOT;

/**
 * IngestionEngine is an engine that ingests data from a stream source.
 */
public class IngestionEngine extends Engine {

    private volatile SegmentInfos lastCommittedSegmentInfos;
    private final CompletionStatsCache completionStatsCache;
    private final IndexWriter indexWriter;
    private final OpenSearchReaderManager internalReaderManager;
    private final ExternalReaderManager externalReaderManager;
    private final Lock flushLock = new ReentrantLock();
    private final ReentrantLock optimizeLock = new ReentrantLock();
    private final OpenSearchConcurrentMergeScheduler mergeScheduler;
    private final AtomicBoolean shouldPeriodicallyFlushAfterBigMerge = new AtomicBoolean(false);
    private final TranslogManager translogManager;
    private final DocumentMapperForType documentMapperForType;
    private final IngestionConsumerFactory ingestionConsumerFactory;
    private StreamPoller streamPoller;

    /**
     * UUID value that is updated every time the engine is force merged.
     */
    @Nullable
    private volatile String forceMergeUUID;

    public IngestionEngine(EngineConfig engineConfig, IngestionConsumerFactory ingestionConsumerFactory) {
        super(engineConfig);
        store.incRef();
        boolean success = false;
        try {
            this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            this.completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
            IndexMetadata indexMetadata = engineConfig.getIndexSettings().getIndexMetadata();
            assert indexMetadata != null;
            mergeScheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            indexWriter = createWriter();
            externalReaderManager = createReaderManager(new InternalEngine.RefreshWarmerListener(logger, isClosed, engineConfig));
            internalReaderManager = externalReaderManager.internalReaderManager;
            translogManager = new NoOpTranslogManager(
                shardId,
                readLock,
                this::ensureOpen,
                new TranslogStats(0, 0, 0, 0, 0),
                EMPTY_TRANSLOG_SNAPSHOT
            );
            documentMapperForType = engineConfig.getDocumentMapperForTypeSupplier().get();
            this.ingestionConsumerFactory = Objects.requireNonNull(ingestionConsumerFactory);

            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (!success) {
                if (streamPoller != null) {
                    try {
                        streamPoller.close();
                    } catch (IOException e) {
                        logger.error("failed to close stream poller", e);
                        throw new RuntimeException(e);
                    }
                }
                if (!isClosed.get()) {
                    // failure, we need to dec the store reference
                    store.decRef();
                }
            }
        }
    }

    /**
     * Starts the ingestion engine to pull.
     */
    public void start() {
        IndexMetadata indexMetadata = engineConfig.getIndexSettings().getIndexMetadata();
        assert indexMetadata != null;
        IngestionSource ingestionSource = Objects.requireNonNull(indexMetadata.getIngestionSource());

        // initialize the ingestion consumer factory
        this.ingestionConsumerFactory.initialize(ingestionSource.params());
        String clientId = engineConfig.getIndexSettings().getNodeName()
            + "-"
            + engineConfig.getIndexSettings().getIndex().getName()
            + "-"
            + engineConfig.getShardId().getId();
        IngestionShardConsumer ingestionShardConsumer = this.ingestionConsumerFactory.createShardConsumer(
            clientId,
            engineConfig.getShardId().getId()
        );
        logger.info("created ingestion consumer for shard [{}]", engineConfig.getShardId());

        Map<String, String> commitData = commitDataAsMap();
        StreamPoller.ResetState resetState = StreamPoller.ResetState.valueOf(
            ingestionSource.getPointerInitReset().toUpperCase(Locale.ROOT)
        );
        IngestionShardPointer startPointer = null;
        Set<IngestionShardPointer> persistedPointers = new HashSet<>();
        if (commitData.containsKey(StreamPoller.BATCH_START)) {
            // try recovering from commit data
            String batchStartStr = commitData.get(StreamPoller.BATCH_START);
            startPointer = this.ingestionConsumerFactory.parsePointerFromString(batchStartStr);
            try (Searcher searcher = acquireSearcher("restore_offset", SearcherScope.INTERNAL)) {
                persistedPointers = fetchPersistedOffsets(Lucene.wrapAllDocsLive(searcher.getDirectoryReader()), startPointer);
                logger.info("recovered persisted pointers: {}", persistedPointers);
            } catch (IOException e) {
                throw new EngineCreationFailureException(config().getShardId(), "failed to restore offset", e);
            }
            // reset to none so the poller will poll from the startPointer
            resetState = StreamPoller.ResetState.NONE;
        }

        streamPoller = new DefaultStreamPoller(startPointer, persistedPointers, ingestionShardConsumer, this, resetState);
        streamPoller.start();
    }

    private IndexWriter createWriter() throws IOException {
        try {
            final IndexWriterConfig iwc = getIndexWriterConfig();
            return createWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    public DocumentMapperForType getDocumentMapperForType() {
        return documentMapperForType;
    }

    protected Set<IngestionShardPointer> fetchPersistedOffsets(DirectoryReader directoryReader, IngestionShardPointer batchStart)
        throws IOException {
        final IndexSearcher searcher = new IndexSearcher(directoryReader);
        searcher.setQueryCache(null);
        var query = batchStart.newRangeQueryGreaterThan(IngestionShardPointer.OFFSET_FIELD);

        // Execute the search
        var topDocs = searcher.search(query, Integer.MAX_VALUE);
        Set<IngestionShardPointer> result = new HashSet<>();
        var storedFields = searcher.getIndexReader().storedFields();
        for (var scoreDoc : topDocs.scoreDocs) {
            var doc = storedFields.document(scoreDoc.doc);
            String valueStr = doc.get(IngestionShardPointer.OFFSET_FIELD);
            IngestionShardPointer value = ingestionConsumerFactory.parsePointerFromString(valueStr);
            result.add(value);
        }

        refresh("restore_offset", SearcherScope.INTERNAL, true);
        return result;
    }

    /**
     * a copy of ExternalReaderManager from InternalEngine
     */
    @SuppressForbidden(reason = "reference counting is required here")
    static final class ExternalReaderManager extends ReferenceManager<OpenSearchDirectoryReader> {
        private final BiConsumer<OpenSearchDirectoryReader, OpenSearchDirectoryReader> refreshListener;
        private final OpenSearchReaderManager internalReaderManager;
        private boolean isWarmedUp; // guarded by refreshLock

        ExternalReaderManager(
            OpenSearchReaderManager internalReaderManager,
            BiConsumer<OpenSearchDirectoryReader, OpenSearchDirectoryReader> refreshListener
        ) throws IOException {
            this.refreshListener = refreshListener;
            this.internalReaderManager = internalReaderManager;
            this.current = internalReaderManager.acquire(); // steal the reference without warming up
        }

        @Override
        protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
            // we simply run a blocking refresh on the internal reference manager and then steal it's reader
            // it's a save operation since we acquire the reader which incs it's reference but then down the road
            // steal it by calling incRef on the "stolen" reader
            internalReaderManager.maybeRefreshBlocking();
            final OpenSearchDirectoryReader newReader = internalReaderManager.acquire();
            if (isWarmedUp == false || newReader != referenceToRefresh) {
                boolean success = false;
                try {
                    refreshListener.accept(newReader, isWarmedUp ? referenceToRefresh : null);
                    isWarmedUp = true;
                    success = true;
                } finally {
                    if (success == false) {
                        internalReaderManager.release(newReader);
                    }
                }
            }
            // nothing has changed - both ref managers share the same instance so we can use reference equality
            if (referenceToRefresh == newReader) {
                internalReaderManager.release(newReader);
                return null;
            } else {
                return newReader; // steal the reference
            }
        }

        @Override
        protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
            return reference.tryIncRef();
        }

        @Override
        protected int getRefCount(OpenSearchDirectoryReader reference) {
            return reference.getRefCount();
        }

        @Override
        protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
            reference.decRef();
        }
    }

    private ExternalReaderManager createReaderManager(InternalEngine.RefreshWarmerListener externalRefreshListener) throws EngineException {
        boolean success = false;
        OpenSearchReaderManager internalReaderManager = null;
        try {
            try {
                final OpenSearchDirectoryReader directoryReader = OpenSearchDirectoryReader.wrap(
                    DirectoryReader.open(indexWriter),
                    shardId
                );
                internalReaderManager = new OpenSearchReaderManager(directoryReader);
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                ExternalReaderManager externalReaderManager = new ExternalReaderManager(internalReaderManager, externalRefreshListener);
                success = true;
                return externalReaderManager;
            } catch (IOException e) {
                maybeFailEngine("start", e);
                try {
                    indexWriter.rollback();
                } catch (IOException inner) { // iw is closed below
                    e.addSuppressed(inner);
                }
                throw new EngineCreationFailureException(shardId, "failed to open reader on writer", e);
            }
        } finally {
            if (success == false) { // release everything we created on a failure
                IOUtils.closeWhileHandlingException(internalReaderManager, indexWriter);
            }
        }
    }

    // pkg-private for testing
    IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
        return new IndexWriter(directory, iwc);
    }

    private IndexWriterConfig getIndexWriterConfig() {
        final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCommitOnClose(false); // we by default don't commit on close
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        // with tests.verbose, lucene sets this up: plumb to align with filesystem stream
        boolean verbose = false;
        try {
            verbose = Boolean.parseBoolean(System.getProperty("tests.verbose"));
        } catch (Exception ignore) {}
        iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
        iwc.setMergeScheduler(mergeScheduler);
        // set merge scheduler
        MergePolicy mergePolicy = config().getMergePolicy();
        boolean shuffleForcedMerge = Booleans.parseBoolean(System.getProperty("opensearch.shuffle_forced_merge", Boolean.TRUE.toString()));
        if (shuffleForcedMerge) {
            // We wrap the merge policy for all indices even though it is mostly useful for time-based indices
            // but there should be no overhead for other type of indices so it's simpler than adding a setting
            // to enable it.
            mergePolicy = new ShuffleForcedMergePolicy(mergePolicy);
        }

        if (config().getIndexSettings().isMergeOnFlushEnabled()) {
            final long maxFullFlushMergeWaitMillis = config().getIndexSettings().getMaxFullFlushMergeWaitTime().millis();
            if (maxFullFlushMergeWaitMillis > 0) {
                iwc.setMaxFullFlushMergeWaitMillis(maxFullFlushMergeWaitMillis);
                final Optional<UnaryOperator<MergePolicy>> mergeOnFlushPolicy = config().getIndexSettings().getMergeOnFlushPolicy();
                if (mergeOnFlushPolicy.isPresent()) {
                    mergePolicy = mergeOnFlushPolicy.get().apply(mergePolicy);
                }
            }
        } else {
            // Disable merge on refresh
            iwc.setMaxFullFlushMergeWaitMillis(0);
        }

        iwc.setCheckPendingFlushUpdate(config().getIndexSettings().isCheckPendingFlushEnabled());
        iwc.setMergePolicy(new OpenSearchMergePolicy(mergePolicy));
        iwc.setSimilarity(engineConfig.getSimilarity());
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setCodec(engineConfig.getCodec());
        iwc.setUseCompoundFile(engineConfig.useCompoundFile());
        if (config().getIndexSort() != null) {
            iwc.setIndexSort(config().getIndexSort());
        }
        if (config().getLeafSorter() != null) {
            iwc.setLeafSorter(config().getLeafSorter()); // The default segment search order
        }

        return new IndexWriterConfig(new StandardAnalyzer());
    }

    @Override
    public TranslogManager translogManager() {
        // ingestion engine does not have translog
        return translogManager;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    protected SegmentInfos getLatestSegmentInfos() {
        throw new UnsupportedOperationException();
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
        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();
        ensureOpen();
        final IndexResult indexResult;
        indexResult = indexIntoLucene(index);
        return indexResult;
    }

    private IndexResult indexIntoLucene(Index index) throws IOException {
        // todo: handle updates
        addDocs(index.docs(), indexWriter);
        return new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), true);
    }

    private void addDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        return null;
    }

    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        ensureOpen();
        NoOpResult noOpResult = new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
        return noOpResult;
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        return getFromSearcher(get, searcherFactory, SearcherScope.EXTERNAL);
    }

    @Override
    protected ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return externalReaderManager;
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
        return 0;
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return 0;
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return 0;
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return null;
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return 0;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        try (ReleasableLock lock = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos, verbose);

            // fill in the merges flag
            Set<OnGoingMerge> onGoingMerges = mergeScheduler.onGoingMerges();
            for (OnGoingMerge onGoingMerge : onGoingMerges) {
                for (SegmentCommitInfo segmentInfoPerCommit : onGoingMerge.getMergedSegments()) {
                    for (Segment segment : segmentsArr) {
                        if (segment.getName().equals(segmentInfoPerCommit.info.name)) {
                            segment.mergeId = onGoingMerge.getId();
                            break;
                        }
                    }
                }
            }
            return Arrays.asList(segmentsArr);
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        refresh(source, SearcherScope.EXTERNAL, true);
    }

    final boolean refresh(String source, SearcherScope scope, boolean block) throws EngineException {
        boolean refreshed;
        try {
            // refresh does not need to hold readLock as ReferenceManager can handle correctly if the engine is closed in mid-way.
            if (store.tryIncRef()) {
                // increment the ref just to ensure nobody closes the store during a refresh
                try {
                    // even though we maintain 2 managers we really do the heavy-lifting only once.
                    // the second refresh will only do the extra work we have to do for warming caches etc.
                    ReferenceManager<OpenSearchDirectoryReader> referenceManager = getReferenceManager(scope);
                    // it is intentional that we never refresh both internal / external together
                    if (block) {
                        referenceManager.maybeRefreshBlocking();
                        refreshed = true;
                    } else {
                        refreshed = referenceManager.maybeRefresh();
                    }
                } finally {
                    store.decRef();
                }
            } else {
                refreshed = false;
            }
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("refresh failed source[" + source + "]", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }
        // We check for pruning in each delete request, but we also prune here e.g. in case a delete burst comes in and then no more deletes
        // for a long time:
        maybePruneDeletes();
        // TODO: use OS merge scheduler
        mergeScheduler.refreshConfig();
        return refreshed;
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return refresh(source, SearcherScope.EXTERNAL, false);
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        refresh("write indexing buffer", SearcherScope.INTERNAL, false);
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        if (force && waitIfOngoing == false) {
            assert false : "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing;
            throw new IllegalArgumentException(
                "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing
            );
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
                // Only flush if (1) Lucene has uncommitted docs, or (2) forced by caller,
                //
                // do we need to consider #3 and #4 as in InternalEngine?
                // (3) the newly created commit points to a different translog generation (can free translog),
                // or (4) the local checkpoint information in the last commit is stale, which slows down future recoveries.
                boolean hasUncommittedChanges = indexWriter.hasUncommittedChanges();
                if (hasUncommittedChanges || force) {
                    logger.trace("starting commit for flush;");

                    // TODO: do we need to close the latest commit as done in InternalEngine?
                    commitIndexWriter(indexWriter);

                    logger.trace("finished commit for flush");

                    // a temporary debugging to investigate test failure - issue#32827. Remove when the issue is resolved
                    logger.debug("new commit on flush, hasUncommittedChanges:{}, force:{}", hasUncommittedChanges, force);

                    // we need to refresh in order to clear older version values
                    refresh("version_table_flush", SearcherScope.INTERNAL, true);
                }
            } catch (FlushFailedEngineException ex) {
                maybeFailEngine("flush", ex);
                throw ex;
            } catch (IOException e) {
                throw new FlushFailedEngineException(shardId, e);
            } finally {
                flushLock.unlock();
            }
        }
    }

    /**
     * Commits the specified index writer.
     *
     * @param writer   the index writer to commit
     */
    protected void commitIndexWriter(final IndexWriter writer) throws IOException {
        try {
            writer.setLiveCommitData(() -> {
                /*
                 * The user data captured the min and max range of the stream poller
                 */
                final Map<String, String> commitData = new HashMap<>(2);

                commitData.put(StreamPoller.BATCH_START, streamPoller.getBatchStartPointer().asString());
                final String currentForceMergeUUID = forceMergeUUID;
                if (currentForceMergeUUID != null) {
                    commitData.put(FORCE_MERGE_UUID_KEY, currentForceMergeUUID);
                }
                logger.trace("committing writer with commit data [{}]", commitData);
                return commitData.entrySet().iterator();
            });
            writer.commit();
        } catch (final Exception ex) {
            try {
                failEngine("lucene commit failed", ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (final AssertionError e) {
            /*
             * If assertions are enabled, IndexWriter throws AssertionError on commit if any files don't exist, but tests that randomly
             * throw FileNotFoundException or NoSuchFileException can also hit this.
             */
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                final EngineException engineException = new EngineException(shardId, "failed to commit engine", e);
                try {
                    failEngine("lucene commit failed", engineException);
                } catch (final Exception inner) {
                    engineException.addSuppressed(inner);
                }
                throw engineException;
            } else {
                throw e;
            }
        }
    }

    @Override
    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        mergeScheduler.refreshConfig();
        // TODO: do we need more?
    }

    protected Map<String, String> commitDataAsMap() {
        return commitDataAsMap(indexWriter);
    }

    /**
     * Gets the commit data from {@link IndexWriter} as a map.
     */
    protected static Map<String, String> commitDataAsMap(final IndexWriter indexWriter) {
        final Map<String, String> commitData = new HashMap<>(8);
        for (Map.Entry<String, String> entry : indexWriter.getLiveCommitData()) {
            commitData.put(entry.getKey(), entry.getValue());
        }
        return commitData;
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
        /*
         * We do NOT acquire the readlock here since we are waiting on the merges to finish
         * that's fine since the IW.rollback should stop all the threads and trigger an IOException
         * causing us to fail the forceMerge
         *
         * The way we implement upgrades is a bit hackish in the sense that we set an instance
         * variable and that this setting will thus apply to the next forced merge that will be run.
         * This is ok because (1) this is the only place we call forceMerge, (2) we have a single
         * thread for optimize, and the 'optimizeLock' guarding this code, and (3) ConcurrentMergeScheduler
         * syncs calls to findForcedMerges.
         */
        assert indexWriter.getConfig().getMergePolicy() instanceof OpenSearchMergePolicy : "MergePolicy is "
            + indexWriter.getConfig().getMergePolicy().getClass().getName();
        OpenSearchMergePolicy mp = (OpenSearchMergePolicy) indexWriter.getConfig().getMergePolicy();
        optimizeLock.lock();
        try {
            ensureOpen();
            if (upgrade) {
                logger.info("starting segment upgrade upgradeOnlyAncientSegments={}", upgradeOnlyAncientSegments);
                mp.setUpgradeInProgress(true, upgradeOnlyAncientSegments);
            }
            store.incRef(); // increment the ref just to ensure nobody closes the store while we optimize
            try {
                if (onlyExpungeDeletes) {
                    assert upgrade == false;
                    indexWriter.forceMergeDeletes(true /* blocks and waits for merges*/);
                } else if (maxNumSegments <= 0) {
                    assert upgrade == false;
                    indexWriter.maybeMerge();
                } else {
                    indexWriter.forceMerge(maxNumSegments, true /* blocks and waits for merges*/);
                    this.forceMergeUUID = forceMergeUUID;
                }
                if (flush) {
                    flush(false, true);
                }
                if (upgrade) {
                    logger.info("finished segment upgrade");
                }
            } finally {
                store.decRef();
            }
        } catch (AlreadyClosedException ex) {
            /* in this case we first check if the engine is still open. If so this exception is just fine
             * and expected. We don't hold any locks while we block on forceMerge otherwise it would block
             * closing the engine as well. If we are not closed we pass it on to failOnTragicEvent which ensures
             * we are handling a tragic even exception here */
            ensureOpen(ex);
            failOnTragicEvent(ex);
            throw ex;
        } catch (Exception e) {
            try {
                maybeFailEngine(FORCE_MERGE, e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        } finally {
            try {
                // reset it just to make sure we reset it in a case of an error
                mp.setUpgradeInProgress(false, false);
            } finally {
                optimizeLock.unlock();
            }
        }
    }

    @Override
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        store.incRef();
        try {
            var reader = getReferenceManager(SearcherScope.INTERNAL).acquire();
            return new GatedCloseable<>(reader.getIndexCommit(), () -> {
                store.decRef();
                getReferenceManager(SearcherScope.INTERNAL).release(reader);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        // TODO: do we need this? likely not
        return acquireLastIndexCommit(false);
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        // TODO: do we need this?
        return SafeCommitInfo.EMPTY;
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                try {
                    IOUtils.close(externalReaderManager, internalReaderManager);
                } catch (Exception e) {
                    logger.warn("Failed to close ReaderManager", e);
                }

                // no need to commit in this case!, we snapshot before we close the shard, so translog and all sync'ed
                logger.trace("rollback indexWriter");
                try {
                    indexWriter.rollback();
                } catch (AlreadyClosedException ex) {
                    failOnTragicEvent(ex);
                    throw ex;
                }
                logger.trace("rollback indexWriter done");
            } catch (Exception e) {
                logger.warn("failed to rollback writer on close", e);
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

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        // if we are already closed due to some tragic exception
        // we need to fail the engine. it might have already been failed before
        // but we are double-checking it's failed and closed
        if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
            final Exception tragicException;
            if (indexWriter.getTragicException() instanceof Exception) {
                tragicException = (Exception) indexWriter.getTragicException();
            } else {
                tragicException = new RuntimeException(indexWriter.getTragicException());
            }
            failEngine("already closed by tragic event on the index writer", tragicException);
            engineFailed = true;
        } else if (failedEngine.get() == null && isClosed.get() == false) { // we are closed but the engine is not failed yet?
            // this smells like a bug - we only expect ACE if we are in a fatal case ie. either translog or IW is closed by
            // a tragic event or has closed itself. if that is not the case we are in a buggy state and raise an assertion error
            throw new AssertionError("Unexpected AlreadyClosedException", ex);
        } else {
            engineFailed = false;
        }
        return engineFailed;
    }

    private final class EngineMergeScheduler extends OpenSearchConcurrentMergeScheduler {
        private final AtomicInteger numMergesInFlight = new AtomicInteger(0);
        private final AtomicBoolean isThrottling = new AtomicBoolean();

        EngineMergeScheduler(ShardId shardId, IndexSettings indexSettings) {
            super(shardId, indexSettings);
        }

        @Override
        public synchronized void beforeMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.incrementAndGet() > maxNumMerges) {
                if (isThrottling.getAndSet(true) == false) {
                    logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    activateThrottling();
                }
            }
        }

        @Override
        public synchronized void afterMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.decrementAndGet() < maxNumMerges) {
                if (isThrottling.getAndSet(false)) {
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    deactivateThrottling();
                }
            }
            if (indexWriter.hasPendingMerges() == false
                && System.nanoTime() - lastWriteNanos >= engineConfig.getFlushMergesAfter().nanos()) {
                // NEVER do this on a merge thread since we acquire some locks blocking here and if we concurrently rollback the writer
                // we deadlock on engine#close for instance.
                engineConfig.getThreadPool().executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        if (isClosed.get() == false) {
                            logger.warn("failed to flush after merge has finished");
                        }
                    }

                    @Override
                    protected void doRun() {
                        // if we have no pending merges and we are supposed to flush once merges have finished to
                        // free up transient disk usage of the (presumably biggish) segments that were just merged
                        flush();
                    }
                });
            } else if (merge.getTotalBytesSize() >= engineConfig.getIndexSettings().getFlushAfterMergeThresholdSize().getBytes()) {
                // we hit a significant merge which would allow us to free up memory if we'd commit it hence on the next change
                // we should execute a flush on the next operation if that's a flush after inactive or indexing a document.
                // we could fork a thread and do it right away but we try to minimize forking and piggyback on outside events.
                shouldPeriodicallyFlushAfterBigMerge.set(true);
            }
        }

        @Override
        protected void handleMergeException(final Throwable exc) {
            engineConfig.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("merge failure action rejected", e);
                }

                @Override
                protected void doRun() throws Exception {
                    /*
                     * We do this on another thread rather than the merge thread that we are initially called on so that we have complete
                     * confidence that the call stack does not contain catch statements that would cause the error that might be thrown
                     * here from being caught and never reaching the uncaught exception handler.
                     */
                    failEngine(MERGE_FAILED, new MergePolicy.MergeException(exc));
                }
            });
        }
    }

    @Override
    public void activateThrottling() {
        // TODO: add this when we have a thread pool for indexing in parallel
    }

    @Override
    public void deactivateThrottling() {
        // TODO: is this needed?
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        // TODO: is this needed?
        return 0;
    }

    @Override
    public void maybePruneDeletes() {
        // no need to prune deletes in ingestion engine
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        // TODO: is this needed?
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        // TODO: is this needed?
        return 0;
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        // TODO: is this needed?
    }

    @Override
    public void close() throws IOException {
        if (streamPoller != null) {
            streamPoller.close();
        }
        super.close();
    }
}
