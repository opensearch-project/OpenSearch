/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Booleans;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.*;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.OpenSearchMergePolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.indices.ingest.DefaultStreamPoller;
import org.opensearch.indices.ingest.DocumentProcessor;
import org.opensearch.indices.ingest.StreamPoller;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

public class IngestionEngine extends Engine {

    private volatile SegmentInfos lastCommittedSegmentInfos;
    private final CompletionStatsCache completionStatsCache;
    private final IndexWriter indexWriter;
    private final ExternalReaderManager externalReaderManager;
    private final SoftDeletesPolicy softDeletesPolicy;
    protected StreamPoller streamPoller;


    public IngestionEngine(EngineConfig engineConfig) {
        super(engineConfig);

        store.incRef();
        boolean success = false;
        try {
            this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            this.completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
            this.softDeletesPolicy = newSoftDeletesPolicy();
            IndexMetadata indexMetadata = engineConfig.getIndexSettings().getIndexMetadata();
            assert indexMetadata != null;
            IngestionSourceConfig ingestionSourceConfig = indexMetadata.getIngestionSourceConfig();
            assert ingestionSourceConfig != null;
            indexWriter = createWriter();
            externalReaderManager = createReaderManager(new InternalEngine.RefreshWarmerListener(logger, isClosed, engineConfig));

            // todo: get IngestionConsumerFactory from the config
            KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory();
            kafkaConsumerFactory.initialize(ingestionSourceConfig);
            IngestionShardConsumer ingestionShardConsumer = kafkaConsumerFactory.createShardConsumer("clientId", 0);

            // todo: get pointer policy from the config
            KafkaOffset kafkaOffset = new KafkaOffset(0);
            // todo: support other kinds of stream pollers
            streamPoller = new DefaultStreamPoller(kafkaOffset, ingestionShardConsumer,
                new DocumentProcessor(this));
            streamPoller.start();
            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (!success) {
                if (streamPoller != null) {
                    streamPoller.close();
                }
                if (!isClosed.get()) {
                    // failure, we need to dec the store reference
                    store.decRef();
                }
            }
        }
    }

    private SoftDeletesPolicy newSoftDeletesPolicy() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().userData;
        final long lastMinRetainedSeqNo;
        if (commitUserData.containsKey(Engine.MIN_RETAINED_SEQNO)) {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(Engine.MIN_RETAINED_SEQNO));
        } else {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO)) + 1;
        }
        return new SoftDeletesPolicy(
            () -> 0L,
            lastMinRetainedSeqNo,
            engineConfig.getIndexSettings().getSoftDeleteRetentionOperations(),
            engineConfig.retentionLeasesSupplier()
        );
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

    /**
     * a copy of ExternalReaderManager from InternalEngine
     */
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
        // set merge scheduler
        MergePolicy mergePolicy = config().getMergePolicy();
        // always configure soft-deletes field so an engine with soft-deletes disabled can open a Lucene index with soft-deletes.
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        mergePolicy = new RecoverySourcePruneMergePolicy(
            SourceFieldMapper.RECOVERY_SOURCE_NAME,
            softDeletesPolicy::getRetentionQuery,
            new SoftDeletesRetentionMergePolicy(
                Lucene.SOFT_DELETES_FIELD,
                softDeletesPolicy::getRetentionQuery,
                new PrunePostingsMergePolicy(mergePolicy, IdFieldMapper.NAME)
            )
        );
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

    protected IngestionShardPointer getPointer() {
        return streamPoller.getCurrentPointer();
    }

    @Override
    public TranslogManager translogManager() {
        // ingestion engine does not have translog
        return null;
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

    protected long generateSeqNoForOperationOnPrimary(final Operation operation) {
        // todo do we need seq no?
        return 0;
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
    public Translog.Snapshot newChangesSnapshot(String source, long fromSeqNo, long toSeqNo, boolean requiredFullRange, boolean accurateCount) throws IOException {
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
        return List.of();
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
                    // TODO: is this needed?
//                    indexWriter.flush();
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
        // use OS merge scheduler
//        mergeScheduler.refreshConfig();
        return refreshed;
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return false;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {

    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments, String forceMergeUUID) throws EngineException, IOException {

    }

    @Override
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return null;
    }

    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        return null;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return null;
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                try {
                    IOUtils.close(externalReaderManager);
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

    @Override
    public void activateThrottling() {

    }

    @Override
    public void deactivateThrottling() {

    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void maybePruneDeletes() {

    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {

    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return 0;
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {

    }

    @Override
    public void close() throws IOException {
        if(streamPoller!=null) {
            streamPoller.close();
        }
        super.close();
    }
}
