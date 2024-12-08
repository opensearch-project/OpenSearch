/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.index.*;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.seqno.SeqNoStats;
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
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

public class IngestionEngine extends Engine {

    private volatile SegmentInfos lastCommittedSegmentInfos;
    private final CompletionStatsCache completionStatsCache;
    private final IndexWriter indexWriter;


    protected StreamPoller streamPoller;

    public IngestionEngine(EngineConfig engineConfig) {
        super(engineConfig);

        store.incRef();
        boolean success = false;
        try {
            this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            this.completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));

            IndexMetadata indexMetadata = engineConfig.getIndexSettings().getIndexMetadata();
            assert indexMetadata != null;
            IngestionSourceConfig ingestionSourceConfig = indexMetadata.getIngestionSourceConfig();
            assert ingestionSourceConfig != null;
            indexWriter = createWriter();

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

    private IndexWriter createWriter() throws IOException {
        try {
            final IndexWriterConfig iwc = getIndexWriterConfig();
            return createWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    // pkg-private for testing
    IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
        return new IndexWriter(directory, iwc);
    }

    private IndexWriterConfig getIndexWriterConfig() {
        // TODO: get the config from the index settings
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
        return null;
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
