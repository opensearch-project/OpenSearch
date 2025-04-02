/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.index.translog.listener.CompositeTranslogEventListener;
import org.opensearch.indices.pollingingest.DefaultStreamPoller;
import org.opensearch.indices.pollingingest.IngestionErrorStrategy;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.indices.pollingingest.StreamPoller;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import static org.opensearch.index.translog.Translog.EMPTY_TRANSLOG_SNAPSHOT;

/**
 * IngestionEngine is an engine that ingests data from a stream source.
 */
public class IngestionEngine extends InternalEngine {

    private StreamPoller streamPoller;
    private final IngestionConsumerFactory ingestionConsumerFactory;
    private final DocumentMapperForType documentMapperForType;

    public IngestionEngine(EngineConfig engineConfig, IngestionConsumerFactory ingestionConsumerFactory) {
        super(engineConfig);
        this.ingestionConsumerFactory = Objects.requireNonNull(ingestionConsumerFactory);
        this.documentMapperForType = engineConfig.getDocumentMapperForTypeSupplier().get();
        registerDynamicIndexSettingsHandlers();
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
        Map<String, String> commitData = commitDataAsMap(indexWriter);
        StreamPoller.ResetState resetState = ingestionSource.getPointerInitReset().getType();
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

        String resetValue = ingestionSource.getPointerInitReset().getValue();
        IngestionErrorStrategy ingestionErrorStrategy = IngestionErrorStrategy.create(
            ingestionSource.getErrorStrategy(),
            ingestionSource.getType()
        );

        StreamPoller.State initialPollerState = indexMetadata.getIngestionStatus().isPaused()
            ? StreamPoller.State.PAUSED
            : StreamPoller.State.NONE;
        streamPoller = new DefaultStreamPoller(
            startPointer,
            persistedPointers,
            ingestionShardConsumer,
            this,
            resetState,
            resetValue,
            ingestionErrorStrategy,
            initialPollerState
        );
        streamPoller.start();
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
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return EMPTY_TRANSLOG_SNAPSHOT;
    }

    /**
     * This method is a copy of commitIndexWriter method from {@link InternalEngine} with some additions for ingestion
     * source.
     */
    @Override
    protected void commitIndexWriter(final IndexWriter writer, final String translogUUID) throws IOException {
        try {
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            writer.setLiveCommitData(() -> {
                /*
                 * The user data captured above (e.g. local checkpoint) contains data that must be evaluated *before* Lucene flushes
                 * segments, including the local checkpoint amongst other values. The maximum sequence number is different, we never want
                 * the maximum sequence number to be less than the last sequence number to go into a Lucene commit, otherwise we run the
                 * risk of re-using a sequence number for two different documents when restoring from this commit point and subsequently
                 * writing new documents to the index. Since we only know which Lucene documents made it into the final commit after the
                 * {@link IndexWriter#commit()} call flushes all documents, we defer computation of the maximum sequence number to the time
                 * of invocation of the commit data iterator (which occurs after all documents have been flushed to Lucene).
                 */
                final Map<String, String> commitData = new HashMap<>(7);
                commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
                commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
                commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
                commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
                commitData.put(HISTORY_UUID_KEY, historyUUID);
                commitData.put(Engine.MIN_RETAINED_SEQNO, Long.toString(softDeletesPolicy.getMinRetainedSeqNo()));

                /*
                 * Ingestion engine needs to record batch start pointer.
                 * Batch start pointer can be null at index creation time, if flush is called before the stream
                 * poller has been completely initialized.
                 */
                if (streamPoller.getBatchStartPointer() != null) {
                    commitData.put(StreamPoller.BATCH_START, streamPoller.getBatchStartPointer().asString());
                } else {
                    logger.warn("ignore null batch start pointer");
                }
                final String currentForceMergeUUID = forceMergeUUID;
                if (currentForceMergeUUID != null) {
                    commitData.put(FORCE_MERGE_UUID_KEY, currentForceMergeUUID);
                }
                logger.trace("committing writer with commit data [{}]", commitData);
                return commitData.entrySet().iterator();
            });
            shouldPeriodicallyFlushAfterBigMerge.set(false);
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
    public void activateThrottling() {
        // TODO: add this when we have a thread pool for indexing in parallel
    }

    @Override
    public void deactivateThrottling() {
        // TODO: is this needed?
    }

    @Override
    public void maybePruneDeletes() {
        // no need to prune deletes in ingestion engine
    }

    @Override
    public void close() throws IOException {
        if (streamPoller != null) {
            streamPoller.close();
        }
        super.close();
    }

    public DocumentMapperForType getDocumentMapperForType() {
        return documentMapperForType;
    }

    @Override
    protected TranslogManager createTranslogManager(
        String translogUUID,
        TranslogDeletionPolicy translogDeletionPolicy,
        CompositeTranslogEventListener translogEventListener
    ) throws IOException {
        return new NoOpTranslogManager(
            shardId,
            readLock,
            this::ensureOpen,
            new TranslogStats(),
            EMPTY_TRANSLOG_SNAPSHOT,
            translogUUID,
            true
        );
    }

    protected Map<String, String> commitDataAsMap() {
        return commitDataAsMap(indexWriter);
    }

    @Override
    public PollingIngestStats pollingIngestStats() {
        return streamPoller.getStats();
    }

    private void registerDynamicIndexSettingsHandlers() {
        engineConfig.getIndexSettings()
            .getScopedSettings()
            .addSettingsUpdateConsumer(IndexMetadata.INGESTION_SOURCE_ERROR_STRATEGY_SETTING, this::updateErrorHandlingStrategy);
    }

    /**
     * Handler for updating ingestion error strategy in the stream poller on dynamic index settings update.
     */
    private void updateErrorHandlingStrategy(IngestionErrorStrategy.ErrorStrategy errorStrategy) {
        IngestionErrorStrategy updatedIngestionErrorStrategy = IngestionErrorStrategy.create(
            errorStrategy,
            engineConfig.getIndexSettings().getIndexMetadata().getIngestionSource().getType()
        );
        streamPoller.updateErrorStrategy(updatedIngestionErrorStrategy);
    }

    /**
     * Pause the poller. Used by management flows.
     */
    public void pauseIngestion() {
        streamPoller.pause();
    }

    /**
     * Resumes the poller. Used by management flows.
     */
    public void resumeIngestion() {
        streamPoller.resume();
    }

    /**
     * Get current ingestion state. Used by management flows.
     */
    public ShardIngestionState getIngestionState() {
        return new ShardIngestionState(
            engineConfig.getIndexSettings().getIndex().getName(),
            engineConfig.getShardId().getId(),
            streamPoller.getState().toString(),
            streamPoller.getErrorStrategy().getName(),
            streamPoller.isPaused()
        );
    }
}
