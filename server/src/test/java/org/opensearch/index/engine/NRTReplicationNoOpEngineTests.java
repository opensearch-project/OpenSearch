/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.hamcrest.MatcherAssert;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class NRTReplicationNoOpEngineTests extends EngineTestCase {

    public void testCreateEngine() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (
            final Store nrtEngineStore = createStore();
            final AbstractNRTReplicationEngine nrtEngine = buildNrtNoOpReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            final SegmentInfos latestSegmentInfos = nrtEngine.getLatestSegmentInfos();
            final SegmentInfos lastCommittedSegmentInfos = nrtEngine.getLastCommittedSegmentInfos();
            assertEquals(latestSegmentInfos.version, lastCommittedSegmentInfos.version);
            assertEquals(latestSegmentInfos.getGeneration(), lastCommittedSegmentInfos.getGeneration());
            assertEquals(latestSegmentInfos.getUserData(), lastCommittedSegmentInfos.getUserData());
            assertEquals(latestSegmentInfos.files(true), lastCommittedSegmentInfos.files(true));

            assertTrue(nrtEngine.segments(true).isEmpty());

            try (final GatedCloseable<IndexCommit> indexCommitGatedCloseable = nrtEngine.acquireLastIndexCommit(false)) {
                final IndexCommit indexCommit = indexCommitGatedCloseable.get();
                assertEquals(indexCommit.getUserData(), lastCommittedSegmentInfos.getUserData());
                assertTrue(indexCommit.getFileNames().containsAll(lastCommittedSegmentInfos.files(true)));
            }
        }
    }

    public void testEngineNoOpsToTranslog() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore();
            final AbstractNRTReplicationEngine nrtNoOpEngine = buildNrtNoOpReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            List<Engine.Operation> operations = generateHistoryOnReplica(
                between(1, 500),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            for (Engine.Operation op : operations) {
                applyOperation(nrtNoOpEngine, op);
            }

            Translog.Location zeroLocation = new Translog.Location(0, 0, 0);

            // we don't write to translog, so location it returns should be a default zero location returned by NoOpTranslogManager
            assertEquals(zeroLocation, nrtNoOpEngine.translogManager().getTranslogLastWriteLocation());
            assertEquals(-1, nrtNoOpEngine.getLastSyncedGlobalCheckpoint());

            // since we have not written to xlog and we recover from no op engine's xlog, we wont have any data.
            try (InternalEngine engine = new InternalEngine(nrtNoOpEngine.config())) {
                TranslogHandler translogHandler = createTranslogHandler(nrtNoOpEngine.config().getIndexSettings(), engine);
                engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
                assertEquals(getDocIds(engine, true), Collections.emptyList());
            }
            assertEngineCleanedUp(nrtNoOpEngine);
        }
    }

    public void testUpdateSegments() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore();
            final AbstractNRTReplicationEngine nrtNoOpEngine = buildNrtNoOpReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            // add docs to the primary engine.
            List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 500), randomBoolean(), randomBoolean(), randomBoolean())
                .stream()
                .filter(op -> op.operationType().equals(Engine.Operation.TYPE.INDEX))
                .collect(Collectors.toList());
            for (Engine.Operation op : operations) {
                applyOperation(engine, op);
                applyOperation(nrtNoOpEngine, op);
            }

            engine.refresh("test");

            final SegmentInfos latestPrimaryInfos = engine.getLatestSegmentInfos();
            nrtNoOpEngine.updateSegments(latestPrimaryInfos, engine.getProcessedLocalCheckpoint());
            assertMatchingSegmentsAndCheckpoints(nrtNoOpEngine, latestPrimaryInfos);

            // assert a doc from the operations exists.
            final ParsedDocument parsedDoc = createParsedDoc(operations.stream().findFirst().get().id(), null);
            try (Engine.GetResult getResult = engine.get(newGet(true, parsedDoc), engine::acquireSearcher)) {
                assertThat(getResult.exists(), equalTo(true));
                assertThat(getResult.docIdAndVersion(), notNullValue());
            }

            try (Engine.GetResult getResult = nrtNoOpEngine.get(newGet(true, parsedDoc), nrtNoOpEngine::acquireSearcher)) {
                assertThat(getResult.exists(), equalTo(true));
                assertThat(getResult.docIdAndVersion(), notNullValue());
            }

            // Flush the primary and update the NRTEngine with the latest committed infos.
            engine.flush();

            final SegmentInfos primaryInfos = engine.getLastCommittedSegmentInfos();
            nrtNoOpEngine.updateSegments(primaryInfos, engine.getProcessedLocalCheckpoint());
            assertMatchingSegmentsAndCheckpoints(nrtNoOpEngine, primaryInfos);

            // Ensure the same hit count between engines.
            int expectedDocCount;
            try (final Engine.Searcher test = engine.acquireSearcher("test")) {
                expectedDocCount = test.count(Queries.newMatchAllQuery());
                assertSearcherHits(nrtNoOpEngine, expectedDocCount);
            }
            assertEngineCleanedUp(nrtNoOpEngine);
        }
    }

    private void assertMatchingSegmentsAndCheckpoints(AbstractNRTReplicationEngine nrtEngine, SegmentInfos expectedSegmentInfos)
        throws IOException {
        // For NRTReplNoOpEngine, getPersistedLocalCheckpoint() is returning the max seq number seen so far.
        assertEquals(engine.getLocalCheckpointTracker().getMaxSeqNo(), nrtEngine.getPersistedLocalCheckpoint());
        assertEquals(engine.getProcessedLocalCheckpoint(), nrtEngine.getProcessedLocalCheckpoint());
        assertEquals(engine.getLocalCheckpointTracker().getMaxSeqNo(), nrtEngine.getLocalCheckpointTracker().getMaxSeqNo());
        assertEquals(expectedSegmentInfos.files(true), nrtEngine.getLatestSegmentInfos().files(true));
        assertEquals(expectedSegmentInfos.getUserData(), nrtEngine.getLatestSegmentInfos().getUserData());
        assertEquals(expectedSegmentInfos.getVersion(), nrtEngine.getLatestSegmentInfos().getVersion());
    }

    private void assertSearcherHits(Engine engine, int hits) {
        try (final Engine.Searcher test = engine.acquireSearcher("test")) {
            MatcherAssert.assertThat(test, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(hits));
        }
    }

    private AbstractNRTReplicationEngine buildNrtNoOpReplicaEngine(AtomicLong globalCheckpoint, Store store) throws IOException {
        Lucene.cleanLuceneIndex(store.directory());
        final Path translogDir = createTempDir();
        final EngineConfig replicaConfig = config(
            defaultSettings,
            store,
            translogDir,
            NoMergePolicy.INSTANCE,
            null,
            null,
            globalCheckpoint::get
        );
        if (Lucene.indexExists(store.directory()) == false) {
            store.createEmpty(replicaConfig.getIndexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUuid = Translog.createEmptyTranslog(
                replicaConfig.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUuid);
        }
        return new NRTReplicationNoOpEngine(replicaConfig);
    }
}
