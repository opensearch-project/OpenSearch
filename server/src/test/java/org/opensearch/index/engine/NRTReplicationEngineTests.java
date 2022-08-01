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
import org.opensearch.index.translog.TestTranslog;
import org.opensearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

public class NRTReplicationEngineTests extends EngineTestCase {

    public void testCreateEngine() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (
            final Store nrtEngineStore = createStore();
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore);
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

    public void testEngineWritesOpsToTranslog() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore();
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore);
        ) {
            List<Engine.Operation> operations = generateHistoryOnReplica(
                between(1, 500),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            for (Engine.Operation op : operations) {
                applyOperation(engine, op);
                applyOperation(nrtEngine, op);
            }

            assertEquals(nrtEngine.getTranslogLastWriteLocation(), engine.getTranslogLastWriteLocation());
            assertEquals(nrtEngine.getLastSyncedGlobalCheckpoint(), engine.getLastSyncedGlobalCheckpoint());

            // we don't index into nrtEngine, so get the doc ids from the regular engine.
            final List<DocIdSeqNoAndSource> docs = getDocIds(engine, true);

            // recover a new engine from the nrtEngine's xlog.
            nrtEngine.syncTranslog();
            try (InternalEngine engine = new InternalEngine(nrtEngine.config())) {
                TranslogHandler translogHandler = createTranslogHandler(nrtEngine.config().getIndexSettings(), engine);
                engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
                assertEquals(getDocIds(engine, true), docs);
            }
            assertEngineCleanedUp(nrtEngine, nrtEngine.getTranslog());
        }
    }

    public void testUpdateSegments() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore();
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore);
        ) {
            // add docs to the primary engine.
            List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 500), randomBoolean(), randomBoolean(), randomBoolean())
                .stream()
                .filter(op -> op.operationType().equals(Engine.Operation.TYPE.INDEX))
                .collect(Collectors.toList());
            for (Engine.Operation op : operations) {
                applyOperation(engine, op);
                applyOperation(nrtEngine, op);
            }

            engine.refresh("test");

            nrtEngine.updateSegments(engine.getLatestSegmentInfos(), engine.getProcessedLocalCheckpoint());
            assertMatchingSegmentsAndCheckpoints(nrtEngine);

            // assert a doc from the operations exists.
            final ParsedDocument parsedDoc = createParsedDoc(operations.stream().findFirst().get().id(), null);
            try (Engine.GetResult getResult = engine.get(newGet(true, parsedDoc), engine::acquireSearcher)) {
                assertThat(getResult.exists(), equalTo(true));
                assertThat(getResult.docIdAndVersion(), notNullValue());
            }

            try (Engine.GetResult getResult = nrtEngine.get(newGet(true, parsedDoc), nrtEngine::acquireSearcher)) {
                assertThat(getResult.exists(), equalTo(true));
                assertThat(getResult.docIdAndVersion(), notNullValue());
            }

            // Flush the primary and update the NRTEngine with the latest committed infos.
            engine.flush();
            nrtEngine.syncTranslog(); // to advance persisted checkpoint

            Set<Long> seqNos = operations.stream().map(Engine.Operation::seqNo).collect(Collectors.toSet());

            try (Translog.Snapshot snapshot = nrtEngine.getTranslog().newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(operations.size()));
                assertThat(
                    TestTranslog.drainSnapshot(snapshot, false).stream().map(Translog.Operation::seqNo).collect(Collectors.toSet()),
                    equalTo(seqNos)
                );
            }

            nrtEngine.updateSegments(engine.getLastCommittedSegmentInfos(), engine.getProcessedLocalCheckpoint());
            assertMatchingSegmentsAndCheckpoints(nrtEngine);

            assertEquals(
                nrtEngine.getTranslog().getGeneration().translogFileGeneration,
                engine.translogManager().getTranslog().getGeneration().translogFileGeneration
            );

            try (Translog.Snapshot snapshot = nrtEngine.getTranslog().newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(operations.size()));
                assertThat(
                    TestTranslog.drainSnapshot(snapshot, false).stream().map(Translog.Operation::seqNo).collect(Collectors.toSet()),
                    equalTo(seqNos)
                );
            }

            // Ensure the same hit count between engines.
            int expectedDocCount;
            try (final Engine.Searcher test = engine.acquireSearcher("test")) {
                expectedDocCount = test.count(Queries.newMatchAllQuery());
                assertSearcherHits(nrtEngine, expectedDocCount);
            }
            assertEngineCleanedUp(nrtEngine, nrtEngine.getTranslog());
        }
    }

    public void testTrimTranslogOps() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore();
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore);
        ) {
            List<Engine.Operation> operations = generateHistoryOnReplica(
                between(1, 100),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            applyOperations(nrtEngine, operations);
            Set<Long> seqNos = operations.stream().map(Engine.Operation::seqNo).collect(Collectors.toSet());
            try (Translog.Snapshot snapshot = nrtEngine.getTranslog().newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(operations.size()));
                assertThat(
                    TestTranslog.drainSnapshot(snapshot, false).stream().map(Translog.Operation::seqNo).collect(Collectors.toSet()),
                    equalTo(seqNos)
                );
            }
            nrtEngine.rollTranslogGeneration();
            nrtEngine.trimOperationsFromTranslog(primaryTerm.get(), NO_OPS_PERFORMED);
            try (Translog.Snapshot snapshot = getTranslog(engine).newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(0));
                assertNull(snapshot.next());
            }
        }
    }

    private void assertMatchingSegmentsAndCheckpoints(NRTReplicationEngine nrtEngine) throws IOException {
        assertEquals(engine.getPersistedLocalCheckpoint(), nrtEngine.getPersistedLocalCheckpoint());
        assertEquals(engine.getProcessedLocalCheckpoint(), nrtEngine.getProcessedLocalCheckpoint());
        assertEquals(engine.getLocalCheckpointTracker().getMaxSeqNo(), nrtEngine.getLocalCheckpointTracker().getMaxSeqNo());
        assertEquals(engine.getLatestSegmentInfos().files(true), nrtEngine.getLatestSegmentInfos().files(true));
        assertEquals(engine.getLatestSegmentInfos().getUserData(), nrtEngine.getLatestSegmentInfos().getUserData());
        assertEquals(engine.getLatestSegmentInfos().getVersion(), nrtEngine.getLatestSegmentInfos().getVersion());
        assertEquals(engine.segments(true), nrtEngine.segments(true));
    }

    private void assertSearcherHits(Engine engine, int hits) {
        try (final Engine.Searcher test = engine.acquireSearcher("test")) {
            MatcherAssert.assertThat(test, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(hits));
        }
    }

    private NRTReplicationEngine buildNrtReplicaEngine(AtomicLong globalCheckpoint, Store store) throws IOException {
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
        return new NRTReplicationEngine(replicaConfig);
    }
}
