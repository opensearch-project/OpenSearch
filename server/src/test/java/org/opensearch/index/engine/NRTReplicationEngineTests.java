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
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TestTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;
import static org.opensearch.index.seqno.SequenceNumbers.MAX_SEQ_NO;
import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.hamcrest.Matchers.equalTo;

public class NRTReplicationEngineTests extends EngineTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build()
    );

    public void testCreateEngine() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore)
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
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore)
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

            assertEquals(
                nrtEngine.translogManager().getTranslogLastWriteLocation(),
                engine.translogManager().getTranslogLastWriteLocation()
            );
            assertEquals(nrtEngine.getLastSyncedGlobalCheckpoint(), engine.getLastSyncedGlobalCheckpoint());

            // we don't index into nrtEngine, so get the doc ids from the regular engine.
            final List<DocIdSeqNoAndSource> docs = getDocIds(engine, true);

            // close the NRTEngine, it will commit on close and we'll reuse its store for an IE.
            nrtEngine.close();

            // recover a new engine from the nrtEngine's xlog.
            nrtEngine.translogManager().syncTranslog();
            try (InternalEngine engine = new InternalEngine(nrtEngine.config())) {
                TranslogHandler translogHandler = createTranslogHandler(nrtEngine.config().getIndexSettings(), engine);
                engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
                assertEquals(getDocIds(engine, true), docs);
            }
            assertEngineCleanedUp(nrtEngine, assertAndGetInternalTranslogManager(nrtEngine.translogManager()).getDeletionPolicy());
        }
    }

    public void testUpdateSegments_replicaReceivesSISWithHigherGen() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            // assume we start at the same gen.
            assertEquals(2, nrtEngine.getLatestSegmentInfos().getGeneration());
            assertEquals(nrtEngine.getLatestSegmentInfos().getGeneration(), nrtEngine.getLastCommittedSegmentInfos().getGeneration());
            assertEquals(engine.getLatestSegmentInfos().getGeneration(), nrtEngine.getLatestSegmentInfos().getGeneration());

            // flush the primary engine - we don't need any segments, just force a new commit point.
            engine.flush(true, true);
            assertEquals(3, engine.getLatestSegmentInfos().getGeneration());
            nrtEngine.updateSegments(engine.getLatestSegmentInfos());
            assertEquals(3, nrtEngine.getLastCommittedSegmentInfos().getGeneration());
            assertEquals(3, nrtEngine.getLatestSegmentInfos().getGeneration());
        }
    }

    public void testUpdateSegments_replicaReceivesSISWithLowerGen() throws IOException {
        // if the replica is already at segments_N that is received, it will commit segments_N+1.
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            assertEquals(5, nrtEngine.getLatestSegmentInfos().getVersion());
            nrtEngine.getLatestSegmentInfos().changed();
            nrtEngine.getLatestSegmentInfos().changed();
            assertEquals(7, nrtEngine.getLatestSegmentInfos().getVersion());
            assertEquals(2, nrtEngine.getLastCommittedSegmentInfos().getGeneration());

            // commit the infos to push us to segments_3.
            nrtEngine.flush();
            assertEquals(3, nrtEngine.getLastCommittedSegmentInfos().getGeneration());
            assertEquals(3, nrtEngine.getLatestSegmentInfos().getGeneration());

            // update the replica with segments_2 from the primary.
            final SegmentInfos primaryInfos = engine.getLatestSegmentInfos();
            assertEquals(2, primaryInfos.getGeneration());
            assertEquals(5, primaryInfos.getVersion());
            nrtEngine.updateSegments(primaryInfos);
            assertEquals(4, nrtEngine.getLastCommittedSegmentInfos().getGeneration());
            assertEquals(4, nrtEngine.getLatestSegmentInfos().getGeneration());
            assertEquals(primaryInfos.getVersion(), nrtEngine.getLatestSegmentInfos().getVersion());
            assertEquals(primaryInfos.getVersion(), nrtEngine.getLastCommittedSegmentInfos().getVersion());

            nrtEngine.close();
            assertEquals(5, nrtEngine.getLastCommittedSegmentInfos().getGeneration());
        }
    }

    public void testSimultaneousEngineCloseAndCommit() throws IOException, InterruptedException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            CountDownLatch latch = new CountDownLatch(1);
            Thread commitThread = new Thread(() -> {
                try {
                    nrtEngine.updateSegments(store.readLastCommittedSegmentsInfo());
                    latch.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            Thread closeThread = new Thread(() -> {
                try {
                    latch.await();
                    nrtEngine.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            commitThread.start();
            closeThread.start();
            commitThread.join();
            closeThread.join();
        }
    }

    public void testUpdateSegments_replicaCommitsFirstReceivedInfos() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            assertEquals(2, nrtEngine.getLastCommittedSegmentInfos().getGeneration());
            assertEquals(2, nrtEngine.getLatestSegmentInfos().getGeneration());
            // bump the latest infos version a couple of times so that we can assert the correct version after commit.
            engine.getLatestSegmentInfos().changed();
            engine.getLatestSegmentInfos().changed();
            assertNotEquals(nrtEngine.getLatestSegmentInfos().getVersion(), engine.getLatestSegmentInfos().getVersion());

            // update replica with the latest primary infos, it will be the same gen, segments_2, ensure it is also committed.
            final SegmentInfos primaryInfos = engine.getLatestSegmentInfos();
            assertEquals(2, primaryInfos.getGeneration());
            nrtEngine.updateSegments(primaryInfos);
            final SegmentInfos lastCommittedSegmentInfos = nrtEngine.getLastCommittedSegmentInfos();
            assertEquals(primaryInfos.getVersion(), nrtEngine.getLatestSegmentInfos().getVersion());
            assertEquals(primaryInfos.getVersion(), lastCommittedSegmentInfos.getVersion());
        }
    }

    public void testRefreshOnNRTEngine() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            assertEquals(2, nrtEngine.getLastCommittedSegmentInfos().getGeneration());
            assertEquals(2, nrtEngine.getLatestSegmentInfos().getGeneration());

            ReferenceManager<OpenSearchDirectoryReader> referenceManager = nrtEngine.getReferenceManager(Engine.SearcherScope.EXTERNAL);
            OpenSearchDirectoryReader readerBeforeRefresh = referenceManager.acquire();

            nrtEngine.refresh("test refresh");
            OpenSearchDirectoryReader readerAfterRefresh = referenceManager.acquire();

            // Verify both readers before and after refresh are same and no change in segments
            assertSame(readerBeforeRefresh, readerAfterRefresh);

        }
    }

    public void testTrimTranslogOps() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
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
            nrtEngine.ensureOpen();
            try (
                Translog.Snapshot snapshot = assertAndGetInternalTranslogManager(nrtEngine.translogManager()).getTranslog().newSnapshot()
            ) {
                assertThat(snapshot.totalOperations(), equalTo(operations.size()));
                assertThat(
                    TestTranslog.drainSnapshot(snapshot, false).stream().map(Translog.Operation::seqNo).collect(Collectors.toSet()),
                    equalTo(seqNos)
                );
            }
            nrtEngine.translogManager().rollTranslogGeneration();
            nrtEngine.translogManager().trimOperationsFromTranslog(primaryTerm.get(), NO_OPS_PERFORMED);
            try (Translog.Snapshot snapshot = getTranslog(engine).newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(0));
                assertNull(snapshot.next());
            }
        }
    }

    public void testFlush() throws Exception {
        // This test asserts that NRTReplication#commitSegmentInfos creates a new commit point with the latest checkpoints
        // stored in user data.
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore)
        ) {
            List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 500), randomBoolean(), randomBoolean(), randomBoolean())
                .stream()
                .filter(op -> op.operationType().equals(Engine.Operation.TYPE.INDEX))
                .collect(Collectors.toList());
            for (Engine.Operation op : operations) {
                applyOperation(nrtEngine, op);
            }

            final SegmentInfos previousInfos = nrtEngine.getLatestSegmentInfos();
            LocalCheckpointTracker localCheckpointTracker = nrtEngine.getLocalCheckpointTracker();
            final long maxSeqNo = localCheckpointTracker.getMaxSeqNo();
            final long processedCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            nrtEngine.flush();

            // ensure getLatestSegmentInfos returns an updated infos ref with correct userdata.
            final SegmentInfos latestSegmentInfos = nrtEngine.getLatestSegmentInfos();
            assertEquals(previousInfos.getGeneration(), latestSegmentInfos.getLastGeneration());
            assertEquals(previousInfos.getVersion(), latestSegmentInfos.getVersion());
            assertEquals(previousInfos.counter, latestSegmentInfos.counter);
            Map<String, String> userData = latestSegmentInfos.getUserData();
            assertEquals(processedCheckpoint, localCheckpointTracker.getProcessedCheckpoint());
            assertEquals(maxSeqNo, Long.parseLong(userData.get(MAX_SEQ_NO)));
            assertEquals(processedCheckpoint, Long.parseLong(userData.get(LOCAL_CHECKPOINT_KEY)));

            // read infos from store and assert userdata
            final String lastCommitSegmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(nrtEngineStore.directory());
            final SegmentInfos committedInfos = SegmentInfos.readCommit(nrtEngineStore.directory(), lastCommitSegmentsFileName);
            userData = committedInfos.getUserData();
            assertEquals(processedCheckpoint, Long.parseLong(userData.get(LOCAL_CHECKPOINT_KEY)));
            assertEquals(maxSeqNo, Long.parseLong(userData.get(MAX_SEQ_NO)));

            try (final GatedCloseable<IndexCommit> indexCommit = nrtEngine.acquireLastIndexCommit(true)) {
                assertEquals(committedInfos.getGeneration() + 1, indexCommit.get().getGeneration());
            }
        }
    }

    private NRTReplicationEngine buildNrtReplicaEngine(AtomicLong globalCheckpoint, Store store, IndexSettings settings)
        throws IOException {
        Lucene.cleanLuceneIndex(store.directory());
        final Path translogDir = createTempDir();
        final EngineConfig replicaConfig = config(settings, store, translogDir, NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
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

    private NRTReplicationEngine buildNrtReplicaEngine(AtomicLong globalCheckpoint, Store store) throws IOException {
        return buildNrtReplicaEngine(globalCheckpoint, store, defaultSettings);
    }

    public void testGetSegmentInfosSnapshotPreservesFilesUntilRelease() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore, INDEX_SETTINGS)
        ) {
            // only index 2 docs here, this will create segments _0 and _1 and after forcemerge into _2.
            final int docCount = 2;
            List<Engine.Operation> operations = generateHistoryOnReplica(docCount, randomBoolean(), randomBoolean(), randomBoolean());
            for (Engine.Operation op : operations) {
                applyOperation(engine, op);
                applyOperation(nrtEngine, op);
                // refresh to create a lot of segments.
                engine.refresh("test");
            }
            assertEquals(2, engine.segmentsStats(false, false).getCount());
            // wipe the nrt directory initially so we can sync with primary.
            Lucene.cleanLuceneIndex(nrtEngineStore.directory());
            assertFalse(
                Arrays.stream(nrtEngineStore.directory().listAll())
                    .anyMatch(file -> file.equals("write.lock") == false && file.equals("extra0") == false)
            );
            for (String file : engine.getLatestSegmentInfos().files(true)) {
                nrtEngineStore.directory().copyFrom(store.directory(), file, file, IOContext.DEFAULT);
            }
            nrtEngine.updateSegments(engine.getLatestSegmentInfos());
            assertEquals(engine.getLatestSegmentInfos(), nrtEngine.getLatestSegmentInfos());
            final GatedCloseable<SegmentInfos> snapshot = nrtEngine.getSegmentInfosSnapshot();
            final Collection<String> replicaSnapshotFiles = snapshot.get().files(false);
            List<String> replicaFiles = List.of(nrtEngine.store.directory().listAll());

            // merge primary down to 1 segment
            engine.forceMerge(true, 1, false, false, false, UUIDs.randomBase64UUID());
            // we expect a 3rd segment to be created after merge.
            assertEquals(3, engine.segmentsStats(false, false).getCount());
            final Collection<String> latestPrimaryFiles = engine.getLatestSegmentInfos().files(false);

            // copy new segments in and load reader.
            for (String file : latestPrimaryFiles) {
                if (replicaFiles.contains(file) == false) {
                    nrtEngineStore.directory().copyFrom(store.directory(), file, file, IOContext.DEFAULT);
                }
            }
            nrtEngine.updateSegments(engine.getLatestSegmentInfos());

            replicaFiles = List.of(nrtEngine.store.directory().listAll());
            assertTrue(replicaFiles.containsAll(replicaSnapshotFiles));

            // close snapshot, files should be cleaned up
            snapshot.close();

            replicaFiles = List.of(nrtEngine.store.directory().listAll());
            assertFalse(replicaFiles.containsAll(replicaSnapshotFiles));

            // Ensure we still have all the active files. Note - we exclude the infos file here if we aren't committing
            // the nrt reader will still reference segments_n-1 after being loaded until a local commit occurs.
            assertTrue(replicaFiles.containsAll(nrtEngine.getLatestSegmentInfos().files(false)));
        }
    }

    public void testRemoveExtraSegmentsOnStartup() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        List<Engine.Operation> operations = generateHistoryOnReplica(2, randomBoolean(), randomBoolean(), randomBoolean());
        for (Engine.Operation op : operations) {
            applyOperation(engine, op);
            // refresh to create a lot of segments.
            engine.refresh("test");
        }
        try (final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());) {
            nrtEngineStore.createEmpty(Version.LATEST);
            final Collection<String> extraSegments = engine.getLatestSegmentInfos().files(false);
            for (String file : extraSegments) {
                nrtEngineStore.directory().copyFrom(store.directory(), file, file, IOContext.DEFAULT);
            }
            List<String> replicaFiles = List.of(nrtEngineStore.directory().listAll());
            for (String file : extraSegments) {
                assertTrue(replicaFiles.contains(file));
            }
            try (NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore, INDEX_SETTINGS)) {
                assertUnreferenced(nrtEngine, extraSegments);
            }
        }
    }

    public void testPreserveLatestCommit() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore, INDEX_SETTINGS)
        ) {
            final int docCount = 4;
            List<Engine.Operation> operations = generateHistoryOnReplica(docCount, randomBoolean(), randomBoolean(), randomBoolean());
            indexOperations(nrtEngine, operations.subList(0, 2));
            // wipe the nrt directory initially so we can sync with primary.
            cleanAndCopySegmentsFromPrimary(nrtEngine);
            SegmentInfos primaryInfos;

            final SegmentInfos lastCommittedSegmentInfos = nrtEngine.getLastCommittedSegmentInfos();
            final Collection<String> lastCommittedFiles = lastCommittedSegmentInfos.files(true);
            assertRefCounted(nrtEngine, lastCommittedFiles);

            // get and close a snapshot - this will decref files when closed.
            final GatedCloseable<SegmentInfos> segmentInfosSnapshot = nrtEngine.getSegmentInfosSnapshot();
            segmentInfosSnapshot.close();
            assertRefCounted(nrtEngine, lastCommittedFiles);

            // index more docs and refresh the reader - this will incref/decref files again
            indexOperations(nrtEngine, operations.subList(2, 4));
            primaryInfos = engine.getLatestSegmentInfos();
            copySegments(primaryInfos.files(false), nrtEngine);
            nrtEngine.updateSegments(primaryInfos);

            // get the additional segments that are only on the reader - not part of a commit.
            final Collection<String> readerOnlySegments = primaryInfos.files(false);
            readerOnlySegments.removeAll(lastCommittedFiles);
            assertRefCounted(nrtEngine, readerOnlySegments);
            // re-read the last commit from disk here in case the primary engine has flushed.
            assertRefCounted(nrtEngine, nrtEngine.getLastCommittedSegmentInfos().files(true));

            // flush the primary
            engine.flush(true, true);
            final Collection<String> latestPrimaryInfos = engine.getLatestSegmentInfos().files(false);
            final Collection<String> mergedAwayFiles = nrtEngine.getLastCommittedSegmentInfos().files(false);
            // remove files still part of latest commit.
            mergedAwayFiles.removeAll(latestPrimaryInfos);
            copySegments(latestPrimaryInfos, nrtEngine);
            nrtEngine.updateSegments(engine.getLatestSegmentInfos());
            // after flush our original segment_n is removed but some segments may remain.
            assertUnreferenced(nrtEngine, List.of(lastCommittedSegmentInfos.getSegmentsFileName()));
            assertUnreferenced(nrtEngine, mergedAwayFiles);
            // close the engine - ensure we preserved the last commit
            final SegmentInfos infosBeforeClose = nrtEngine.getLatestSegmentInfos();
            nrtEngine.close();
            assertRefCounted(nrtEngine, infosBeforeClose.files(false));
        }
    }

    private void assertRefCounted(NRTReplicationEngine nrtEngine, Collection<String> files) throws IOException {
        List<String> storeFiles = List.of(nrtEngine.store.directory().listAll());
        for (String file : files) {
            // refCount for our segments is 2 because they are still active on the reader
            assertTrue("Expected: " + file + " to be referenced", nrtEngine.replicaFileTracker.refCount(file) >= 1);
            assertTrue(storeFiles.contains(file));
        }
    }

    private void assertUnreferenced(NRTReplicationEngine nrtEngine, Collection<String> files) throws IOException {
        List<String> storeFiles = List.of(nrtEngine.store.directory().listAll());
        for (String file : files) {
            // refCount for our segments is 2 because they are still active on the reader
            assertEquals("Expected: " + file + " to be unreferenced", 0, nrtEngine.replicaFileTracker.refCount(file));
            assertFalse(storeFiles.contains(file));
        }
    }

    private void cleanAndCopySegmentsFromPrimary(NRTReplicationEngine nrtEngine) throws IOException {
        Lucene.cleanLuceneIndex(nrtEngine.store.directory());
        assertFalse(
            Arrays.stream(nrtEngine.store.directory().listAll())
                .anyMatch(file -> file.equals("write.lock") == false && file.equals("extra0") == false)
        );
        SegmentInfos primaryInfos = engine.getLatestSegmentInfos();
        copySegments(primaryInfos.files(false), nrtEngine);
        nrtEngine.updateSegments(primaryInfos);
    }

    private void indexOperations(NRTReplicationEngine nrtEngine, List<Engine.Operation> operations) throws IOException {
        for (Engine.Operation op : operations) {
            applyOperation(engine, op);
            applyOperation(nrtEngine, op);
            engine.refresh("test");
        }
    }

    public void testDecrefToZeroRemovesFile() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        try (
            final Store nrtEngineStore = createStore(INDEX_SETTINGS, newDirectory());
            final NRTReplicationEngine nrtEngine = buildNrtReplicaEngine(globalCheckpoint, nrtEngineStore, INDEX_SETTINGS)
        ) {
            Lucene.cleanLuceneIndex(nrtEngineStore.directory());
            copySegments(engine.getLatestSegmentInfos().files(true), nrtEngine);
            nrtEngine.updateSegments(engine.getLatestSegmentInfos());
            final SegmentInfos lastCommittedSegmentInfos = nrtEngine.getLastCommittedSegmentInfos();
            assertEquals(
                "Segments_N is incref'd to 1",
                1,
                nrtEngine.replicaFileTracker.refCount(lastCommittedSegmentInfos.getSegmentsFileName())
            );
            // create a new commit and update infos
            engine.flush(true, true);
            nrtEngine.updateSegments(engine.getLatestSegmentInfos());
            assertEquals(
                "Segments_N is removed",
                0,
                nrtEngine.replicaFileTracker.refCount(lastCommittedSegmentInfos.getSegmentsFileName())
            );
            assertFalse(List.of(nrtEngineStore.directory().listAll()).contains(lastCommittedSegmentInfos.getSegmentsFileName()));
        }
    }

    private void copySegments(Collection<String> latestPrimaryFiles, Engine nrtEngine) throws IOException {
        final Store store = nrtEngine.store;
        final List<String> replicaFiles = List.of(store.directory().listAll());
        // copy new segments in and load reader.
        for (String file : latestPrimaryFiles) {
            if (replicaFiles.contains(file) == false) {
                store.directory().copyFrom(this.store.directory(), file, file, IOContext.DEFAULT);
            }
        }
    }
}
