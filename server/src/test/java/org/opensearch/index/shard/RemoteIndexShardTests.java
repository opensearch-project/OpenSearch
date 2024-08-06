/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.Version;
import org.opensearch.action.StepListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.RemoteStoreReplicationSource;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.CorruptionUtils;
import org.opensearch.test.junit.annotations.TestLogging;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.opensearch.index.engine.EngineTestCase.assertAtMostOneLuceneDocumentPerSequenceNumber;
import static org.opensearch.index.shard.RemoteStoreRefreshListener.EXCLUDE_FILES;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexShardTests extends SegmentReplicationIndexShardTests {

    private static final String REPOSITORY_NAME = "temp-fs";
    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
        .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, REPOSITORY_NAME)
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, REPOSITORY_NAME)
        .build();

    protected Settings getIndexSettings() {
        return settings;
    }

    protected ReplicationGroup getReplicationGroup(int numberOfReplicas) throws IOException {
        return createGroup(numberOfReplicas, settings, indexMapping, new NRTReplicationEngineFactory(), createTempDir());
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimaryRefreshRefresh() throws Exception {
        testNRTReplicaWithRemoteStorePromotedAsPrimary(false, false);
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimaryRefreshCommit() throws Exception {
        testNRTReplicaWithRemoteStorePromotedAsPrimary(false, true);
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimaryCommitRefresh() throws Exception {
        testNRTReplicaWithRemoteStorePromotedAsPrimary(true, false);
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimaryCommitCommit() throws Exception {
        testNRTReplicaWithRemoteStorePromotedAsPrimary(true, true);
    }

    public void testNRTReplicaWithRemoteStorePromotedAsPrimary(boolean performFlushFirst, boolean performFlushSecond) throws Exception {
        try (
            ReplicationGroup shards = createGroup(1, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory(), createTempDir())
        ) {
            shards.startAll();
            IndexShard oldPrimary = shards.getPrimary();
            final IndexShard nextPrimary = shards.getReplicas().get(0);

            // 1. Create ops that are in the index and xlog of both shards but not yet part of a commit point.
            final int numDocs = shards.indexDocs(randomInt(10));

            // refresh but do not copy the segments over.
            if (performFlushFirst) {
                flushShard(oldPrimary, true);
            } else {
                oldPrimary.refresh("Test");
            }
            // replicateSegments(primary, shards.getReplicas());

            // at this point both shards should have numDocs persisted and searchable.
            assertDocCounts(oldPrimary, numDocs, numDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, numDocs, 0);
            }

            // 2. Create ops that are in the replica's xlog, not in the index.
            // index some more into both but don't replicate. replica will have only numDocs searchable, but should have totalDocs
            // persisted.
            final int additonalDocs = shards.indexDocs(randomInt(10));
            final int totalDocs = numDocs + additonalDocs;

            if (performFlushSecond) {
                flushShard(oldPrimary, true);
            } else {
                oldPrimary.refresh("Test");
            }
            assertDocCounts(oldPrimary, totalDocs, totalDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, totalDocs, 0);
            }
            assertTrue(nextPrimary.translogStats().estimatedNumberOfOperations() >= additonalDocs);
            assertTrue(nextPrimary.translogStats().getUncommittedOperations() >= additonalDocs);

            int prevOperationCount = nextPrimary.translogStats().estimatedNumberOfOperations();

            // promote the replica
            shards.promoteReplicaToPrimary(nextPrimary).get();

            // close oldPrimary.
            oldPrimary.close("demoted", false, false);
            oldPrimary.store().close();

            assertEquals(InternalEngine.class, nextPrimary.getEngine().getClass());
            assertDocCounts(nextPrimary, totalDocs, totalDocs);

            // refresh and push segments to our other replica.
            nextPrimary.refresh("test");

            for (IndexShard shard : shards) {
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
            final List<DocIdSeqNoAndSource> docsAfterRecovery = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterRecovery));
            }
        }
    }

    public void testNoDuplicateSeqNo() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
        ReplicationGroup shards = createGroup(1, settings, indexMapping, new NRTReplicationEngineFactory(), createTempDir());
        final IndexShard primaryShard = shards.getPrimary();
        final IndexShard replicaShard = shards.getReplicas().get(0);
        shards.startPrimary();
        shards.startAll();
        shards.indexDocs(10);
        replicateSegments(primaryShard, shards.getReplicas());

        flushShard(primaryShard);
        shards.indexDocs(10);
        replicateSegments(primaryShard, shards.getReplicas());

        shards.indexDocs(10);
        primaryShard.refresh("test");
        replicateSegments(primaryShard, shards.getReplicas());

        CountDownLatch latch = new CountDownLatch(1);
        shards.promoteReplicaToPrimary(replicaShard, (shard, listener) -> {
            try {
                assertAtMostOneLuceneDocumentPerSequenceNumber(replicaShard.getEngine());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        });
        latch.await();
        for (IndexShard shard : shards) {
            if (shard != null) {
                closeShard(shard, false);
            }
        }
    }

    public void testReplicaCommitsInfosBytesOnRecovery() throws Exception {
        final Path remotePath = createTempDir();
        try (ReplicationGroup shards = createGroup(0, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory(), remotePath)) {
            shards.startAll();
            // ensure primary has uploaded something
            shards.indexDocs(10);
            shards.refresh("test");

            final IndexShard primary = shards.getPrimary();
            final Engine primaryEngine = getEngine(primary);
            assertNotNull(primaryEngine);
            final SegmentInfos latestCommit = SegmentInfos.readLatestCommit(primary.store().directory());
            assertEquals("On-disk commit references no segments", Set.of("segments_3"), latestCommit.files(true));
            assertEquals(
                "Latest remote commit On-disk commit references no segments",
                Set.of("segments_3"),
                primary.remoteStore().readLastCommittedSegmentsInfo().files(true)
            );

            try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = primaryEngine.getSegmentInfosSnapshot()) {
                MatcherAssert.assertThat(
                    "Segments are referenced in memory only",
                    segmentInfosSnapshot.get().files(false),
                    containsInAnyOrder("_0.cfe", "_0.si", "_0.cfs")
                );
            }

            final IndexShard replica = shards.addReplica(remotePath);
            replica.store().createEmpty(Version.LATEST);
            assertEquals(
                "Replica starts at empty segment 2",
                Set.of("segments_1"),
                replica.store().readLastCommittedSegmentsInfo().files(true)
            );
            // commit replica infos so it has a conflicting commit with remote.
            final SegmentInfos segmentCommitInfos = replica.store().readLastCommittedSegmentsInfo();
            segmentCommitInfos.commit(replica.store().directory());
            segmentCommitInfos.commit(replica.store().directory());
            assertEquals(
                "Replica starts recovery at empty segment 3",
                Set.of("segments_3"),
                replica.store().readLastCommittedSegmentsInfo().files(true)
            );

            shards.recoverReplica(replica);

            final Engine replicaEngine = getEngine(replica);
            assertNotNull(replicaEngine);
            final SegmentInfos latestReplicaCommit = SegmentInfos.readLatestCommit(replica.store().directory());
            logger.info(List.of(replica.store().directory().listAll()));
            MatcherAssert.assertThat(
                "Replica commits infos bytes referencing latest refresh point",
                latestReplicaCommit.files(true),
                containsInAnyOrder("_0.cfe", "_0.si", "_0.cfs", "segments_6")
            );

            try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = replicaEngine.getSegmentInfosSnapshot()) {
                MatcherAssert.assertThat(
                    "Segments are referenced in memory",
                    segmentInfosSnapshot.get().files(false),
                    containsInAnyOrder("_0.cfe", "_0.si", "_0.cfs")
                );
            }

            final Store.RecoveryDiff recoveryDiff = Store.segmentReplicationDiff(
                primary.getSegmentMetadataMap(),
                replica.getSegmentMetadataMap()
            );
            assertTrue(recoveryDiff.missing.isEmpty());
            assertTrue(recoveryDiff.different.isEmpty());
        }
    }

    public void testPrimaryRestart_PrimaryHasExtraCommits() throws Exception {
        final Path remotePath = createTempDir();
        try (ReplicationGroup shards = createGroup(0, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory(), remotePath)) {
            shards.startAll();
            // ensure primary has uploaded something
            shards.indexDocs(10);
            IndexShard primary = shards.getPrimary();
            if (randomBoolean()) {
                flushShard(primary);
            } else {
                primary.refresh("test");
            }
            assertDocCount(primary, 10);
            // get a metadata map - we'll use segrep diff to ensure segments on reader are identical after restart.
            final Map<String, StoreFileMetadata> metadataBeforeRestart = primary.getSegmentMetadataMap();
            // restart the primary
            shards.reinitPrimaryShard(remotePath);
            // the store is open at this point but the shard has not yet run through recovery
            primary = shards.getPrimary();
            SegmentInfos latestPrimaryCommit = SegmentInfos.readLatestCommit(primary.store().directory());
            latestPrimaryCommit.commit(primary.store().directory());
            latestPrimaryCommit = SegmentInfos.readLatestCommit(primary.store().directory());
            latestPrimaryCommit.commit(primary.store().directory());
            shards.startPrimary();
            assertDocCount(primary, 10);
            final Store.RecoveryDiff diff = Store.segmentReplicationDiff(metadataBeforeRestart, primary.getSegmentMetadataMap());
            assertTrue(diff.missing.isEmpty());
            assertTrue(diff.different.isEmpty());
        }
    }

    @TestLogging(reason = "Getting trace logs from replication package", value = "org.opensearch.indices.replication:TRACE")
    public void testRepicaCleansUpOldCommitsWhenReceivingNew() throws Exception {
        final Path remotePath = createTempDir();
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory(), remotePath)) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final Store store = replica.store();
            final SegmentInfos initialCommit = store.readLastCommittedSegmentsInfo();
            shards.indexDocs(1);
            flushShard(primary);
            replicateSegments(primary, shards.getReplicas());

            assertDocCount(primary, 1);
            assertDocCount(replica, 1);
            assertSingleSegmentFile(replica);
            final SegmentInfos secondCommit = store.readLastCommittedSegmentsInfo();
            assertTrue(secondCommit.getGeneration() > initialCommit.getGeneration());

            shards.indexDocs(1);
            primary.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            assertDocCount(replica, 2);
            assertSingleSegmentFile(replica);
            assertEquals(store.readLastCommittedSegmentsInfo().getGeneration(), secondCommit.getGeneration());

            shards.indexDocs(1);
            flushShard(primary);
            replicateSegments(primary, shards.getReplicas());
            assertDocCount(replica, 3);
            assertSingleSegmentFile(replica);
            final SegmentInfos thirdCommit = store.readLastCommittedSegmentsInfo();
            assertTrue(thirdCommit.getGeneration() > secondCommit.getGeneration());

            final Store.RecoveryDiff diff = Store.segmentReplicationDiff(primary.getSegmentMetadataMap(), replica.getSegmentMetadataMap());
            assertTrue(diff.missing.isEmpty());
            assertTrue(diff.different.isEmpty());
        }
    }

    public void testPrimaryRestart() throws Exception {
        final Path remotePath = createTempDir();
        try (ReplicationGroup shards = createGroup(0, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory(), remotePath)) {
            shards.startAll();
            // ensure primary has uploaded something
            shards.indexDocs(10);
            IndexShard primary = shards.getPrimary();
            if (randomBoolean()) {
                flushShard(primary);
            } else {
                primary.refresh("test");
            }
            assertDocCount(primary, 10);
            // get a metadata map - we'll use segrep diff to ensure segments on reader are identical after restart.
            final Map<String, StoreFileMetadata> metadataBeforeRestart = primary.getSegmentMetadataMap();
            // restart the primary
            shards.reinitPrimaryShard(remotePath);
            // the store is open at this point but the shard has not yet run through recovery
            primary = shards.getPrimary();
            shards.startPrimary();
            assertDocCount(primary, 10);
            final Store.RecoveryDiff diff = Store.segmentReplicationDiff(metadataBeforeRestart, primary.getSegmentMetadataMap());
            assertTrue(diff.missing.isEmpty());
            logger.info("DIFF FILE {}", diff.different);
            assertTrue(diff.different.isEmpty());
        }
    }

    /**
     * This test validates that unreferenced on disk file are ignored while requesting files from replication source to
     * prevent FileAlreadyExistsException. It does so by only copying files in first round of segment replication without
     * committing locally so that in next round of segment replication those files are not considered for download again
     */
    public void testSegRepSucceedsOnPreviousCopiedFiles() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            shards.indexDocs(10);
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            when(sourceFactory.get(any())).thenReturn(
                getRemoteStoreReplicationSource(replica, () -> { throw new RuntimeException("Simulated"); })
            );
            CountDownLatch latch = new CountDownLatch(1);

            logger.info("--> Starting first round of replication");
            // Start first round of segment replication. This should fail with simulated error but with replica having
            // files in its local store but not in active reader.
            final SegmentReplicationTarget target = targetService.startReplication(
                replica,
                primary.getLatestReplicationCheckpoint(),
                new SegmentReplicationTargetService.SegmentReplicationListener() {
                    @Override
                    public void onReplicationDone(SegmentReplicationState state) {
                        latch.countDown();
                        Assert.fail("Replication should fail with simulated error");
                    }

                    @Override
                    public void onReplicationFailure(
                        SegmentReplicationState state,
                        ReplicationFailedException e,
                        boolean sendShardFailure
                    ) {
                        latch.countDown();
                        assertFalse(sendShardFailure);
                        logger.error("Replication error", e);
                    }
                }
            );
            latch.await();
            Set<String> onDiskFiles = new HashSet<>(Arrays.asList(replica.store().directory().listAll()));
            onDiskFiles.removeIf(name -> EXCLUDE_FILES.contains(name) || name.startsWith(IndexFileNames.SEGMENTS));
            List<String> activeFiles = replica.getSegmentMetadataMap()
                .values()
                .stream()
                .map(metadata -> metadata.name())
                .collect(Collectors.toList());
            assertTrue("Files should not be committed", activeFiles.isEmpty());
            assertEquals("Files should be copied to disk", false, onDiskFiles.isEmpty());
            assertEquals(target.state().getStage(), SegmentReplicationState.Stage.GET_FILES);

            // Start next round of segment replication and not throwing exception resulting in commit on replica
            when(sourceFactory.get(any())).thenReturn(getRemoteStoreReplicationSource(replica, () -> {}));
            CountDownLatch waitForSecondRound = new CountDownLatch(1);
            logger.info("--> Starting second round of replication");
            final SegmentReplicationTarget newTarget = targetService.startReplication(
                replica,
                primary.getLatestReplicationCheckpoint(),
                new SegmentReplicationTargetService.SegmentReplicationListener() {
                    @Override
                    public void onReplicationDone(SegmentReplicationState state) {
                        waitForSecondRound.countDown();
                    }

                    @Override
                    public void onReplicationFailure(
                        SegmentReplicationState state,
                        ReplicationFailedException e,
                        boolean sendShardFailure
                    ) {
                        waitForSecondRound.countDown();
                        logger.error("Replication error", e);
                        Assert.fail("Replication should not fail");
                    }
                }
            );
            waitForSecondRound.await();
            assertEquals(newTarget.state().getStage(), SegmentReplicationState.Stage.DONE);
            activeFiles = replica.getSegmentMetadataMap().values().stream().map(metadata -> metadata.name()).collect(Collectors.toList());
            assertTrue("Replica should have consistent disk & reader", activeFiles.containsAll(onDiskFiles));
            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    /**
     * This test validates that local non-readable (corrupt, partially) on disk are deleted vs failing the
     * replication event. This test mimics local files (not referenced by reader) by throwing exception post file copy and
     * blocking update of reader. Once this is done, it corrupts one segment file and ensure that file is deleted in next
     * round of segment replication by ensuring doc count.
     */
    public void testNoFailuresOnFileReads() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int docCount = 10;
            shards.indexDocs(docCount);
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            when(sourceFactory.get(any())).thenReturn(
                getRemoteStoreReplicationSource(replica, () -> { throw new RuntimeException("Simulated"); })
            );
            CountDownLatch waitOnReplicationCompletion = new CountDownLatch(1);

            // Start first round of segment replication. This should fail with simulated error but with replica having
            // files in its local store but not in active reader.
            SegmentReplicationTarget segmentReplicationTarget = targetService.startReplication(
                replica,
                primary.getLatestReplicationCheckpoint(),
                new SegmentReplicationTargetService.SegmentReplicationListener() {
                    @Override
                    public void onReplicationDone(SegmentReplicationState state) {
                        waitOnReplicationCompletion.countDown();
                        Assert.fail("Replication should fail with simulated error");
                    }

                    @Override
                    public void onReplicationFailure(
                        SegmentReplicationState state,
                        ReplicationFailedException e,
                        boolean sendShardFailure
                    ) {
                        waitOnReplicationCompletion.countDown();
                        assertFalse(sendShardFailure);
                    }
                }
            );
            waitOnReplicationCompletion.await();
            assertBusy(() -> { assertEquals("Target should be closed", 0, segmentReplicationTarget.refCount()); });
            String fileToCorrupt = null;
            // Corrupt one data file
            Path shardPath = replica.shardPath().getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);
            for (String file : replica.store().directory().listAll()) {
                if (file.equals("write.lock") || file.startsWith("extra") || file.startsWith("segment")) {
                    continue;
                }
                fileToCorrupt = file;
                logger.info("--> Corrupting file {}", fileToCorrupt);
                try (FileChannel raf = FileChannel.open(shardPath.resolve(file), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    CorruptionUtils.corruptAt(shardPath.resolve(file), raf, (int) (raf.size() - 8));
                }
                break;
            }
            Assert.assertNotNull(fileToCorrupt);

            // Ingest more data and start next round of segment replication
            shards.indexDocs(docCount);
            primary.refresh("Post corruption");
            replicateSegments(primary, List.of(replica));

            assertDocCount(primary, 2 * docCount);
            assertDocCount(replica, 2 * docCount);

            final Store.RecoveryDiff diff = Store.segmentReplicationDiff(primary.getSegmentMetadataMap(), replica.getSegmentMetadataMap());
            assertTrue(diff.missing.isEmpty());
            assertTrue(diff.different.isEmpty());

            // clean up
            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    private RemoteStoreReplicationSource getRemoteStoreReplicationSource(IndexShard shard, Runnable postGetFilesRunnable) {
        return new RemoteStoreReplicationSource(shard) {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                super.getCheckpointMetadata(replicationId, checkpoint, listener);
            }

            @Override
            public void getSegmentFiles(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                List<StoreFileMetadata> filesToFetch,
                IndexShard indexShard,
                BiConsumer<String, Long> fileProgressTracker,
                ActionListener<GetSegmentFilesResponse> listener
            ) {
                StepListener<GetSegmentFilesResponse> waitForCopyFilesListener = new StepListener();
                super.getSegmentFiles(
                    replicationId,
                    checkpoint,
                    filesToFetch,
                    indexShard,
                    (fileName, bytesRecovered) -> {},
                    waitForCopyFilesListener
                );
                waitForCopyFilesListener.whenComplete(response -> {
                    postGetFilesRunnable.run();
                    listener.onResponse(response);
                }, listener::onFailure);
            }

            @Override
            public String getDescription() {
                return "TestRemoteStoreReplicationSource";
            }
        };
    }

    @Override
    protected void validateShardIdleWithNoReplicas(IndexShard primary) {
        // ensure search idle conditions are met.
        assertFalse(primary.isSearchIdleSupported());
        assertTrue(primary.isSearchIdle());
        assertTrue(primary.scheduledRefresh());
        assertFalse(primary.hasRefreshPending());
    }

    private void assertSingleSegmentFile(IndexShard shard) throws IOException {
        final Set<String> segmentsFileNames = Arrays.stream(shard.store().directory().listAll())
            .filter(file -> file.startsWith(IndexFileNames.SEGMENTS))
            .collect(Collectors.toSet());
        assertEquals("Expected a single segment file", 1, segmentsFileNames.size());
    }
}
