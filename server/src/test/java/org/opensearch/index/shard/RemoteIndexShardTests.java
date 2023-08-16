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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.SegmentReplicationSource;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.opensearch.index.engine.EngineTestCase.assertAtMostOneLuceneDocumentPerSequenceNumber;
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

    @Before
    public void setup() {
        // Todo: Remove feature flag once remote store integration with segrep goes GA
        FeatureFlags.initializeFeatureFlags(
            Settings.builder().put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL_SETTING.getKey(), "true").build()
        );
    }

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

    public void testCloseShardWhileGettingCheckpoint() throws Exception {
        String indexMapping = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": {} }";
        try (
            ReplicationGroup shards = createGroup(1, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory(), createTempDir())
        ) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);

            // Create custom replication source in order to trigger shard close operations at specific point of segment replication
            // lifecycle
            SegmentReplicationSource source = new TestReplicationSource() {
                RemoteSegmentStoreDirectory remoteDirectory;

                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    // shard is closing while fetching metadata
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                    FilterDirectory remoteStoreDirectory = (FilterDirectory) replica.remoteStore().directory();
                    FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
                    this.remoteDirectory = (RemoteSegmentStoreDirectory) byteSizeCachingStoreDirectory.getDelegate();

                    RemoteSegmentMetadata mdFile = null;
                    try {
                        mdFile = remoteDirectory.init();
                        final Version version = replica.getSegmentInfosSnapshot().get().getCommitLuceneVersion();
                        Map<String, StoreFileMetadata> metadataMap = mdFile.getMetadata()
                            .entrySet()
                            .stream()
                            .collect(
                                Collectors.toMap(
                                    e -> e.getKey(),
                                    e -> new StoreFileMetadata(
                                        e.getValue().getOriginalFilename(),
                                        e.getValue().getLength(),
                                        Store.digestToString(Long.valueOf(e.getValue().getChecksum())),
                                        version,
                                        null
                                    )
                                )
                            );
                        listener.onResponse(
                            new CheckpointInfoResponse(mdFile.getReplicationCheckpoint(), metadataMap, mdFile.getSegmentInfosBytes())
                        );
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    Assert.fail("Unreachable");
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, primary, targetService);
            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testBeforeIndexShardClosedWhileCopyingFiles() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            shards.indexDocs(10);
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                RemoteSegmentStoreDirectory remoteDirectory;

                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    try {
                        FilterDirectory remoteStoreDirectory = (FilterDirectory) replica.remoteStore().directory();
                        FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
                        this.remoteDirectory = (RemoteSegmentStoreDirectory) byteSizeCachingStoreDirectory.getDelegate();

                        RemoteSegmentMetadata mdFile = remoteDirectory.init();
                        final Version version = replica.getSegmentInfosSnapshot().get().getCommitLuceneVersion();
                        Map<String, StoreFileMetadata> metadataMap = mdFile.getMetadata()
                            .entrySet()
                            .stream()
                            .collect(
                                Collectors.toMap(
                                    e -> e.getKey(),
                                    e -> new StoreFileMetadata(
                                        e.getValue().getOriginalFilename(),
                                        e.getValue().getLength(),
                                        Store.digestToString(Long.valueOf(e.getValue().getChecksum())),
                                        version,
                                        null
                                    )
                                )
                            );
                        listener.onResponse(
                            new CheckpointInfoResponse(mdFile.getReplicationCheckpoint(), metadataMap, mdFile.getSegmentInfosBytes())
                        );
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    try {
                        // shard is closing while we are copying files.
                        targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                        final Directory storeDirectory = indexShard.store().directory();
                        for (StoreFileMetadata fileMetadata : filesToFetch) {
                            String file = fileMetadata.name();
                            storeDirectory.copyFrom(remoteDirectory, file, file, IOContext.DEFAULT);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, primary, targetService);
            shards.removeReplica(replica);
            closeShards(replica);
        }
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

            // As we are downloading segments from remote segment store on failover, there should not be
            // any operations replayed from translog
            assertEquals(prevOperationCount, nextPrimary.translogStats().estimatedNumberOfOperations());

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
            MatcherAssert.assertThat(
                "Segments are referenced in memory only",
                primaryEngine.getSegmentInfosSnapshot().get().files(false),
                containsInAnyOrder("_0.cfe", "_0.si", "_0.cfs")
            );

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
                containsInAnyOrder("_0.cfe", "_0.si", "_0.cfs", "segments_5")
            );
            MatcherAssert.assertThat(
                "Segments are referenced in memory",
                replicaEngine.getSegmentInfosSnapshot().get().files(false),
                containsInAnyOrder("_0.cfe", "_0.si", "_0.cfs")
            );
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

    public void testRepicaCleansUpOldCommitsWhenReceivingNew() throws Exception {
        final Path remotePath = createTempDir();
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory(), remotePath)) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            shards.indexDocs(1);
            flushShard(primary);
            replicateSegments(primary, shards.getReplicas());
            assertDocCount(primary, 1);
            assertDocCount(replica, 1);
            assertEquals("segments_4", replica.store().readLastCommittedSegmentsInfo().getSegmentsFileName());
            assertSingleSegmentFile(replica, "segments_4");

            shards.indexDocs(1);
            primary.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            assertDocCount(replica, 2);
            assertSingleSegmentFile(replica, "segments_4");

            shards.indexDocs(1);
            flushShard(primary);
            replicateSegments(primary, shards.getReplicas());
            assertDocCount(replica, 3);
            assertSingleSegmentFile(replica, "segments_5");

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

    private void assertSingleSegmentFile(IndexShard shard, String fileName) throws IOException {
        final Set<String> segmentsFileNames = Arrays.stream(shard.store().directory().listAll())
            .filter(file -> file.startsWith(IndexFileNames.SEGMENTS))
            .collect(Collectors.toSet());
        assertEquals("Expected a single segment file", 1, segmentsFileNames.size());
        assertEquals(segmentsFileNames.stream().findFirst().get(), fileName);
    }
}
