/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.index.SegmentInfos;
import org.junit.Assert;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.SegmentReplicationSource;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentReplicationWithNodeToNodeIndexShardTests extends SegmentReplicationIndexShardTests {

    public void testReplicaClosesWhileReplicating_AfterGetSegmentFiles() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    resolveCheckpointInfoResponseListener(listener, primary);
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    // randomly resolve the listener, indicating the source has resolved.
                    listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testReplicaClosesWhileReplicating_AfterGetCheckpoint() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    // trigger a cancellation by closing the replica.
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                    resolveCheckpointInfoResponseListener(listener, primary);
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    Assert.fail("Should not be reached");
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testCloseShardWhileGettingCheckpoint() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {

                ActionListener<CheckpointInfoResponse> listener;

                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    // set the listener, we will only fail it once we cancel the source.
                    this.listener = listener;
                    // shard is closing while we are copying files.
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
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

                @Override
                public void cancel() {
                    // simulate listener resolving, but only after we have issued a cancel from beforeIndexShardClosed .
                    final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("retryable action was cancelled");
                    listener.onFailure(exception);
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testPrimaryCancelsExecution() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    listener.onFailure(new CancellableThreads.ExecutionCancelledException("Cancelled"));
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {}
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    public void testReplicaPromotedWhileReplicating() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard nextPrimary = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            oldPrimary.refresh("Test");
            shards.syncGlobalCheckpoint();

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {
                @Override
                public void getCheckpointMetadata(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    ActionListener<CheckpointInfoResponse> listener
                ) {
                    resolveCheckpointInfoResponseListener(listener, oldPrimary);
                    ShardRouting oldRouting = nextPrimary.shardRouting;
                    try {
                        shards.promoteReplicaToPrimary(nextPrimary);
                    } catch (IOException e) {
                        Assert.fail("Promotion should not fail");
                    }
                    targetService.shardRoutingChanged(nextPrimary, oldRouting, nextPrimary.shardRouting);
                }

                @Override
                public void getSegmentFiles(
                    long replicationId,
                    ReplicationCheckpoint checkpoint,
                    List<StoreFileMetadata> filesToFetch,
                    IndexShard indexShard,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(nextPrimary, targetService);
            // wait for replica to finish being promoted, and assert doc counts.
            final CountDownLatch latch = new CountDownLatch(1);
            nextPrimary.acquirePrimaryOperationPermit(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            }, ThreadPool.Names.GENERIC, "");
            latch.await();
            assertEquals(nextPrimary.getEngine().getClass(), InternalEngine.class);
            nextPrimary.refresh("test");

            oldPrimary.close("demoted", false, false);
            oldPrimary.store().close();
            IndexShard newReplica = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            assertDocCount(nextPrimary, numDocs);
            assertDocCount(newReplica, numDocs);

            nextPrimary.refresh("test");
            replicateSegments(nextPrimary, shards.getReplicas());
            final List<DocIdSeqNoAndSource> docsAfterRecovery = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterRecovery));
            }
        }
    }

    public void testReplicaReceivesGenIncrease() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final int numDocs = randomIntBetween(10, 100);
            shards.indexDocs(numDocs);
            assertEquals(numDocs, primary.translogStats().estimatedNumberOfOperations());
            assertEquals(numDocs, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(numDocs, primary.translogStats().getUncommittedOperations());
            assertEquals(numDocs, replica.translogStats().getUncommittedOperations());
            flushShard(primary, true);
            replicateSegments(primary, shards.getReplicas());
            assertEquals(0, primary.translogStats().estimatedNumberOfOperations());
            assertEquals(0, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(0, primary.translogStats().getUncommittedOperations());
            assertEquals(0, replica.translogStats().getUncommittedOperations());

            final int additionalDocs = shards.indexDocs(randomIntBetween(numDocs + 1, numDocs + 10));

            final int totalDocs = numDocs + additionalDocs;
            primary.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            assertEquals(additionalDocs, primary.translogStats().estimatedNumberOfOperations());
            assertEquals(additionalDocs, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(additionalDocs, primary.translogStats().getUncommittedOperations());
            assertEquals(additionalDocs, replica.translogStats().getUncommittedOperations());
            flushShard(primary, true);
            replicateSegments(primary, shards.getReplicas());

            assertEqualCommittedSegments(primary, replica);
            assertDocCount(primary, totalDocs);
            assertDocCount(replica, totalDocs);
            assertEquals(0, primary.translogStats().estimatedNumberOfOperations());
            assertEquals(0, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(0, primary.translogStats().getUncommittedOperations());
            assertEquals(0, replica.translogStats().getUncommittedOperations());
        }
    }

    /**
     * Verifies that commits on replica engine resulting from engine or reader close does not cleanup the temporary
     * replication files from ongoing round of segment replication
     * @throws Exception
     */
    public void testTemporaryFilesNotCleanup() throws Exception {
        String mappings = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": { \"properties\": { \"foo\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), mappings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primaryShard = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            // Step 1. Ingest numDocs documents, commit to create commit point on primary & replicate
            final int numDocs = randomIntBetween(100, 200);
            logger.info("--> Inserting documents {}", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(numDocs);

            // Step 2. Ingest numDocs documents again to create a new commit on primary
            logger.info("--> Ingest {} docs again", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));

            // Step 3. Copy segment files to replica shard but prevent commit
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            Map<String, StoreFileMetadata> primaryMetadata;
            try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = primaryShard.getSegmentInfosSnapshot()) {
                final SegmentInfos primarySegmentInfos = segmentInfosSnapshot.get();
                primaryMetadata = primaryShard.store().getSegmentMetadataMap(primarySegmentInfos);
            }
            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final IndicesService indicesService = mock(IndicesService.class);
            when(indicesService.getShardOrNull(replica.shardId)).thenReturn(replica);
            final SegmentReplicationTargetService targetService = new SegmentReplicationTargetService(
                threadPool,
                new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
                mock(TransportService.class),
                sourceFactory,
                indicesService,
                clusterService
            );
            final Consumer<IndexShard> runnablePostGetFiles = (indexShard) -> {
                try {
                    Collection<String> temporaryFiles = Stream.of(indexShard.store().directory().listAll())
                        .filter(name -> name.startsWith(SegmentReplicationTarget.REPLICATION_PREFIX))
                        .collect(Collectors.toList());

                    // Step 4. Perform a commit on replica shard.
                    NRTReplicationEngine engine = (NRTReplicationEngine) indexShard.getEngine();
                    engine.updateSegments(engine.getSegmentInfosSnapshot().get());

                    // Step 5. Validate temporary files are not deleted from store.
                    Collection<String> replicaStoreFiles = List.of(indexShard.store().directory().listAll());
                    assertTrue(replicaStoreFiles.containsAll(temporaryFiles));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            SegmentReplicationSource segmentReplicationSource = getSegmentReplicationSource(
                primaryShard,
                (repId) -> targetService.get(repId),
                runnablePostGetFiles
            );
            when(sourceFactory.get(any())).thenReturn(segmentReplicationSource);
            targetService.startReplication(replica, getTargetListener(primaryShard, replica, primaryMetadata, countDownLatch));
            countDownLatch.await(30, TimeUnit.SECONDS);
            assertEquals("Replication failed", 0, countDownLatch.getCount());
            shards.assertAllEqual(numDocs);
        }
    }

    // Todo: Move this test to SegmentReplicationIndexShardTests so that it runs for both node-node & remote store
    public void testReplicaReceivesLowerGeneration() throws Exception {
        // when a replica gets incoming segments that are lower than what it currently has on disk.

        // start 3 nodes Gens: P [2], R [2], R[2]
        // index some docs and flush twice, push to only 1 replica.
        // State Gens: P [4], R-1 [3], R-2 [2]
        // Promote R-2 as the new primary and demote the old primary.
        // State Gens: R[4], R-1 [3], P [4] - *commit on close of NRTEngine, xlog replayed and commit made.
        // index docs on new primary and flush
        // replicate to all.
        // Expected result: State Gens: P[4], R-1 [4], R-2 [4]
        try (ReplicationGroup shards = createGroup(2, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica_1 = shards.getReplicas().get(0);
            final IndexShard replica_2 = shards.getReplicas().get(1);
            int numDocs = randomIntBetween(10, 100);
            shards.indexDocs(numDocs);
            flushShard(primary, false);
            replicateSegments(primary, List.of(replica_1));
            numDocs = randomIntBetween(numDocs + 1, numDocs + 10);
            shards.indexDocs(numDocs);
            flushShard(primary, false);
            replicateSegments(primary, List.of(replica_1));

            assertEqualCommittedSegments(primary, replica_1);

            shards.promoteReplicaToPrimary(replica_2).get();
            primary.close("demoted", false, false);
            primary.store().close();
            IndexShard oldPrimary = shards.addReplicaWithExistingPath(primary.shardPath(), primary.routingEntry().currentNodeId());
            shards.recoverReplica(oldPrimary);

            numDocs = randomIntBetween(numDocs + 1, numDocs + 10);
            shards.indexDocs(numDocs);
            flushShard(replica_2, false);
            replicateSegments(replica_2, shards.getReplicas());
            assertEqualCommittedSegments(replica_2, oldPrimary, replica_1);
        }
    }

    // Todo: Move this test to SegmentReplicationIndexShardTests so that it runs for both node-node & remote store
    public void testPrimaryRelocation() throws Exception {
        final IndexShard primarySource = newStartedShard(true, getIndexSettings());
        int totalOps = randomInt(10);
        for (int i = 0; i < totalOps; i++) {
            indexDoc(primarySource, "_doc", Integer.toString(i));
        }
        IndexShardTestCase.updateRoutingEntry(primarySource, primarySource.routingEntry().relocate(randomAlphaOfLength(10), -1));
        final IndexShard primaryTarget = newShard(
            primarySource.routingEntry().getTargetRelocatingShard(),
            getIndexSettings(),
            new NRTReplicationEngineFactory()
        );
        updateMappings(primaryTarget, primarySource.indexSettings().getIndexMetadata());

        Function<List<IndexShard>, List<SegmentReplicationTarget>> replicatePrimaryFunction = (shardList) -> {
            try {
                assert shardList.size() >= 2;
                final IndexShard primary = shardList.get(0);
                return replicateSegments(primary, shardList.subList(1, shardList.size()));
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        recoverReplica(primaryTarget, primarySource, true, replicatePrimaryFunction);

        // check that local checkpoint of new primary is properly tracked after primary relocation
        assertThat(primaryTarget.getLocalCheckpoint(), equalTo(totalOps - 1L));
        assertThat(
            primaryTarget.getReplicationTracker()
                .getTrackedLocalCheckpointForShard(primaryTarget.routingEntry().allocationId().getId())
                .getLocalCheckpoint(),
            equalTo(totalOps - 1L)
        );
        assertDocCount(primaryTarget, totalOps);
        closeShards(primarySource, primaryTarget);
    }

    // Todo: Move this test to SegmentReplicationIndexShardTests so that it runs for both node-node & remote store
    public void testNRTReplicaPromotedAsPrimary() throws Exception {
        try (ReplicationGroup shards = createGroup(2, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard oldPrimary = shards.getPrimary();
            final IndexShard nextPrimary = shards.getReplicas().get(0);
            final IndexShard replica = shards.getReplicas().get(1);

            // 1. Create ops that are in the index and xlog of both shards but not yet part of a commit point.
            final int numDocs = shards.indexDocs(randomInt(10));

            // refresh and copy the segments over.
            oldPrimary.refresh("Test");
            replicateSegments(oldPrimary, shards.getReplicas());

            // at this point both shards should have numDocs persisted and searchable.
            assertDocCounts(oldPrimary, numDocs, numDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, numDocs, numDocs);
            }
            assertEqualTranslogOperations(shards, oldPrimary);

            // 2. Create ops that are in the replica's xlog, not in the index.
            // index some more into both but don't replicate. replica will have only numDocs searchable, but should have totalDocs
            // persisted.
            final int additonalDocs = shards.indexDocs(randomInt(10));
            final int totalDocs = numDocs + additonalDocs;

            assertDocCounts(oldPrimary, totalDocs, totalDocs);
            assertEqualTranslogOperations(shards, oldPrimary);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, totalDocs, numDocs);
            }
            assertEquals(totalDocs, oldPrimary.translogStats().estimatedNumberOfOperations());
            assertEquals(totalDocs, oldPrimary.translogStats().estimatedNumberOfOperations());
            assertEquals(totalDocs, nextPrimary.translogStats().estimatedNumberOfOperations());
            assertEquals(totalDocs, replica.translogStats().estimatedNumberOfOperations());
            assertEquals(totalDocs, nextPrimary.translogStats().getUncommittedOperations());
            assertEquals(totalDocs, replica.translogStats().getUncommittedOperations());

            // promote the replica
            shards.syncGlobalCheckpoint();
            shards.promoteReplicaToPrimary(nextPrimary);

            // close and start the oldPrimary as a replica.
            oldPrimary.close("demoted", false, false);
            oldPrimary.store().close();
            oldPrimary = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(oldPrimary);

            assertEquals(NRTReplicationEngine.class, oldPrimary.getEngine().getClass());
            assertEquals(InternalEngine.class, nextPrimary.getEngine().getClass());
            assertDocCounts(nextPrimary, totalDocs, totalDocs);
            assertEquals(0, nextPrimary.translogStats().estimatedNumberOfOperations());

            // refresh and push segments to our other replica.
            nextPrimary.refresh("test");
            replicateSegments(nextPrimary, asList(replica));

            for (IndexShard shard : shards) {
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
            final List<DocIdSeqNoAndSource> docsAfterRecovery = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterRecovery));
            }
        }
    }

    // Todo: Move this test to SegmentReplicationIndexShardTests so that it runs for both node-node & remote store
    public void testReplicaRestarts() throws Exception {
        try (ReplicationGroup shards = createGroup(3, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            // 1. Create ops that are in the index and xlog of both shards but not yet part of a commit point.
            final int numDocs = shards.indexDocs(randomInt(10));
            logger.info("--> Index {} documents on primary", numDocs);

            // refresh and copy the segments over.
            if (randomBoolean()) {
                flushShard(primary);
            }
            primary.refresh("Test");
            logger.info("--> Replicate segments");
            replicateSegments(primary, shards.getReplicas());

            // at this point both shards should have numDocs persisted and searchable.
            logger.info("--> Verify doc count");
            assertDocCounts(primary, numDocs, numDocs);
            for (IndexShard shard : shards.getReplicas()) {
                assertDocCounts(shard, numDocs, numDocs);
            }

            final int i1 = randomInt(5);
            logger.info("--> Index {} more docs", i1);
            for (int i = 0; i < i1; i++) {
                shards.indexDocs(randomInt(10));

                // randomly restart a replica
                final IndexShard replicaToRestart = getRandomReplica(shards);
                logger.info("--> Restarting replica {}", replicaToRestart.shardId);
                replicaToRestart.close("restart", false, false);
                replicaToRestart.store().close();
                shards.removeReplica(replicaToRestart);
                final IndexShard newReplica = shards.addReplicaWithExistingPath(
                    replicaToRestart.shardPath(),
                    replicaToRestart.routingEntry().currentNodeId()
                );
                logger.info("--> Recover newReplica {}", newReplica.shardId);
                shards.recoverReplica(newReplica);

                // refresh and push segments to our other replicas.
                if (randomBoolean()) {
                    failAndPromoteRandomReplica(shards);
                }
                flushShard(shards.getPrimary());
                replicateSegments(shards.getPrimary(), shards.getReplicas());
            }
            primary = shards.getPrimary();

            // refresh and push segments to our other replica.
            flushShard(primary);
            replicateSegments(primary, shards.getReplicas());

            for (IndexShard shard : shards) {
                assertConsistentHistoryBetweenTranslogAndLucene(shard);
            }
            final List<DocIdSeqNoAndSource> docsAfterReplication = getDocIdAndSeqNos(shards.getPrimary());
            for (IndexShard shard : shards.getReplicas()) {
                assertThat(shard.routingEntry().toString(), getDocIdAndSeqNos(shard), equalTo(docsAfterReplication));
            }
        }
    }

    // Todo: Move this test to SegmentReplicationIndexShardTests so that it runs for both node-node & remote store
    public void testSegmentReplication_Index_Update_Delete() throws Exception {
        String mappings = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": { \"properties\": { \"foo\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = createGroup(2, getIndexSettings(), mappings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primaryShard = shards.getPrimary();

            final int numDocs = randomIntBetween(100, 200);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", XContentType.JSON));
            }

            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.refresh("Test");
            replicateSegments(primaryShard, shards.getReplicas());

            shards.assertAllEqual(numDocs);

            for (int i = 0; i < numDocs; i++) {
                // randomly update docs.
                if (randomBoolean()) {
                    shards.index(
                        new IndexRequest(index.getName()).id(String.valueOf(i)).source("{ \"foo\" : \"baz\" }", XContentType.JSON)
                    );
                }
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.refresh("Test");
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(numDocs);

            final List<DocIdSeqNoAndSource> docs = getDocIdAndSeqNos(primaryShard);
            for (IndexShard shard : shards.getReplicas()) {
                assertEquals(getDocIdAndSeqNos(shard), docs);
            }
            for (int i = 0; i < numDocs; i++) {
                // randomly delete.
                if (randomBoolean()) {
                    shards.delete(new DeleteRequest(index.getName()).id(String.valueOf(i)));
                }
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.refresh("Test");
            replicateSegments(primaryShard, shards.getReplicas());
            final List<DocIdSeqNoAndSource> docsAfterDelete = getDocIdAndSeqNos(primaryShard);
            for (IndexShard shard : shards.getReplicas()) {
                assertEquals(getDocIdAndSeqNos(shard), docsAfterDelete);
            }
        }
    }

}
