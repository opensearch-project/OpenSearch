/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.engine.ReadOnlyEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.SnapshotMatchers;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.SegmentReplicationSource;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.SegmentReplicator;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfoTests;
import org.opensearch.snapshots.SnapshotShardsService;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Assert;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.opensearch.index.engine.EngineTestCase.assertAtMostOneLuceneDocumentPerSequenceNumber;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentReplicationIndexShardTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    protected ReplicationGroup getReplicationGroup(int numberOfReplicas) throws IOException {
        return createGroup(numberOfReplicas, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory());
    }

    protected ReplicationGroup getReplicationGroup(int numberOfReplicas, String indexMapping) throws IOException {
        return createGroup(numberOfReplicas, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory());
    }

    protected Settings getIndexSettings() {
        return settings;
    }

    /**
     * Validates happy path of segment replication where primary index docs which are replicated to replica shards. Assertions
     * made on doc count on both primary and replica.
     */
    public void testReplication() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory());) {
            shards.startAll();
            final IndexShard primaryShard = shards.getPrimary();
            final IndexShard replicaShard = shards.getReplicas().get(0);

            // index and replicate segments to replica.
            int numDocs = randomIntBetween(10, 20);
            shards.indexDocs(numDocs);
            primaryShard.refresh("test");
            flushShard(primaryShard);
            replicateSegments(primaryShard, List.of(replicaShard));

            // Assertions
            shards.assertAllEqual(numDocs);
        }
    }

    /**
     * Test that latestReplicationCheckpoint returns null only for docrep enabled indices
     */
    public void testReplicationCheckpointNullForDocRep() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, "DOCUMENT").put(Settings.EMPTY).build();
        final IndexShard indexShard = newStartedShard(false, indexSettings);
        assertNull(indexShard.getLatestReplicationCheckpoint());
        closeShards(indexShard);
    }

    /**
     * Test that latestReplicationCheckpoint returns ReplicationCheckpoint for segrep enabled indices
     */
    public void testReplicationCheckpointNotNullForSegRep() throws IOException {
        final IndexShard indexShard = newStartedShard(randomBoolean(), getIndexSettings(), new NRTReplicationEngineFactory());
        final ReplicationCheckpoint replicationCheckpoint = indexShard.getLatestReplicationCheckpoint();
        assertNotNull(replicationCheckpoint);
        closeShards(indexShard);
    }

    public void testNRTReplicasDoNotAcceptRefreshListeners() throws IOException {
        final IndexShard indexShard = newStartedShard(false, settings, new NRTReplicationEngineFactory());
        indexShard.addRefreshListener(mock(Translog.Location.class), Assert::assertFalse);
        closeShards(indexShard);
    }

    public void testSegmentInfosAndReplicationCheckpointTuple() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), indexMapping, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            // assert before any indexing:
            // replica:
            Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> replicaTuple = replica.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = replicaTuple.v1()) {
                assertReplicationCheckpoint(replica, gatedCloseable.get(), replicaTuple.v2());
            }

            // primary:
            Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> primaryTuple = primary.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = primaryTuple.v1()) {
                assertReplicationCheckpoint(primary, gatedCloseable.get(), primaryTuple.v2());
            }
            // We use compareTo here instead of equals because we ignore segments gen with replicas performing their own commits.
            // However infos version we expect to be equal.
            assertEquals(1, primary.getLatestReplicationCheckpoint().compareTo(replica.getLatestReplicationCheckpoint()));

            // index and copy segments to replica.
            int numDocs = randomIntBetween(10, 20);
            shards.indexDocs(numDocs);
            primary.refresh("test");
            replicateSegments(primary, List.of(replica));

            replicaTuple = replica.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = replicaTuple.v1()) {
                assertReplicationCheckpoint(replica, gatedCloseable.get(), replicaTuple.v2());
            }

            primaryTuple = primary.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = primaryTuple.v1()) {
                assertReplicationCheckpoint(primary, gatedCloseable.get(), primaryTuple.v2());
            }

            replicaTuple = replica.getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> gatedCloseable = replicaTuple.v1()) {
                assertReplicationCheckpoint(replica, gatedCloseable.get(), replicaTuple.v2());
            }
            assertEquals(1, primary.getLatestReplicationCheckpoint().compareTo(replica.getLatestReplicationCheckpoint()));
        }
    }

    public void testPrimaryRelocationWithSegRepFailure() throws Exception {
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
                throw new IOException("Expected failure");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        Exception e = expectThrows(
            Exception.class,
            () -> recoverReplica(
                primaryTarget,
                primarySource,
                (primary, sourceNode) -> new RecoveryTarget(primary, sourceNode, new ReplicationListener() {
                    @Override
                    public void onDone(ReplicationState state) {
                        throw new AssertionError("recovery must fail");
                    }

                    @Override
                    public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                        assertEquals(ExceptionsHelper.unwrap(e, IOException.class).getMessage(), "Expected failure");
                    }
                }, threadPool),
                true,
                true,
                replicatePrimaryFunction
            )
        );
        closeShards(primarySource, primaryTarget);
    }

    private void assertReplicationCheckpoint(IndexShard shard, SegmentInfos segmentInfos, ReplicationCheckpoint checkpoint)
        throws IOException {
        assertNotNull(segmentInfos);
        assertEquals(checkpoint.getSegmentInfosVersion(), segmentInfos.getVersion());
        assertEquals(checkpoint.getSegmentsGen(), segmentInfos.getGeneration());
    }

    public void testIsSegmentReplicationAllowed_WrongEngineType() throws IOException {
        final IndexShard indexShard = newShard(false, getIndexSettings(), new InternalEngineFactory());
        assertFalse(indexShard.isSegmentReplicationAllowed());
        closeShards(indexShard);
    }

    /**
     * This test mimics the segment replication failure due to CorruptIndexException exception which happens when
     * reader close operation on replica shard deletes the segment files copied in current round of segment replication.
     * It does this by blocking the finalizeReplication on replica shard and performing close operation on acquired
     * searcher that triggers the reader close operation.
     */
    public void testSegmentReplication_With_ReaderClosedConcurrently() throws Exception {
        String mappings = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": { \"properties\": { \"foo\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), mappings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primaryShard = shards.getPrimary();
            final IndexShard replicaShard = shards.getReplicas().get(0);

            // Step 1. Ingest numDocs documents & replicate to replica shard
            final int numDocs = randomIntBetween(10, 20);
            logger.info("--> Inserting documents {}", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.refresh("Test");
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            replicateSegments(primaryShard, shards.getReplicas());

            IndexShard spyShard = spy(replicaShard);
            Engine.Searcher test = replicaShard.getEngine().acquireSearcher("testSegmentReplication_With_ReaderClosedConcurrently");
            shards.assertAllEqual(numDocs);

            // Step 2. Ingest numDocs documents again & replicate to replica shard
            logger.info("--> Ingest {} docs again", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            replicateSegments(primaryShard, shards.getReplicas());

            // Step 3. Perform force merge down to 1 segment on primary
            primaryShard.forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(true));
            logger.info("--> primary store after force merge {}", Arrays.toString(primaryShard.store().directory().listAll()));
            // Perform close on searcher before IndexShard::finalizeReplication
            doAnswer(n -> {
                test.close();
                n.callRealMethod();
                return null;
            }).when(spyShard).finalizeReplication(any());
            replicateSegments(primaryShard, List.of(spyShard));
            shards.assertAllEqual(numDocs);
        }
    }

    /**
     * Similar to test above, this test shows the issue where an engine close operation during active segment replication
     * can result in Lucene CorruptIndexException.
     */
    public void testSegmentReplication_With_EngineClosedConcurrently() throws Exception {
        String mappings = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": { \"properties\": { \"foo\": { \"type\": \"keyword\"} }}}";
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), mappings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primaryShard = shards.getPrimary();
            final IndexShard replicaShard = shards.getReplicas().get(0);

            // Step 1. Ingest numDocs documents
            final int numDocs = randomIntBetween(10, 20);
            logger.info("--> Inserting documents {}", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.refresh("Test");
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(numDocs);

            // Step 2. Ingest numDocs documents again to create a new commit
            logger.info("--> Ingest {} docs again", numDocs);
            for (int i = 0; i < numDocs; i++) {
                shards.index(new IndexRequest(index.getName()).id(String.valueOf(i)).source("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON));
            }
            assertEqualTranslogOperations(shards, primaryShard);
            primaryShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
            logger.info("--> primary store after final flush {}", Arrays.toString(primaryShard.store().directory().listAll()));

            // Step 3. Before replicating segments, block finalizeReplication and perform engine commit directly that
            // cleans up recently copied over files
            IndexShard spyShard = spy(replicaShard);
            doAnswer(n -> {
                NRTReplicationEngine engine = (NRTReplicationEngine) replicaShard.getEngine();
                // Using engine.close() prevents indexShard.finalizeReplication execution due to engine AlreadyClosedException,
                // thus as workaround, use updateSegments which eventually calls commitSegmentInfos on latest segment infos.
                engine.updateSegments(engine.getSegmentInfosSnapshot().get());
                n.callRealMethod();
                return null;
            }).when(spyShard).finalizeReplication(any());
            replicateSegments(primaryShard, List.of(spyShard));
            shards.assertAllEqual(numDocs);
        }
    }

    public void testIgnoreShardIdle() throws Exception {
        Settings updatedSettings = Settings.builder()
            .put(getIndexSettings())
            .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO)
            .build();
        try (ReplicationGroup shards = createGroup(1, updatedSettings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final int numDocs = shards.indexDocs(randomIntBetween(1, 10));
            // ensure search idle conditions are met.
            assertTrue(primary.isSearchIdle());
            assertTrue(replica.isSearchIdle());

            // invoke scheduledRefresh, returns true if refresh is immediately invoked.
            assertTrue(primary.scheduledRefresh());
            // replica would always return false here as there is no indexed doc to refresh on.
            assertFalse(replica.scheduledRefresh());

            // assert there is no pending refresh
            assertFalse(primary.hasRefreshPending());
            assertFalse(replica.hasRefreshPending());
            shards.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            shards.assertAllEqual(numDocs);
        }
    }

    public void testShardIdle_Docrep() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .build();
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final int numDocs = shards.indexDocs(randomIntBetween(1, 10));
            // ensure search idle conditions are met.
            assertTrue(primary.isSearchIdle());
            assertTrue(replica.isSearchIdle());
            assertFalse(primary.scheduledRefresh());
            assertFalse(replica.scheduledRefresh());
            assertTrue(primary.hasRefreshPending());
            assertTrue(replica.hasRefreshPending());
            shards.refresh("test");
            shards.assertAllEqual(numDocs);
        }
    }

    public void testShardIdleWithNoReplicas() throws Exception {
        Settings updatedSettings = Settings.builder()
            .put(getIndexSettings())
            .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO)
            .build();
        try (ReplicationGroup shards = createGroup(0, updatedSettings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            shards.indexDocs(randomIntBetween(1, 10));
            validateShardIdleWithNoReplicas(primary);
        }
    }

    protected void validateShardIdleWithNoReplicas(IndexShard primary) {
        // ensure search idle conditions are met.
        assertTrue(primary.isSearchIdle());
        assertFalse(primary.scheduledRefresh());
        assertTrue(primary.hasRefreshPending());
    }

    /**
     * here we are starting a new primary shard in PrimaryMode and testing if the shard publishes checkpoint after refresh.
     */
    public void testPublishCheckpointOnPrimaryMode() throws IOException, InterruptedException {
        final SegmentReplicationCheckpointPublisher mock = mock(SegmentReplicationCheckpointPublisher.class);
        IndexShard shard = newStartedShard(p -> newShard(false, mock, settings), false);

        final ShardRouting shardRouting = shard.routingEntry();
        promoteReplica(
            shard,
            Collections.singleton(shardRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(shardRouting.shardId()).addShard(shardRouting).build()
        );

        final CountDownLatch latch = new CountDownLatch(1);
        shard.acquirePrimaryOperationPermit(new ActionListener<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                releasable.close();
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        }, ThreadPool.Names.GENERIC, "");

        latch.await();
        // verify checkpoint is published
        verify(mock, times(1)).publish(any(), any());
        closeShards(shard);
    }

    public void testPublishCheckpointOnPrimaryMode_segrep_off() throws IOException, InterruptedException {
        final SegmentReplicationCheckpointPublisher mock = mock(SegmentReplicationCheckpointPublisher.class);
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build();
        IndexShard shard = newStartedShard(p -> newShard(false, mock, settings), false);

        final ShardRouting shardRouting = shard.routingEntry();
        promoteReplica(
            shard,
            Collections.singleton(shardRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(shardRouting.shardId()).addShard(shardRouting).build()
        );

        final CountDownLatch latch = new CountDownLatch(1);
        shard.acquirePrimaryOperationPermit(new ActionListener<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                releasable.close();
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        }, ThreadPool.Names.GENERIC, "");

        latch.await();
        // verify checkpoint is published
        verify(mock, times(0)).publish(any(), any());
        closeShards(shard);
    }

    public void testPublishCheckpointPostFailover() throws IOException {
        final SegmentReplicationCheckpointPublisher mock = mock(SegmentReplicationCheckpointPublisher.class);
        IndexShard shard = newStartedShard(true);
        CheckpointRefreshListener refreshListener = new CheckpointRefreshListener(shard, mock);
        refreshListener.afterRefresh(true);

        // verify checkpoint is published
        verify(mock, times(1)).publish(any(), any());
        closeShards(shard);
    }

    /**
     * here we are starting a new primary shard in PrimaryMode initially and starting relocation handoff. Later we complete relocation handoff then shard is no longer
     * in PrimaryMode, and we test if the shard does not publish checkpoint after refresh.
     */
    public void testPublishCheckpointAfterRelocationHandOff() throws IOException {
        final SegmentReplicationCheckpointPublisher mock = mock(SegmentReplicationCheckpointPublisher.class);
        IndexShard shard = newStartedShard(true);
        CheckpointRefreshListener refreshListener = new CheckpointRefreshListener(shard, mock);
        String id = shard.routingEntry().allocationId().getId();

        // Starting relocation handoff
        shard.getReplicationTracker().startRelocationHandoff(id);

        // Completing relocation handoff
        shard.getReplicationTracker().completeRelocationHandoff();
        refreshListener.afterRefresh(true);

        // verify checkpoint is not published
        verify(mock, times(0)).publish(any(), any());
        closeShards(shard);
    }

    /**
     * here we are starting a new primary shard and testing that we don't process a checkpoint on a shard when it's shard routing is primary.
     */
    public void testRejectCheckpointOnShardRoutingPrimary() throws IOException {
        IndexShard primaryShard = newStartedShard(true);
        SegmentReplicationTargetService sut;
        sut = prepareForReplication(primaryShard, null);
        SegmentReplicationTargetService spy = spy(sut);

        // Starting a new shard in PrimaryMode and shard routing primary.
        IndexShard spyShard = spy(primaryShard);
        String id = primaryShard.routingEntry().allocationId().getId();

        // Starting relocation handoff
        primaryShard.getReplicationTracker().startRelocationHandoff(id);

        // Completing relocation handoff.
        primaryShard.getReplicationTracker().completeRelocationHandoff();

        // Assert that primary shard is no longer in Primary Mode and shard routing is still Primary
        assertEquals(false, primaryShard.getReplicationTracker().isPrimaryMode());
        assertEquals(true, primaryShard.routingEntry().primary());

        spy.onNewCheckpoint(new ReplicationCheckpoint(primaryShard.shardId(), 0L, 0L, 0L, Codec.getDefault().getName()), spyShard);

        // Verify that checkpoint is not processed as shard routing is primary.
        verify(spy, times(0)).startReplication(any(), any(), any());
        closeShards(primaryShard);
    }

    // Todo: Remove this test when there is a better mechanism to test a functionality passing in different replication
    // strategy.
    public void testLockingBeforeAndAfterRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true, getIndexSettings());
        final ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        CountDownLatch latch = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            latch.countDown();
            try {
                shard.relocated(routing.getTargetRelocatingShard().allocationId().getId(), primaryContext -> {}, () -> {});
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try (Releasable ignored = acquirePrimaryOperationPermitBlockingly(shard)) {
            // start finalization of recovery
            recoveryThread.start();
            latch.await();
            // recovery can only be finalized after we release the current primaryOperationLock
            assertFalse(shard.isRelocatedPrimary());
        }
        // recovery can be now finalized
        recoveryThread.join();
        assertTrue(shard.isRelocatedPrimary());
        final ExecutionException e = expectThrows(ExecutionException.class, () -> acquirePrimaryOperationPermitBlockingly(shard));
        assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
        assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));

        closeShards(shard);
    }

    // Todo: Remove this test when there is a better mechanism to test a functionality passing in different replication
    // strategy.
    public void testDelayedOperationsBeforeAndAfterRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true, getIndexSettings());
        final ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        final CountDownLatch startRecovery = new CountDownLatch(1);
        final CountDownLatch relocationStarted = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            try {
                startRecovery.await();
                shard.relocated(
                    routing.getTargetRelocatingShard().allocationId().getId(),
                    primaryContext -> relocationStarted.countDown(),
                    () -> {}
                );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        recoveryThread.start();

        final int numberOfAcquisitions = randomIntBetween(1, 10);
        final List<Runnable> assertions = new ArrayList<>(numberOfAcquisitions);
        final int recoveryIndex = randomIntBetween(0, numberOfAcquisitions - 1);

        for (int i = 0; i < numberOfAcquisitions; i++) {
            final PlainActionFuture<Releasable> onLockAcquired;
            if (i < recoveryIndex) {
                final AtomicBoolean invoked = new AtomicBoolean();
                onLockAcquired = new PlainActionFuture<Releasable>() {

                    @Override
                    public void onResponse(Releasable releasable) {
                        invoked.set(true);
                        releasable.close();
                        super.onResponse(releasable);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError();
                    }

                };
                assertions.add(() -> assertTrue(invoked.get()));
            } else if (recoveryIndex == i) {
                startRecovery.countDown();
                relocationStarted.await();
                onLockAcquired = new PlainActionFuture<>();
                assertions.add(() -> {
                    final ExecutionException e = expectThrows(ExecutionException.class, () -> onLockAcquired.get(30, TimeUnit.SECONDS));
                    assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
                    assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));
                });
            } else {
                onLockAcquired = new PlainActionFuture<>();
                assertions.add(() -> {
                    final ExecutionException e = expectThrows(ExecutionException.class, () -> onLockAcquired.get(30, TimeUnit.SECONDS));
                    assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
                    assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));
                });
            }

            shard.acquirePrimaryOperationPermit(onLockAcquired, ThreadPool.Names.WRITE, "i_" + i);
        }

        for (final Runnable assertion : assertions) {
            assertion.run();
        }

        recoveryThread.join();

        closeShards(shard);
    }

    public void testCloseShardDuringFinalize() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);
            final IndexShard replicaSpy = spy(replica);

            primary.refresh("Test");

            doThrow(AlreadyClosedException.class).when(replicaSpy).finalizeReplication(any());

            replicateSegments(primary, List.of(replicaSpy));
        }
    }

    public void testBeforeIndexShardClosedWhileCopyingFiles() throws Exception {
        try (ReplicationGroup shards = createGroup(1, getIndexSettings(), new NRTReplicationEngineFactory())) {
            shards.startAll();
            IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            primary.refresh("Test");

            final SegmentReplicationSourceFactory sourceFactory = mock(SegmentReplicationSourceFactory.class);
            final SegmentReplicationTargetService targetService = newTargetService(sourceFactory);
            SegmentReplicationSource source = new TestReplicationSource() {

                ActionListener<GetSegmentFilesResponse> listener;

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
                    BiConsumer<String, Long> fileProgressTracker,
                    ActionListener<GetSegmentFilesResponse> listener
                ) {
                    // set the listener, we will only fail it once we cancel the source.
                    this.listener = listener;
                    // shard is closing while we are copying files.
                    targetService.beforeIndexShardClosed(replica.shardId, replica, Settings.EMPTY);
                }

                @Override
                public void cancel() {
                    // simulate listener resolving, but only after we have issued a cancel from beforeIndexShardClosed .
                    final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("retryable action was cancelled");
                    listener.onFailure(exception);
                }
            };
            when(sourceFactory.get(any())).thenReturn(source);
            startReplicationAndAssertCancellation(replica, primary, targetService);

            shards.removeReplica(replica);
            closeShards(replica);
        }
    }

    protected SegmentReplicationTargetService newTargetService(SegmentReplicationSourceFactory sourceFactory) {
        return new SegmentReplicationTargetService(
            threadPool,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(TransportService.class),
            sourceFactory,
            null,
            null,
            new SegmentReplicator(threadPool)
        );
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

    public void testQueryDuringEngineResetShowsDocs() throws Exception {
        final NRTReplicationEngineFactory engineFactory = new NRTReplicationEngineFactory();
        final NRTReplicationEngineFactory spy = spy(engineFactory);
        try (ReplicationGroup shards = createGroup(1, settings, indexMapping, spy, createTempDir())) {
            final IndexShard primaryShard = shards.getPrimary();
            final IndexShard replicaShard = shards.getReplicas().get(0);
            shards.startAll();
            shards.indexDocs(10);
            shards.refresh("test");
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(10);

            final AtomicReference<Throwable> failed = new AtomicReference<>();
            doAnswer(ans -> {
                try {
                    final Engine engineOrNull = replicaShard.getEngineOrNull();
                    assertNotNull(engineOrNull);
                    assertTrue(engineOrNull instanceof ReadOnlyEngine);
                    shards.assertAllEqual(10);
                } catch (Throwable e) {
                    failed.set(e);
                }
                return ans.callRealMethod();
            }).when(spy).newReadWriteEngine(any());
            shards.promoteReplicaToPrimary(replicaShard).get();
            assertNull("Expected correct doc count during engine reset", failed.get());
        }
    }

    public void testSegmentReplicationStats() throws Exception {
        final NRTReplicationEngineFactory engineFactory = new NRTReplicationEngineFactory();
        final NRTReplicationEngineFactory spy = spy(engineFactory);
        try (ReplicationGroup shards = createGroup(1, settings, indexMapping, spy, createTempDir())) {
            final IndexShard primaryShard = shards.getPrimary();
            final IndexShard replicaShard = shards.getReplicas().get(0);
            shards.startAll();

            assertReplicaCaughtUp(primaryShard);

            shards.indexDocs(10);
            shards.refresh("test");

            final ReplicationCheckpoint primaryCheckpoint = primaryShard.getLatestReplicationCheckpoint();
            final long initialCheckpointSize = primaryCheckpoint.getMetadataMap()
                .values()
                .stream()
                .mapToLong(StoreFileMetadata::length)
                .sum();

            Set<SegmentReplicationShardStats> postRefreshStats = primaryShard.getReplicationStatsForTrackedReplicas();
            SegmentReplicationShardStats shardStats = postRefreshStats.stream().findFirst().get();
            assertEquals(1, shardStats.getCheckpointsBehindCount());
            assertEquals(initialCheckpointSize, shardStats.getBytesBehindCount());
            replicateSegments(primaryShard, shards.getReplicas());
            assertReplicaCaughtUp(primaryShard);
            shards.assertAllEqual(10);

            final List<DocIdSeqNoAndSource> docIdAndSeqNos = getDocIdAndSeqNos(primaryShard);
            for (DocIdSeqNoAndSource docIdAndSeqNo : docIdAndSeqNos.subList(0, 5)) {
                deleteDoc(primaryShard, docIdAndSeqNo.getId());
                // delete on replica for xlog.
                deleteDoc(replicaShard, docIdAndSeqNo.getId());
            }
            primaryShard.forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(true));

            final Map<String, StoreFileMetadata> segmentMetadataMap = primaryShard.getSegmentMetadataMap();
            final Store.RecoveryDiff diff = Store.segmentReplicationDiff(segmentMetadataMap, replicaShard.getSegmentMetadataMap());
            final long sizeAfterDeleteAndCommit = diff.missing.stream().mapToLong(StoreFileMetadata::length).sum();

            final Set<SegmentReplicationShardStats> statsAfterFlush = primaryShard.getReplicationStatsForTrackedReplicas();
            shardStats = statsAfterFlush.stream().findFirst().get();
            assertEquals(sizeAfterDeleteAndCommit, shardStats.getBytesBehindCount());
            assertEquals(1, shardStats.getCheckpointsBehindCount());

            replicateSegments(primaryShard, shards.getReplicas());
            assertReplicaCaughtUp(primaryShard);
            shards.assertAllEqual(5);
        }
    }

    public void testSnapshotWhileFailoverIncomplete() throws Exception {
        final NRTReplicationEngineFactory engineFactory = new NRTReplicationEngineFactory();
        final NRTReplicationEngineFactory spy = spy(engineFactory);
        try (ReplicationGroup shards = createGroup(1, settings, indexMapping, spy, createTempDir())) {
            final IndexShard primaryShard = shards.getPrimary();
            final IndexShard replicaShard = shards.getReplicas().get(0);
            shards.startAll();
            shards.indexDocs(10);
            shards.refresh("test");
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(10);

            final SnapshotShardsService shardsService = getSnapshotShardsService(replicaShard);
            final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));

            final ClusterState initState = addSnapshotIndex(clusterService.state(), snapshot, replicaShard, SnapshotsInProgress.State.INIT);
            shardsService.clusterChanged(new ClusterChangedEvent("test", initState, clusterService.state()));

            CountDownLatch latch = new CountDownLatch(1);
            doAnswer(ans -> {
                final Engine engineOrNull = replicaShard.getEngineOrNull();
                assertNotNull(engineOrNull);
                assertTrue(engineOrNull instanceof ReadOnlyEngine);
                shards.assertAllEqual(10);
                shardsService.clusterChanged(
                    new ClusterChangedEvent(
                        "test",
                        addSnapshotIndex(clusterService.state(), snapshot, replicaShard, SnapshotsInProgress.State.STARTED),
                        initState
                    )
                );
                latch.countDown();
                return ans.callRealMethod();
            }).when(spy).newReadWriteEngine(any());
            shards.promoteReplicaToPrimary(replicaShard).get();
            latch.await();
            assertBusy(() -> {
                final IndexShardSnapshotStatus.Copy copy = shardsService.currentSnapshotShards(snapshot).get(replicaShard.shardId).asCopy();
                final IndexShardSnapshotStatus.Stage stage = copy.getStage();
                assertEquals(IndexShardSnapshotStatus.Stage.FAILURE, stage);
                assertNotNull(copy.getFailure());
                assertTrue(
                    copy.getFailure()
                        .contains("snapshot triggered on a new primary following failover and cannot proceed until promotion is complete")
                );
            });
        }
    }

    public void testReuseReplicationCheckpointWhenLatestInfosIsUnChanged() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, indexMapping, new NRTReplicationEngineFactory(), createTempDir())) {
            final IndexShard primaryShard = shards.getPrimary();
            shards.startAll();
            shards.indexDocs(10);
            shards.refresh("test");
            replicateSegments(primaryShard, shards.getReplicas());
            shards.assertAllEqual(10);
            final ReplicationCheckpoint latestReplicationCheckpoint = primaryShard.getLatestReplicationCheckpoint();
            try (GatedCloseable<SegmentInfos> segmentInfosSnapshot = primaryShard.getSegmentInfosSnapshot()) {
                assertEquals(latestReplicationCheckpoint, primaryShard.computeReplicationCheckpoint(segmentInfosSnapshot.get()));
            }
            final Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> latestSegmentInfosAndCheckpoint = primaryShard
                .getLatestSegmentInfosAndCheckpoint();
            try (final GatedCloseable<SegmentInfos> closeable = latestSegmentInfosAndCheckpoint.v1()) {
                assertEquals(latestReplicationCheckpoint, primaryShard.computeReplicationCheckpoint(closeable.get()));
            }
        }
    }

    public void testComputeReplicationCheckpointNullInfosReturnsEmptyCheckpoint() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, indexMapping, new NRTReplicationEngineFactory(), createTempDir())) {
            final IndexShard primaryShard = shards.getPrimary();
            assertEquals(ReplicationCheckpoint.empty(primaryShard.shardId), primaryShard.computeReplicationCheckpoint(null));
        }
    }

    private SnapshotShardsService getSnapshotShardsService(IndexShard replicaShard) {
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        final IndicesService indicesService = mock(IndicesService.class);
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShardOrNull(anyInt())).thenReturn(replicaShard);
        return new SnapshotShardsService(settings, clusterService, createRepositoriesService(), transportService, indicesService);
    }

    private ClusterState addSnapshotIndex(
        ClusterState state,
        Snapshot snapshot,
        IndexShard shard,
        SnapshotsInProgress.State snapshotState
    ) {
        final Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = new HashMap<>();
        ShardRouting shardRouting = shard.shardRouting;
        shardsBuilder.put(
            shardRouting.shardId(),
            new SnapshotsInProgress.ShardSnapshotStatus(state.getNodes().getLocalNode().getId(), "1")
        );
        final SnapshotsInProgress.Entry entry = new SnapshotsInProgress.Entry(
            snapshot,
            randomBoolean(),
            false,
            snapshotState,
            Collections.singletonList(new IndexId(index.getName(), index.getUUID())),
            Collections.emptyList(),
            randomNonNegativeLong(),
            randomLong(),
            shardsBuilder,
            null,
            SnapshotInfoTests.randomUserMetadata(),
            VersionUtils.randomVersion(random()),
            false
        );
        return ClusterState.builder(state)
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(Collections.singletonList(entry)))
            .build();
    }

    private void assertReplicaCaughtUp(IndexShard primaryShard) {
        Set<SegmentReplicationShardStats> initialStats = primaryShard.getReplicationStatsForTrackedReplicas();
        assertEquals(initialStats.size(), 1);
        SegmentReplicationShardStats shardStats = initialStats.stream().findFirst().get();
        assertEquals(0, shardStats.getCheckpointsBehindCount());
        assertEquals(0, shardStats.getBytesBehindCount());
    }

    /**
     * Assert persisted and searchable doc counts.  This method should not be used while docs are concurrently indexed because
     * it asserts point in time seqNos are relative to the doc counts.
     */
    protected void assertDocCounts(IndexShard indexShard, int expectedPersistedDocCount, int expectedSearchableDocCount)
        throws IOException {
        assertDocCount(indexShard, expectedSearchableDocCount);
        // assigned seqNos start at 0, so assert max & local seqNos are 1 less than our persisted doc count.
        assertEquals(expectedPersistedDocCount - 1, indexShard.seqNoStats().getMaxSeqNo());
        assertEquals(expectedPersistedDocCount - 1, indexShard.seqNoStats().getLocalCheckpoint());
    }

    protected void resolveCheckpointInfoResponseListener(ActionListener<CheckpointInfoResponse> listener, IndexShard primary) {
        try (final CopyState copyState = new CopyState(primary)) {
            listener.onResponse(
                new CheckpointInfoResponse(copyState.getCheckpoint(), copyState.getMetadataMap(), copyState.getInfosBytes())
            );
        } catch (IOException e) {
            logger.error("Unexpected error computing CopyState", e);
            Assert.fail("Failed to compute copyState");
            throw new UncheckedIOException(e);
        }
    }

    protected void startReplicationAndAssertCancellation(
        IndexShard replica,
        IndexShard primary,
        SegmentReplicationTargetService targetService
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        final SegmentReplicationTarget target = targetService.startReplication(
            replica,
            primary.getLatestReplicationCheckpoint(),
            new SegmentReplicationTargetService.SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {
                    Assert.fail("Replication should not complete");
                }

                @Override
                public void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                    assertFalse(sendShardFailure);
                    latch.countDown();
                }
            }
        );

        latch.await(2, TimeUnit.SECONDS);
        assertEquals("Should have resolved listener with failure", 0, latch.getCount());
        assertNull(targetService.get(target.getId()));
    }

    protected IndexShard getRandomReplica(ReplicationGroup shards) {
        return shards.getReplicas().get(randomInt(shards.getReplicas().size() - 1));
    }

    protected IndexShard failAndPromoteRandomReplica(ReplicationGroup shards) throws IOException {
        IndexShard primary = shards.getPrimary();
        final IndexShard newPrimary = getRandomReplica(shards);
        shards.promoteReplicaToPrimary(newPrimary);
        primary.close("demoted", true, false);
        primary.store().close();
        primary = shards.addReplicaWithExistingPath(primary.shardPath(), primary.routingEntry().currentNodeId());
        shards.recoverReplica(primary);
        return newPrimary;
    }

    protected void assertEqualCommittedSegments(IndexShard primary, IndexShard... replicas) throws IOException {
        for (IndexShard replica : replicas) {
            final SegmentInfos replicaInfos = replica.store().readLastCommittedSegmentsInfo();
            final SegmentInfos primaryInfos = primary.store().readLastCommittedSegmentsInfo();
            final Map<String, StoreFileMetadata> latestReplicaMetadata = replica.store().getSegmentMetadataMap(replicaInfos);
            final Map<String, StoreFileMetadata> latestPrimaryMetadata = primary.store().getSegmentMetadataMap(primaryInfos);
            final Store.RecoveryDiff diff = Store.segmentReplicationDiff(latestPrimaryMetadata, latestReplicaMetadata);
            assertTrue(diff.different.isEmpty());
            assertTrue(diff.missing.isEmpty());
        }
    }

    protected void assertEqualTranslogOperations(ReplicationGroup shards, IndexShard primaryShard) throws IOException {
        try (final Translog.Snapshot snapshot = getTranslog(primaryShard).newSnapshot()) {
            List<Translog.Operation> operations = new ArrayList<>();
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                final Translog.Operation newOp = op;
                operations.add(newOp);
            }
            for (IndexShard replica : shards.getReplicas()) {
                try (final Translog.Snapshot replicaSnapshot = getTranslog(replica).newSnapshot()) {
                    assertThat(replicaSnapshot, SnapshotMatchers.containsOperationsInAnyOrder(operations));
                }
            }
        }
    }
}
