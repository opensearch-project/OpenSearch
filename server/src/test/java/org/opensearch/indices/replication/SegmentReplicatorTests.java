/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.TestReplicationSource;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SegmentReplicatorTests extends IndexShardTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    public void testReplicationWithUnassignedPrimary() throws Exception {
        final IndexShard replica = newStartedShard(false, settings, new NRTReplicationEngineFactory());
        final IndexShard primary = newStartedShard(true, settings, new NRTReplicationEngineFactory());
        SegmentReplicator replicator = new SegmentReplicator(threadPool);

        ClusterService cs = mock(ClusterService.class);
        IndexShardRoutingTable.Builder shardRoutingTable = new IndexShardRoutingTable.Builder(replica.shardId());
        shardRoutingTable.addShard(replica.routingEntry());
        shardRoutingTable.addShard(primary.routingEntry().moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, "test")));

        when(cs.state()).thenReturn(buildClusterState(replica, shardRoutingTable));
        replicator.setSourceFactory(new SegmentReplicationSourceFactory(mock(TransportService.class), mock(RecoverySettings.class), cs));
        expectThrows(IllegalStateException.class, () -> replicator.startReplication(replica));
        closeShards(replica, primary);
    }

    public void testReplicationWithUnknownPrimaryNode() throws Exception {
        final IndexShard replica = newStartedShard(false, settings, new NRTReplicationEngineFactory());
        final IndexShard primary = newStartedShard(true, settings, new NRTReplicationEngineFactory());
        SegmentReplicator replicator = new SegmentReplicator(threadPool);

        ClusterService cs = mock(ClusterService.class);
        IndexShardRoutingTable.Builder shardRoutingTable = new IndexShardRoutingTable.Builder(replica.shardId());
        shardRoutingTable.addShard(replica.routingEntry());
        shardRoutingTable.addShard(primary.routingEntry());

        when(cs.state()).thenReturn(buildClusterState(replica, shardRoutingTable));
        replicator.setSourceFactory(new SegmentReplicationSourceFactory(mock(TransportService.class), mock(RecoverySettings.class), cs));
        expectThrows(IllegalStateException.class, () -> replicator.startReplication(replica));
        closeShards(replica, primary);
    }

    private ClusterState buildClusterState(IndexShard replica, IndexShardRoutingTable.Builder indexShard) {
        return ClusterState.builder(clusterService.state())
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(replica.shardId().getIndex()).addIndexShard(indexShard.build()).build())
                    .build()
            )
            .build();
    }

    public void testStartReplicationWithoutSourceFactory() {
        ThreadPool threadpool = mock(ThreadPool.class);
        ExecutorService mock = mock(ExecutorService.class);
        when(threadpool.generic()).thenReturn(mock);
        SegmentReplicator segmentReplicator = new SegmentReplicator(threadpool);

        IndexShard shard = mock(IndexShard.class);
        segmentReplicator.startReplication(shard);
        Mockito.verifyNoInteractions(mock);
    }

    public void testStartReplicationRunsSuccessfully() throws Exception {
        final IndexShard replica = newStartedShard(false, settings, new NRTReplicationEngineFactory());
        final IndexShard primary = newStartedShard(true, settings, new NRTReplicationEngineFactory());

        // index and copy segments to replica.
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(primary, "_doc", Integer.toString(i));
        }
        primary.refresh("test");

        SegmentReplicator segmentReplicator = spy(new SegmentReplicator(threadPool));
        SegmentReplicationSourceFactory factory = mock(SegmentReplicationSourceFactory.class);
        when(factory.get(replica)).thenReturn(new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                resolveCheckpointListener(listener, primary);
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
                try {
                    Lucene.cleanLuceneIndex(indexShard.store().directory());
                    Map<String, StoreFileMetadata> segmentMetadataMap = primary.getSegmentMetadataMap();
                    for (String file : segmentMetadataMap.keySet()) {
                        indexShard.store().directory().copyFrom(primary.store().directory(), file, file, IOContext.DEFAULT);
                    }
                    listener.onResponse(new GetSegmentFilesResponse(new ArrayList<>(segmentMetadataMap.values())));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        segmentReplicator.setSourceFactory(factory);
        segmentReplicator.startReplication(replica);
        assertBusy(() -> assertDocCount(replica, numDocs));
        closeShards(primary, replica);
    }

    public void testReplicationFails() throws Exception {
        allowShardFailures();
        final IndexShard replica = newStartedShard(false, settings, new NRTReplicationEngineFactory());
        final IndexShard primary = newStartedShard(true, settings, new NRTReplicationEngineFactory());

        SegmentReplicator segmentReplicator = spy(new SegmentReplicator(threadPool));
        SegmentReplicationSourceFactory factory = mock(SegmentReplicationSourceFactory.class);
        when(factory.get(replica)).thenReturn(new TestReplicationSource() {
            @Override
            public void getCheckpointMetadata(
                long replicationId,
                ReplicationCheckpoint checkpoint,
                ActionListener<CheckpointInfoResponse> listener
            ) {
                resolveCheckpointListener(listener, primary);
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
                OpenSearchCorruptionException corruptIndexException = new OpenSearchCorruptionException("test");
                try {
                    indexShard.store().markStoreCorrupted(corruptIndexException);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                listener.onFailure(corruptIndexException);
            }
        });
        // assert shard failure on corruption
        AtomicBoolean failureCallbackTriggered = new AtomicBoolean(false);
        replica.addShardFailureCallback((ig) -> failureCallbackTriggered.set(true));
        segmentReplicator.setSourceFactory(factory);
        segmentReplicator.startReplication(replica);
        assertBusy(() -> assertTrue(failureCallbackTriggered.get()));
        closeShards(primary, replica);
    }

    public void testGetSegmentReplicationStats_WhenNoReplication() {
        SegmentReplicator segmentReplicator = new SegmentReplicator(threadPool);
        ShardId shardId = new ShardId("index", "uuid", 0);
        ReplicationStats replicationStats = segmentReplicator.getSegmentReplicationStats(shardId);
        assertEquals(0, replicationStats.maxReplicationLag);
        assertEquals(0, replicationStats.totalBytesBehind);
        assertEquals(0, replicationStats.maxBytesBehind);
    }

    public void testGetSegmentReplicationStats_WhileOnGoingReplicationAndPrimaryRefreshedToNewCheckPoint() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        ReplicationCheckpoint firstReplicationCheckpoint = ReplicationCheckpoint.empty(shardId);

        StoreFileMetadata storeFileMetadata1 = new StoreFileMetadata("test-1", 500, "1", Version.LATEST, new BytesRef(500));
        StoreFileMetadata storeFileMetadata2 = new StoreFileMetadata("test-2", 500, "1", Version.LATEST, new BytesRef(500));
        Map<String, StoreFileMetadata> stringStoreFileMetadataMapOne = new HashMap<>();
        stringStoreFileMetadataMapOne.put("test-1", storeFileMetadata1);
        stringStoreFileMetadataMapOne.put("test-2", storeFileMetadata2);
        ReplicationCheckpoint secondReplicationCheckpoint = new ReplicationCheckpoint(
            shardId,
            2,
            2,
            2,
            1000,
            "",
            stringStoreFileMetadataMapOne,
            System.nanoTime() - TimeUnit.MINUTES.toNanos(1)
        );

        IndexShard replicaShard = mock(IndexShard.class);
        when(replicaShard.shardId()).thenReturn(shardId);
        when(replicaShard.getLatestReplicationCheckpoint()).thenReturn(firstReplicationCheckpoint)
            .thenReturn(firstReplicationCheckpoint)
            .thenReturn(firstReplicationCheckpoint)
            .thenReturn(secondReplicationCheckpoint);

        SegmentReplicator segmentReplicator = new SegmentReplicator(threadPool);
        segmentReplicator.initializeStats(shardId);
        segmentReplicator.updateReplicationCheckpointStats(firstReplicationCheckpoint, replicaShard);
        segmentReplicator.updateReplicationCheckpointStats(secondReplicationCheckpoint, replicaShard);

        Map<String, StoreFileMetadata> stringStoreFileMetadataMapTwo = new HashMap<>();
        StoreFileMetadata storeFileMetadata3 = new StoreFileMetadata("test-3", 200, "1", Version.LATEST, new BytesRef(200));
        stringStoreFileMetadataMapTwo.put("test-1", storeFileMetadata1);
        stringStoreFileMetadataMapTwo.put("test-2", storeFileMetadata2);
        stringStoreFileMetadataMapTwo.put("test-3", storeFileMetadata3);
        ReplicationCheckpoint thirdReplicationCheckpoint = new ReplicationCheckpoint(
            shardId,
            3,
            3,
            3,
            200,
            "",
            stringStoreFileMetadataMapTwo,
            System.nanoTime() - TimeUnit.MINUTES.toNanos(1)
        );

        segmentReplicator.updateReplicationCheckpointStats(thirdReplicationCheckpoint, replicaShard);

        ReplicationStats replicationStatsFirst = segmentReplicator.getSegmentReplicationStats(shardId);
        assertEquals(1200, replicationStatsFirst.totalBytesBehind);
        assertEquals(1200, replicationStatsFirst.maxBytesBehind);
        assertTrue(replicationStatsFirst.maxReplicationLag > 0);

        segmentReplicator.pruneCheckpointsUpToLastSync(replicaShard);

        ReplicationStats replicationStatsSecond = segmentReplicator.getSegmentReplicationStats(shardId);
        assertEquals(200, replicationStatsSecond.totalBytesBehind);
        assertEquals(200, replicationStatsSecond.maxBytesBehind);
        assertTrue(replicationStatsSecond.maxReplicationLag > 0);
    }

    public void testGetSegmentReplicationStats_WhenCheckPointReceivedOutOfOrder() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        ReplicationCheckpoint firstReplicationCheckpoint = ReplicationCheckpoint.empty(shardId);

        StoreFileMetadata storeFileMetadata1 = new StoreFileMetadata("test-1", 500, "1", Version.LATEST, new BytesRef(500));
        StoreFileMetadata storeFileMetadata2 = new StoreFileMetadata("test-2", 500, "1", Version.LATEST, new BytesRef(500));
        Map<String, StoreFileMetadata> stringStoreFileMetadataMapOne = new HashMap<>();
        stringStoreFileMetadataMapOne.put("test-1", storeFileMetadata1);
        stringStoreFileMetadataMapOne.put("test-2", storeFileMetadata2);
        ReplicationCheckpoint secondReplicationCheckpoint = new ReplicationCheckpoint(
            shardId,
            2,
            2,
            2,
            1000,
            "",
            stringStoreFileMetadataMapOne,
            System.nanoTime() - TimeUnit.MINUTES.toNanos(1)
        );

        IndexShard replicaShard = mock(IndexShard.class);
        when(replicaShard.shardId()).thenReturn(shardId);
        when(replicaShard.getLatestReplicationCheckpoint()).thenReturn(firstReplicationCheckpoint)
            .thenReturn(firstReplicationCheckpoint)
            .thenReturn(firstReplicationCheckpoint);

        SegmentReplicator segmentReplicator = new SegmentReplicator(threadPool);
        segmentReplicator.initializeStats(shardId);
        segmentReplicator.updateReplicationCheckpointStats(firstReplicationCheckpoint, replicaShard);

        Map<String, StoreFileMetadata> stringStoreFileMetadataMapTwo = new HashMap<>();
        StoreFileMetadata storeFileMetadata3 = new StoreFileMetadata("test-3", 200, "1", Version.LATEST, new BytesRef(200));
        stringStoreFileMetadataMapTwo.put("test-1", storeFileMetadata1);
        stringStoreFileMetadataMapTwo.put("test-2", storeFileMetadata2);
        stringStoreFileMetadataMapTwo.put("test-3", storeFileMetadata3);
        ReplicationCheckpoint thirdReplicationCheckpoint = new ReplicationCheckpoint(
            shardId,
            3,
            3,
            3,
            200,
            "",
            stringStoreFileMetadataMapTwo,
            System.nanoTime() - TimeUnit.MINUTES.toNanos(1)
        );

        segmentReplicator.updateReplicationCheckpointStats(thirdReplicationCheckpoint, replicaShard);

        ReplicationStats replicationStatsFirst = segmentReplicator.getSegmentReplicationStats(shardId);
        assertEquals(1200, replicationStatsFirst.totalBytesBehind);
        assertEquals(1200, replicationStatsFirst.maxBytesBehind);
        assertTrue(replicationStatsFirst.maxReplicationLag > 0);

        segmentReplicator.updateReplicationCheckpointStats(secondReplicationCheckpoint, replicaShard);
        ReplicationStats replicationStatsSecond = segmentReplicator.getSegmentReplicationStats(shardId);
        assertEquals(1200, replicationStatsSecond.totalBytesBehind);
        assertEquals(1200, replicationStatsSecond.maxBytesBehind);
        assertTrue(replicationStatsSecond.maxReplicationLag > 0);
    }

    public void testUpdateReplicationCheckpointStatsIgnoresWhenOutOfOrderCheckPointReceived() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        IndexShard replicaShard = mock(IndexShard.class);
        when(replicaShard.shardId()).thenReturn(shardId);

        SegmentReplicator segmentReplicator = new SegmentReplicator(threadPool);
        ReplicationCheckpoint replicationCheckpoint = new ReplicationCheckpoint(
            shardId,
            2,
            2,
            2,
            1000,
            "",
            new HashMap<>(),
            System.nanoTime() - TimeUnit.MINUTES.toNanos(1)
        );
        segmentReplicator.updateReplicationCheckpointStats(replicationCheckpoint, replicaShard);

        assertEquals(replicationCheckpoint, segmentReplicator.getPrimaryCheckpoint(shardId));

        ReplicationCheckpoint oldReplicationCheckpoint = new ReplicationCheckpoint(
            shardId,
            1,
            1,
            1,
            500,
            "",
            new HashMap<>(),
            System.nanoTime() - TimeUnit.MINUTES.toNanos(1)
        );
        segmentReplicator.updateReplicationCheckpointStats(oldReplicationCheckpoint, replicaShard);

        assertEquals(replicationCheckpoint, segmentReplicator.getPrimaryCheckpoint(shardId));
    }

    protected void resolveCheckpointListener(ActionListener<CheckpointInfoResponse> listener, IndexShard primary) {
        try (final CopyState copyState = new CopyState(primary)) {
            listener.onResponse(
                new CheckpointInfoResponse(copyState.getCheckpoint(), copyState.getMetadataMap(), copyState.getInfosBytes())
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
