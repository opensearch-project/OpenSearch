/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.replication;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportSegmentReplicationStatsActionTests extends OpenSearchTestCase {
    @Mock
    private ClusterService clusterService;
    @Mock
    private TransportService transportService;
    @Mock
    private IndicesService indicesService;
    @Mock
    private SegmentReplicationTargetService targetService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;
    @Mock
    private SegmentReplicationPressureService pressureService;
    @Mock
    private IndexShard indexShard;
    @Mock
    private IndexService indexService;

    private TransportSegmentReplicationStatsAction action;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        super.setUp();
        action = new TransportSegmentReplicationStatsAction(
            clusterService,
            transportService,
            indicesService,
            targetService,
            actionFilters,
            indexNameExpressionResolver,
            pressureService
        );
    }

    public void testShardReturnsAllTheShardsForTheIndex() {
        SegmentReplicationStatsRequest segmentReplicationStatsRequest = mock(SegmentReplicationStatsRequest.class);
        String[] concreteIndices = new String[] { "test-index" };
        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable routingTables = mock(RoutingTable.class);
        ShardsIterator shardsIterator = mock(ShardIterator.class);

        when(clusterState.routingTable()).thenReturn(routingTables);
        when(routingTables.allShardsIncludingRelocationTargets(any())).thenReturn(shardsIterator);
        assertEquals(shardsIterator, action.shards(clusterState, segmentReplicationStatsRequest, concreteIndices));
    }

    public void testShardOperationWithPrimaryShard() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(true);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        verify(pressureService).getStatsForShard(any());
    }

    public void testShardOperationWithReplicaShard() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        request.activeOnly(false);
        SegmentReplicationState completedSegmentReplicationState = mock(SegmentReplicationState.class);

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(false);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());
        when(targetService.getSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        assertNull(response.getPrimaryStats());
        assertNotNull(response.getReplicaStats());
        verify(targetService).getSegmentReplicationState(shardId);
    }

    public void testShardOperationWithReplicaShardActiveOnly() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        request.activeOnly(true);
        SegmentReplicationState onGoingSegmentReplicationState = mock(SegmentReplicationState.class);

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(false);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        assertNull(response.getPrimaryStats());
        assertNotNull(response.getReplicaStats());
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testComputeBytesRemainingToReplicateWhenCompletedAndOngoingStateNotNull() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        SegmentReplicationState completedSegmentReplicationState = mock(SegmentReplicationState.class);
        SegmentReplicationState onGoingSegmentReplicationState = mock(SegmentReplicationState.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        ReplicationTimer replicationTimerCompleted = mock(ReplicationTimer.class);
        ReplicationTimer replicationTimerOngoing = mock(ReplicationTimer.class);
        long time1 = 10;
        long time2 = 15;
        ReplicationLuceneIndex replicationLuceneIndex = new ReplicationLuceneIndex();
        replicationLuceneIndex.addFileDetail("name1", 10, false);
        replicationLuceneIndex.addFileDetail("name2", 15, false);

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(targetService.getlatestCompletedEventSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(completedSegmentReplicationState.getTimer()).thenReturn(replicationTimerCompleted);
        when(onGoingSegmentReplicationState.getTimer()).thenReturn(replicationTimerOngoing);
        when(replicationTimerOngoing.time()).thenReturn(time1);
        when(replicationTimerCompleted.time()).thenReturn(time2);
        when(onGoingSegmentReplicationState.getIndex()).thenReturn(replicationLuceneIndex);

        SegmentReplicationShardStats segmentReplicationShardStats = action.computeSegmentReplicationShardStats(shardRouting);

        assertNotNull(segmentReplicationShardStats);
        assertEquals(25, segmentReplicationShardStats.getBytesBehindCount());
        assertEquals(10, segmentReplicationShardStats.getCurrentReplicationLagMillis());
        assertEquals(15, segmentReplicationShardStats.getLastCompletedReplicationTimeMillis());

        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testCalculateBytesRemainingToReplicateWhenNoCompletedState() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        SegmentReplicationState onGoingSegmentReplicationState = mock(SegmentReplicationState.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        ReplicationTimer replicationTimerOngoing = mock(ReplicationTimer.class);
        long time1 = 10;
        ReplicationLuceneIndex replicationLuceneIndex = new ReplicationLuceneIndex();
        replicationLuceneIndex.addFileDetail("name1", 10, false);
        replicationLuceneIndex.addFileDetail("name2", 15, false);

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(onGoingSegmentReplicationState.getTimer()).thenReturn(replicationTimerOngoing);
        when(replicationTimerOngoing.time()).thenReturn(time1);
        when(onGoingSegmentReplicationState.getIndex()).thenReturn(replicationLuceneIndex);

        SegmentReplicationShardStats segmentReplicationShardStats = action.computeSegmentReplicationShardStats(shardRouting);

        assertNotNull(segmentReplicationShardStats);
        assertEquals(25, segmentReplicationShardStats.getBytesBehindCount());
        assertEquals(10, segmentReplicationShardStats.getCurrentReplicationLagMillis());
        assertEquals(0, segmentReplicationShardStats.getLastCompletedReplicationTimeMillis());

        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testCalculateBytesRemainingToReplicateWhenNoOnGoingState() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        SegmentReplicationState completedSegmentReplicationState = mock(SegmentReplicationState.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        ReplicationTimer replicationTimerCompleted = mock(ReplicationTimer.class);
        long time2 = 15;

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(targetService.getlatestCompletedEventSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);
        when(completedSegmentReplicationState.getTimer()).thenReturn(replicationTimerCompleted);
        when(replicationTimerCompleted.time()).thenReturn(time2);

        SegmentReplicationShardStats segmentReplicationShardStats = action.computeSegmentReplicationShardStats(shardRouting);

        assertNotNull(segmentReplicationShardStats);
        assertEquals(0, segmentReplicationShardStats.getBytesBehindCount());
        assertEquals(0, segmentReplicationShardStats.getCurrentReplicationLagMillis());
        assertEquals(15, segmentReplicationShardStats.getLastCompletedReplicationTimeMillis());

        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testCalculateBytesRemainingToReplicateWhenNoCompletedAndOngoingState() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.allocationId()).thenReturn(allocationId);

        SegmentReplicationShardStats segmentReplicationShardStats = action.computeSegmentReplicationShardStats(shardRouting);

        assertNotNull(segmentReplicationShardStats);
        assertEquals(0, segmentReplicationShardStats.getBytesBehindCount());
        assertEquals(0, segmentReplicationShardStats.getCurrentReplicationLagMillis());
        assertEquals(0, segmentReplicationShardStats.getLastCompletedReplicationTimeMillis());

        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testNewResponseWhenAllReplicasReturnResponseCombinesTheResults() {
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        String[] shards = { "0", "1" };
        request.shards(shards);

        int totalShards = 6;
        int successfulShards = 6;
        int failedShard = 0;
        String allocIdOne = "allocIdOne";
        String allocIdTwo = "allocIdTwo";
        String allocIdThree = "allocIdThree";
        String allocIdFour = "allocIdFour";
        String allocIdFive = "allocIdFive";
        String allocIdSix = "allocIdSix";

        ShardId shardId0 = mock(ShardId.class);
        ShardRouting primary0 = mock(ShardRouting.class);
        ShardRouting replica0 = mock(ShardRouting.class);
        ShardRouting searchReplica0 = mock(ShardRouting.class);

        ShardId shardId1 = mock(ShardId.class);
        ShardRouting primary1 = mock(ShardRouting.class);
        ShardRouting replica1 = mock(ShardRouting.class);
        ShardRouting searchReplica1 = mock(ShardRouting.class);

        when(shardId0.getId()).thenReturn(0);
        when(shardId0.getIndexName()).thenReturn("test-index-1");
        when(primary0.shardId()).thenReturn(shardId0);
        when(replica0.shardId()).thenReturn(shardId0);
        when(searchReplica0.shardId()).thenReturn(shardId0);

        when(shardId1.getId()).thenReturn(1);
        when(shardId1.getIndexName()).thenReturn("test-index-1");
        when(primary1.shardId()).thenReturn(shardId1);
        when(replica1.shardId()).thenReturn(shardId1);
        when(searchReplica1.shardId()).thenReturn(shardId1);

        AllocationId allocationIdOne = mock(AllocationId.class);
        AllocationId allocationIdTwo = mock(AllocationId.class);
        AllocationId allocationIdThree = mock(AllocationId.class);
        AllocationId allocationIdFour = mock(AllocationId.class);
        AllocationId allocationIdFive = mock(AllocationId.class);
        AllocationId allocationIdSix = mock(AllocationId.class);

        when(allocationIdOne.getId()).thenReturn(allocIdOne);
        when(allocationIdTwo.getId()).thenReturn(allocIdTwo);
        when(allocationIdThree.getId()).thenReturn(allocIdThree);
        when(allocationIdFour.getId()).thenReturn(allocIdFour);
        when(allocationIdFive.getId()).thenReturn(allocIdFive);
        when(allocationIdSix.getId()).thenReturn(allocIdSix);
        when(primary0.allocationId()).thenReturn(allocationIdOne);
        when(replica0.allocationId()).thenReturn(allocationIdTwo);
        when(searchReplica0.allocationId()).thenReturn(allocationIdThree);
        when(primary1.allocationId()).thenReturn(allocationIdFour);
        when(replica1.allocationId()).thenReturn(allocationIdFive);
        when(searchReplica1.allocationId()).thenReturn(allocationIdSix);

        when(primary0.isSearchOnly()).thenReturn(false);
        when(replica0.isSearchOnly()).thenReturn(false);
        when(searchReplica0.isSearchOnly()).thenReturn(true);
        when(primary1.isSearchOnly()).thenReturn(false);
        when(replica1.isSearchOnly()).thenReturn(false);
        when(searchReplica1.isSearchOnly()).thenReturn(true);

        Set<SegmentReplicationShardStats> segmentReplicationShardStats0 = new HashSet<>();
        SegmentReplicationShardStats segmentReplicationShardStatsOfReplica0 = new SegmentReplicationShardStats(allocIdTwo, 0, 0, 0, 0, 0);
        segmentReplicationShardStats0.add(segmentReplicationShardStatsOfReplica0);

        Set<SegmentReplicationShardStats> segmentReplicationShardStats1 = new HashSet<>();
        SegmentReplicationShardStats segmentReplicationShardStatsOfReplica1 = new SegmentReplicationShardStats(allocIdFive, 0, 0, 0, 0, 0);
        segmentReplicationShardStats1.add(segmentReplicationShardStatsOfReplica1);

        SegmentReplicationPerGroupStats segmentReplicationPerGroupStats0 = new SegmentReplicationPerGroupStats(
            shardId0,
            segmentReplicationShardStats0,
            0
        );

        SegmentReplicationPerGroupStats segmentReplicationPerGroupStats1 = new SegmentReplicationPerGroupStats(
            shardId1,
            segmentReplicationShardStats1,
            0
        );

        SegmentReplicationState segmentReplicationState0 = mock(SegmentReplicationState.class);
        SegmentReplicationState searchReplicaSegmentReplicationState0 = mock(SegmentReplicationState.class);
        SegmentReplicationState segmentReplicationState1 = mock(SegmentReplicationState.class);
        SegmentReplicationState searchReplicaSegmentReplicationState1 = mock(SegmentReplicationState.class);

        when(segmentReplicationState0.getShardRouting()).thenReturn(replica0);
        when(searchReplicaSegmentReplicationState0.getShardRouting()).thenReturn(searchReplica0);
        when(segmentReplicationState1.getShardRouting()).thenReturn(replica1);
        when(searchReplicaSegmentReplicationState1.getShardRouting()).thenReturn(searchReplica1);

        List<SegmentReplicationShardStatsResponse> responses = List.of(
            new SegmentReplicationShardStatsResponse(segmentReplicationPerGroupStats0),
            new SegmentReplicationShardStatsResponse(segmentReplicationState0),
            new SegmentReplicationShardStatsResponse(searchReplicaSegmentReplicationState0),
            new SegmentReplicationShardStatsResponse(segmentReplicationPerGroupStats1),
            new SegmentReplicationShardStatsResponse(segmentReplicationState1),
            new SegmentReplicationShardStatsResponse(searchReplicaSegmentReplicationState1)
        );

        SegmentReplicationStatsResponse response = action.newResponse(
            request,
            totalShards,
            successfulShards,
            failedShard,
            responses,
            shardFailures,
            ClusterState.EMPTY_STATE
        );

        List<SegmentReplicationPerGroupStats> responseStats = response.getReplicationStats().get("test-index-1");
        SegmentReplicationPerGroupStats primStats0 = responseStats.get(0);
        Set<SegmentReplicationShardStats> replicaStats0 = primStats0.getReplicaStats();
        assertEquals(2, replicaStats0.size());
        for (SegmentReplicationShardStats replicaStat : replicaStats0) {
            if (replicaStat.getAllocationId().equals(allocIdTwo)) {
                assertEquals(segmentReplicationState0, replicaStat.getCurrentReplicationState());
            }

            if (replicaStat.getAllocationId().equals(allocIdThree)) {
                assertEquals(searchReplicaSegmentReplicationState0, replicaStat.getCurrentReplicationState());
            }
        }

        SegmentReplicationPerGroupStats primStats1 = responseStats.get(1);
        Set<SegmentReplicationShardStats> replicaStats1 = primStats1.getReplicaStats();
        assertEquals(2, replicaStats1.size());
        for (SegmentReplicationShardStats replicaStat : replicaStats1) {
            if (replicaStat.getAllocationId().equals(allocIdFive)) {
                assertEquals(segmentReplicationState1, replicaStat.getCurrentReplicationState());
            }

            if (replicaStat.getAllocationId().equals(allocIdSix)) {
                assertEquals(searchReplicaSegmentReplicationState1, replicaStat.getCurrentReplicationState());
            }
        }
    }

    public void testNewResponseWhenShardsToFetchEmptyAndResponsesContainsNull() {
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        String[] shards = {};
        request.shards(shards);

        int totalShards = 3;
        int successfulShards = 3;
        int failedShard = 0;
        String allocIdOne = "allocIdOne";
        String allocIdTwo = "allocIdTwo";
        ShardId shardIdOne = mock(ShardId.class);
        ShardId shardIdTwo = mock(ShardId.class);
        ShardId shardIdThree = mock(ShardId.class);
        ShardRouting shardRoutingOne = mock(ShardRouting.class);
        ShardRouting shardRoutingTwo = mock(ShardRouting.class);
        ShardRouting shardRoutingThree = mock(ShardRouting.class);
        when(shardIdOne.getId()).thenReturn(1);
        when(shardIdTwo.getId()).thenReturn(2);
        when(shardIdThree.getId()).thenReturn(3);
        when(shardRoutingOne.shardId()).thenReturn(shardIdOne);
        when(shardRoutingTwo.shardId()).thenReturn(shardIdTwo);
        when(shardRoutingThree.shardId()).thenReturn(shardIdThree);
        AllocationId allocationId = mock(AllocationId.class);
        when(allocationId.getId()).thenReturn(allocIdOne);
        when(shardRoutingTwo.allocationId()).thenReturn(allocationId);
        when(shardIdOne.getIndexName()).thenReturn("test-index");

        Set<SegmentReplicationShardStats> segmentReplicationShardStats = new HashSet<>();
        SegmentReplicationShardStats segmentReplicationShardStatsOfReplica = new SegmentReplicationShardStats(allocIdOne, 0, 0, 0, 0, 0);
        segmentReplicationShardStats.add(segmentReplicationShardStatsOfReplica);
        SegmentReplicationPerGroupStats segmentReplicationPerGroupStats = new SegmentReplicationPerGroupStats(
            shardIdOne,
            segmentReplicationShardStats,
            0
        );

        SegmentReplicationState segmentReplicationState = mock(SegmentReplicationState.class);
        SegmentReplicationShardStats segmentReplicationShardStatsFromSearchReplica = mock(SegmentReplicationShardStats.class);
        when(segmentReplicationShardStatsFromSearchReplica.getAllocationId()).thenReturn("alloc2");
        when(segmentReplicationState.getShardRouting()).thenReturn(shardRoutingTwo);

        List<SegmentReplicationShardStatsResponse> responses = new ArrayList<>();
        responses.add(null);
        responses.add(new SegmentReplicationShardStatsResponse(segmentReplicationPerGroupStats));
        responses.add(new SegmentReplicationShardStatsResponse(segmentReplicationState));

        SegmentReplicationStatsResponse response = action.newResponse(
            request,
            totalShards,
            successfulShards,
            failedShard,
            responses,
            shardFailures,
            ClusterState.EMPTY_STATE
        );

        List<SegmentReplicationPerGroupStats> responseStats = response.getReplicationStats().get("test-index");
        SegmentReplicationPerGroupStats primStats = responseStats.get(0);
        Set<SegmentReplicationShardStats> segRpShardStatsSet = primStats.getReplicaStats();

        for (SegmentReplicationShardStats segRpShardStats : segRpShardStatsSet) {
            if (segRpShardStats.getAllocationId().equals(allocIdOne)) {
                assertEquals(segmentReplicationState, segRpShardStats.getCurrentReplicationState());
            }

            if (segRpShardStats.getAllocationId().equals(allocIdTwo)) {
                assertEquals(segmentReplicationShardStatsFromSearchReplica, segRpShardStats);
            }
        }
    }

    public void testShardOperationWithSegRepDisabled() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();

        when(shardRouting.shardId()).thenReturn(shardId);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepDisabled());

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNull(response);
    }

    public void testGlobalBlockCheck() {
        ClusterBlock writeClusterBlock = new ClusterBlock(
            1,
            "uuid",
            "",
            true,
            true,
            true,
            RestStatus.OK,
            EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
        );

        ClusterBlock readClusterBlock = new ClusterBlock(
            1,
            "uuid",
            "",
            true,
            true,
            true,
            RestStatus.OK,
            EnumSet.of(ClusterBlockLevel.METADATA_READ)
        );

        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addGlobalBlock(writeClusterBlock);
        ClusterState metadataWriteBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNull(action.checkGlobalBlock(metadataWriteBlockedState, new SegmentReplicationStatsRequest()));

        builder = ClusterBlocks.builder();
        builder.addGlobalBlock(readClusterBlock);
        ClusterState metadataReadBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNotNull(action.checkGlobalBlock(metadataReadBlockedState, new SegmentReplicationStatsRequest()));
    }

    public void testIndexBlockCheck() {
        ClusterBlock writeClusterBlock = new ClusterBlock(
            1,
            "uuid",
            "",
            true,
            true,
            true,
            RestStatus.OK,
            EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
        );

        ClusterBlock readClusterBlock = new ClusterBlock(
            1,
            "uuid",
            "",
            true,
            true,
            true,
            RestStatus.OK,
            EnumSet.of(ClusterBlockLevel.METADATA_READ)
        );

        String indexName = "test";
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addIndexBlock(indexName, writeClusterBlock);
        ClusterState metadataWriteBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNull(action.checkRequestBlock(metadataWriteBlockedState, new SegmentReplicationStatsRequest(), new String[] { indexName }));

        builder = ClusterBlocks.builder();
        builder.addIndexBlock(indexName, readClusterBlock);
        ClusterState metadataReadBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNotNull(action.checkRequestBlock(metadataReadBlockedState, new SegmentReplicationStatsRequest(), new String[] { indexName }));
    }

    private IndexSettings createIndexSettingsWithSegRepEnabled() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        return new IndexSettings(IndexMetadata.builder("test").settings(settings).build(), settings);
    }

    private IndexSettings createIndexSettingsWithSegRepDisabled() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        return new IndexSettings(IndexMetadata.builder("test").settings(settings).build(), settings);
    }
}
