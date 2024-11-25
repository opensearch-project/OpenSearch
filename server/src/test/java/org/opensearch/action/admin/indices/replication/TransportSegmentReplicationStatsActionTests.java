/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.replication;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        assertNull(response.getSegmentReplicationShardStats());
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
        assertNull(response.getSegmentReplicationShardStats());
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testShardOperationWithSearchOnlyReplicaWhenCompletedAndOngoingStateNotNull() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        SegmentReplicationState completedSegmentReplicationState = mock(SegmentReplicationState.class);
        SegmentReplicationState onGoingSegmentReplicationState = mock(SegmentReplicationState.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        ReplicationTimer replicationTimerCompleted = mock(ReplicationTimer.class);
        ReplicationTimer replicationTimerOngoing = mock(ReplicationTimer.class);
        ReplicationCheckpoint completedCheckpoint = mock(ReplicationCheckpoint.class);
        ReplicationCheckpoint onGoingCheckpoint = mock(ReplicationCheckpoint.class);
        long time1 = 10;
        long time2 = 15;
        final StoreFileMetadata segment_1 = new StoreFileMetadata("segment_1", 1L, "abcd", org.apache.lucene.util.Version.LATEST);
        final StoreFileMetadata segment_2 = new StoreFileMetadata("segment_2", 50L, "abcd", org.apache.lucene.util.Version.LATEST);
        long segmentInfoCompleted = 5;
        long segmentInfoOngoing = 9;

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(false);
        when(shardRouting.isSearchOnly()).thenReturn(true);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());
        when(targetService.getlatestCompletedEventSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(targetService.getSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(completedSegmentReplicationState.getTimer()).thenReturn(replicationTimerCompleted);
        when(completedSegmentReplicationState.getReplicationCheckpoint()).thenReturn(completedCheckpoint);
        when(onGoingSegmentReplicationState.getTimer()).thenReturn(replicationTimerOngoing);
        when(onGoingSegmentReplicationState.getReplicationCheckpoint()).thenReturn(onGoingCheckpoint);
        when(replicationTimerOngoing.time()).thenReturn(time1);
        when(replicationTimerCompleted.time()).thenReturn(time2);
        when(completedCheckpoint.getMetadataMap()).thenReturn(Map.of("segment_1", segment_1));
        when(onGoingCheckpoint.getMetadataMap()).thenReturn(Map.of("segment_1", segment_1, "segment_2", segment_2));
        when(onGoingCheckpoint.getSegmentInfosVersion()).thenReturn(segmentInfoOngoing);
        when(completedCheckpoint.getSegmentInfosVersion()).thenReturn(segmentInfoCompleted);

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testShardOperationWithSearchOnlyReplicaWhenNoCompletedState() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        SegmentReplicationState onGoingSegmentReplicationState = mock(SegmentReplicationState.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        ReplicationTimer replicationTimerOngoing = mock(ReplicationTimer.class);
        ReplicationCheckpoint onGoingCheckpoint = mock(ReplicationCheckpoint.class);
        long time1 = 10;
        final StoreFileMetadata segment_1 = new StoreFileMetadata("segment_1", 1L, "abcd", org.apache.lucene.util.Version.LATEST);
        final StoreFileMetadata segment_2 = new StoreFileMetadata("segment_2", 50L, "abcd", org.apache.lucene.util.Version.LATEST);
        long segmentInfoOngoing = 9;

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(false);
        when(shardRouting.isSearchOnly()).thenReturn(true);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(targetService.getSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(onGoingSegmentReplicationState.getTimer()).thenReturn(replicationTimerOngoing);
        when(onGoingSegmentReplicationState.getReplicationCheckpoint()).thenReturn(onGoingCheckpoint);
        when(replicationTimerOngoing.time()).thenReturn(time1);
        when(onGoingCheckpoint.getMetadataMap()).thenReturn(Map.of("segment_1", segment_1, "segment_2", segment_2));
        when(onGoingCheckpoint.getSegmentInfosVersion()).thenReturn(segmentInfoOngoing);

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testShardOperationWithSearchOnlyReplicaWhenNoCompletedCheckpoint() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        SegmentReplicationState completedSegmentReplicationState = mock(SegmentReplicationState.class);
        SegmentReplicationState onGoingSegmentReplicationState = mock(SegmentReplicationState.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        ReplicationTimer replicationTimerCompleted = mock(ReplicationTimer.class);
        ReplicationTimer replicationTimerOngoing = mock(ReplicationTimer.class);
        ReplicationCheckpoint onGoingCheckpoint = mock(ReplicationCheckpoint.class);
        long time1 = 10;
        long time2 = 15;
        final StoreFileMetadata segment_1 = new StoreFileMetadata("segment_1", 1L, "abcd", org.apache.lucene.util.Version.LATEST);
        final StoreFileMetadata segment_2 = new StoreFileMetadata("segment_2", 50L, "abcd", org.apache.lucene.util.Version.LATEST);
        long segmentInfoOngoing = 9;

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(false);
        when(shardRouting.isSearchOnly()).thenReturn(true);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());
        when(targetService.getlatestCompletedEventSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(targetService.getSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(completedSegmentReplicationState.getTimer()).thenReturn(replicationTimerCompleted);
        when(onGoingSegmentReplicationState.getTimer()).thenReturn(replicationTimerOngoing);
        when(onGoingSegmentReplicationState.getReplicationCheckpoint()).thenReturn(onGoingCheckpoint);
        when(replicationTimerOngoing.time()).thenReturn(time1);
        when(replicationTimerCompleted.time()).thenReturn(time2);
        when(onGoingCheckpoint.getMetadataMap()).thenReturn(Map.of("segment_1", segment_1, "segment_2", segment_2));
        when(onGoingCheckpoint.getSegmentInfosVersion()).thenReturn(segmentInfoOngoing);

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testShardOperationWithSearchOnlyReplicaWhenNoOngoingState() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        SegmentReplicationState completedSegmentReplicationState = mock(SegmentReplicationState.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        ReplicationTimer replicationTimerCompleted = mock(ReplicationTimer.class);
        ReplicationCheckpoint completedCheckpoint = mock(ReplicationCheckpoint.class);
        long time2 = 15;
        final StoreFileMetadata segment_1 = new StoreFileMetadata("segment_1", 1L, "abcd", org.apache.lucene.util.Version.LATEST);
        long segmentInfoCompleted = 5;

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(false);
        when(shardRouting.isSearchOnly()).thenReturn(true);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());
        when(targetService.getlatestCompletedEventSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);
        when(completedSegmentReplicationState.getTimer()).thenReturn(replicationTimerCompleted);
        when(completedSegmentReplicationState.getReplicationCheckpoint()).thenReturn(completedCheckpoint);
        when(replicationTimerCompleted.time()).thenReturn(time2);
        when(completedCheckpoint.getMetadataMap()).thenReturn(Map.of("segment_1", segment_1));
        when(completedCheckpoint.getSegmentInfosVersion()).thenReturn(segmentInfoCompleted);

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testShardOperationWithSearchOnlyReplicaWhenNoOngoingCheckpoint() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        SegmentReplicationState completedSegmentReplicationState = mock(SegmentReplicationState.class);
        SegmentReplicationState onGoingSegmentReplicationState = mock(SegmentReplicationState.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        ReplicationTimer replicationTimerCompleted = mock(ReplicationTimer.class);
        ReplicationTimer replicationTimerOngoing = mock(ReplicationTimer.class);
        ReplicationCheckpoint completedCheckpoint = mock(ReplicationCheckpoint.class);
        long time1 = 10;
        long time2 = 15;
        final StoreFileMetadata segment_1 = new StoreFileMetadata("segment_1", 1L, "abcd", org.apache.lucene.util.Version.LATEST);
        long segmentInfoCompleted = 5;

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(false);
        when(shardRouting.isSearchOnly()).thenReturn(true);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());
        when(targetService.getlatestCompletedEventSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(onGoingSegmentReplicationState.getTimer()).thenReturn(replicationTimerOngoing);
        when(completedSegmentReplicationState.getTimer()).thenReturn(replicationTimerCompleted);
        when(completedSegmentReplicationState.getReplicationCheckpoint()).thenReturn(completedCheckpoint);
        when(replicationTimerCompleted.time()).thenReturn(time2);
        when(replicationTimerOngoing.time()).thenReturn(time1);
        when(completedCheckpoint.getMetadataMap()).thenReturn(Map.of("segment_1", segment_1));
        when(completedCheckpoint.getSegmentInfosVersion()).thenReturn(segmentInfoCompleted);

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testShardOperationWithSearchOnlyReplicaWhenNoCompletedAndOngoingState() {
        ShardRouting shardRouting = mock(ShardRouting.class);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        AllocationId allocationId = AllocationId.newInitializing();
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();

        when(shardRouting.shardId()).thenReturn(shardId);
        when(shardRouting.primary()).thenReturn(false);
        when(shardRouting.isSearchOnly()).thenReturn(true);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.indexSettings()).thenReturn(createIndexSettingsWithSegRepEnabled());

        SegmentReplicationShardStatsResponse response = action.shardOperation(request, shardRouting);

        assertNotNull(response);
        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
        verify(targetService).getlatestCompletedEventSegmentReplicationState(shardId);
        verify(targetService).getOngoingEventSegmentReplicationState(shardId);
    }

    public void testNewResponseWhenAllReplicasReturnResponseCombinesTheResults() {
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        String[] shards = { "1", "2", "3" };
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

        List<SegmentReplicationShardStatsResponse> responses = List.of(
            new SegmentReplicationShardStatsResponse(segmentReplicationPerGroupStats),
            new SegmentReplicationShardStatsResponse(segmentReplicationState),
            new SegmentReplicationShardStatsResponse(segmentReplicationShardStatsFromSearchReplica)
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

    public void testNewResponseWhenTwoPrimaryShardsForSameIndex() {
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        String[] shards = { "1", "2" };
        request.shards(shards);
        int totalShards = 3;
        int successfulShards = 3;
        int failedShard = 0;

        SegmentReplicationPerGroupStats segmentReplicationPerGroupStatsOne = mock(SegmentReplicationPerGroupStats.class);
        SegmentReplicationPerGroupStats segmentReplicationPerGroupStatsTwo = mock(SegmentReplicationPerGroupStats.class);

        ShardId shardIdOne = mock(ShardId.class);
        ShardId shardIdTwo = mock(ShardId.class);
        when(segmentReplicationPerGroupStatsOne.getShardId()).thenReturn(shardIdOne);
        when(segmentReplicationPerGroupStatsTwo.getShardId()).thenReturn(shardIdTwo);
        when(shardIdOne.getIndexName()).thenReturn("test-index");
        when(shardIdTwo.getIndexName()).thenReturn("test-index");
        when(shardIdOne.getId()).thenReturn(1);
        when(shardIdTwo.getId()).thenReturn(2);

        List<SegmentReplicationShardStatsResponse> responses = List.of(
            new SegmentReplicationShardStatsResponse(segmentReplicationPerGroupStatsOne),
            new SegmentReplicationShardStatsResponse(segmentReplicationPerGroupStatsTwo)
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

        List<SegmentReplicationPerGroupStats> responseStats = response.getReplicationStats().get("test-index");

        for (SegmentReplicationPerGroupStats primStat : responseStats) {
            if (primStat.getShardId().equals(shardIdOne)) {
                assertEquals(segmentReplicationPerGroupStatsOne, primStat);
            }

            if (primStat.getShardId().equals(shardIdTwo)) {
                assertEquals(segmentReplicationPerGroupStatsTwo, primStat);
            }
        }
    }

    public void testNewResponseWhenShardsToFetchEmptyAndResponsesContainsNull() {
        SegmentReplicationStatsRequest request = new SegmentReplicationStatsRequest();
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        String[] shards = { };
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
        responses.add(new SegmentReplicationShardStatsResponse(segmentReplicationShardStatsFromSearchReplica));

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
