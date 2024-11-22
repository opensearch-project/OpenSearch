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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.routing.TestShardRouting;
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
import org.junit.Before;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSegmentReplicationStatsActionTests extends OpenSearchTestCase {

    private static final String TEST_INDEX = "test-index";

    private SegmentReplicationPerGroupStats segmentReplicationPerGroupStats;

    private IndexShard indexShard;

    private SegmentReplicationState completedSegmentReplicationState;
    private SegmentReplicationState onGoingSegmentReplicationState;

    private SegmentReplicationTargetService targetService;

    private ShardId shardId;

    TransportSegmentReplicationStatsAction action;

    private final ClusterBlock writeClusterBlock = new ClusterBlock(
        1,
        "uuid",
        "",
        true,
        true,
        true,
        RestStatus.OK,
        EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
    );

    private final ClusterBlock readClusterBlock = new ClusterBlock(
        1,
        "uuid",
        "",
        true,
        true,
        true,
        RestStatus.OK,
        EnumSet.of(ClusterBlockLevel.METADATA_READ)
    );

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Index index = new Index(TEST_INDEX, "_na_");
        shardId = new ShardId(TEST_INDEX, "_na_", 0);

        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);

        indexShard = mock(IndexShard.class);
        SegmentReplicationPressureService pressureService = mock(SegmentReplicationPressureService.class);
        segmentReplicationPerGroupStats = mock(SegmentReplicationPerGroupStats.class);
        targetService = mock(SegmentReplicationTargetService.class);
        completedSegmentReplicationState = mock(SegmentReplicationState.class);
        onGoingSegmentReplicationState = mock(SegmentReplicationState.class);
        ReplicationCheckpoint completedCheckpoint = mock(ReplicationCheckpoint.class);
        ReplicationCheckpoint onGoingCheckpoint = mock(ReplicationCheckpoint.class);

        ReplicationTimer replicationTimerCompleted = mock(ReplicationTimer.class);
        ReplicationTimer replicationTimerOngoing = mock(ReplicationTimer.class);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder(TEST_INDEX).settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(pressureService.getStatsForShard(indexShard)).thenReturn(segmentReplicationPerGroupStats);
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        when(completedSegmentReplicationState.getTimer()).thenReturn(replicationTimerCompleted);
        when(onGoingSegmentReplicationState.getTimer()).thenReturn(replicationTimerOngoing);
        when(onGoingSegmentReplicationState.getReplicationCheckpoint()).thenReturn(onGoingCheckpoint);
        when(completedSegmentReplicationState.getReplicationCheckpoint()).thenReturn(completedCheckpoint);

        long segmentInfoCompleted = 5;
        long segmentInfoOngoing = 9;
        when(onGoingCheckpoint.getSegmentInfosVersion()).thenReturn(segmentInfoOngoing);
        when(completedCheckpoint.getSegmentInfosVersion()).thenReturn(segmentInfoCompleted);

        final StoreFileMetadata segment_1 = new StoreFileMetadata("segment_1", 1L, "abcd", org.apache.lucene.util.Version.LATEST);
        final StoreFileMetadata segment_2 = new StoreFileMetadata("segment_2", 50L, "abcd", org.apache.lucene.util.Version.LATEST);

        when(onGoingCheckpoint.getMetadataMap()).thenReturn(Map.of("segment_1", segment_1, "segment_2", segment_2));
        when(completedCheckpoint.getMetadataMap()).thenReturn(Map.of("segment_1", segment_1));

        long time1 = 10;
        long time2 = 15;
        when(replicationTimerOngoing.time()).thenReturn(time1);
        when(replicationTimerCompleted.time()).thenReturn(time2);

        action = new TransportSegmentReplicationStatsAction(
            mock(ClusterService.class),
            mock(TransportService.class),
            indicesService,
            targetService,
            new ActionFilters(new HashSet<>()),
            mock(IndexNameExpressionResolver.class),
            pressureService
        );
    }

    DiscoveryNode newNode(int nodeId) {
        return new DiscoveryNode("node_" + nodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    }

    public void testShardReturnsAllTheShardsForTheIndex1() {
        SegmentReplicationStatsRequest segmentReplicationStatsRequest = mock(SegmentReplicationStatsRequest.class);
        String[] concreteIndices = new String[] { TEST_INDEX };
        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable routingTables = mock(RoutingTable.class);
        ShardsIterator shardsIterator = mock(ShardIterator.class);

        when(clusterState.routingTable()).thenReturn(routingTables);
        when(routingTables.allShardsIncludingRelocationTargets(any())).thenReturn(shardsIterator);
        assertEquals(shardsIterator, action.shards(clusterState, segmentReplicationStatsRequest, concreteIndices));
    }

    public void testGlobalBlockCheck() {
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

    public void testShardOperationWhenReplicationIsNotSegRep() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder(TEST_INDEX).settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        when(indexShard.indexSettings()).thenReturn(indexSettings);

        final DiscoveryNode node = newNode(0);
        final ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            TEST_INDEX,
            shardId.getId(),
            node.getId(),
            true,
            ShardRoutingState.STARTED
        );
        SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest();
        SegmentReplicationShardStatsResponse response = action.shardOperation(segmentReplicationStatsRequest, shardRouting);
        assertNull(response);
    }

    public void testShardOperationOnPrimaryShard() {
        final DiscoveryNode node = newNode(0);
        final ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            TEST_INDEX,
            shardId.getId(),
            node.getId(),
            true,
            ShardRoutingState.STARTED
        );
        SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest();
        SegmentReplicationShardStatsResponse response = action.shardOperation(segmentReplicationStatsRequest, shardRouting);

        assertEquals(segmentReplicationPerGroupStats, response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNull(response.getSegmentReplicationShardStats());
    }

    public void testShardOperationOnReplicaShard() {
        when(targetService.getSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);

        final DiscoveryNode node = newNode(0);
        final ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            TEST_INDEX,
            shardId.getId(),
            node.getId(),
            false,
            ShardRoutingState.STARTED
        );
        SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest();
        segmentReplicationStatsRequest.activeOnly(false);
        SegmentReplicationShardStatsResponse response = action.shardOperation(segmentReplicationStatsRequest, shardRouting);

        assertEquals(completedSegmentReplicationState, response.getReplicaStats());
        assertNull(response.getPrimaryStats());
        assertNull(response.getSegmentReplicationShardStats());
    }

    public void testShardOperationOnReplicaShardWhenActiveOnlyIsSet() {
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);

        final DiscoveryNode node = newNode(0);
        final ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            TEST_INDEX,
            shardId.getId(),
            node.getId(),
            false,
            ShardRoutingState.STARTED
        );
        SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest();
        segmentReplicationStatsRequest.activeOnly(true);
        SegmentReplicationShardStatsResponse response = action.shardOperation(segmentReplicationStatsRequest, shardRouting);

        assertEquals(onGoingSegmentReplicationState, response.getReplicaStats());
        assertNull(response.getPrimaryStats());
        assertNull(response.getSegmentReplicationShardStats());
    }

    public void testShardOperationOnSearchReplicaWhenCompletedAndOngoingSegRepStateNotNull() {
        when(targetService.getlatestCompletedEventSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(targetService.getSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);

        final DiscoveryNode node = newNode(0);
        final ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        ShardRouting searchShardRouting = TestShardRouting.newShardRouting(
            shardId,
            node.getId(),
            null,
            false,
            true,
            ShardRoutingState.STARTED,
            null
        );

        SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest();
        SegmentReplicationShardStatsResponse response = action.shardOperation(segmentReplicationStatsRequest, searchShardRouting);

        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
    }

    public void testShardOperationOnSearchReplicaWhenCompletedSegRepStateIsNull() {
        when(targetService.getOngoingEventSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);
        when(targetService.getSegmentReplicationState(shardId)).thenReturn(onGoingSegmentReplicationState);

        final DiscoveryNode node = newNode(0);
        final ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        ShardRouting searchShardRouting = TestShardRouting.newShardRouting(
            shardId,
            node.getId(),
            null,
            false,
            true,
            ShardRoutingState.STARTED,
            null
        );

        SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest();
        SegmentReplicationShardStatsResponse response = action.shardOperation(segmentReplicationStatsRequest, searchShardRouting);

        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
    }

    public void testShardOperationOnSearchReplicaWhenOngoingSegRepStateIsNull() {
        when(targetService.getlatestCompletedEventSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);
        when(targetService.getSegmentReplicationState(shardId)).thenReturn(completedSegmentReplicationState);

        final DiscoveryNode node = newNode(0);
        final ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        ShardRouting searchShardRouting = TestShardRouting.newShardRouting(
            shardId,
            node.getId(),
            null,
            false,
            true,
            ShardRoutingState.STARTED,
            null
        );

        SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest();
        SegmentReplicationShardStatsResponse response = action.shardOperation(segmentReplicationStatsRequest, searchShardRouting);

        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
    }

    public void testShardOperationOnSearchReplicaWhenCompletedAndOngoingSegRepStateIsNull() {
        final DiscoveryNode node = newNode(0);
        final ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);

        ShardRouting searchShardRouting = TestShardRouting.newShardRouting(
            shardId,
            node.getId(),
            null,
            false,
            true,
            ShardRoutingState.STARTED,
            null
        );

        SegmentReplicationStatsRequest segmentReplicationStatsRequest = new SegmentReplicationStatsRequest();
        SegmentReplicationShardStatsResponse response = action.shardOperation(segmentReplicationStatsRequest, searchShardRouting);

        assertNull(response.getPrimaryStats());
        assertNull(response.getReplicaStats());
        assertNotNull(response.getSegmentReplicationShardStats());
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
        when(shardIdOne.getIndexName()).thenReturn(TEST_INDEX);

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

        List<SegmentReplicationPerGroupStats> responseStats = response.getReplicationStats().get(TEST_INDEX);
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
        when(shardIdOne.getIndexName()).thenReturn(TEST_INDEX);
        when(shardIdTwo.getIndexName()).thenReturn(TEST_INDEX);
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

        List<SegmentReplicationPerGroupStats> responseStats = response.getReplicationStats().get(TEST_INDEX);

        for (SegmentReplicationPerGroupStats primStat : responseStats) {
            if (primStat.getShardId().equals(shardIdOne)) {
                assertEquals(segmentReplicationPerGroupStatsOne, primStat);
            }

            if (primStat.getShardId().equals(shardIdTwo)) {
                assertEquals(segmentReplicationPerGroupStatsTwo, primStat);
            }
        }
    }
}
