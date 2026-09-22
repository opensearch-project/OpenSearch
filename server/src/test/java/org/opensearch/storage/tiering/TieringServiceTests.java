/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.storage.action.tiering.CancelTieringRequest;
import org.opensearch.storage.action.tiering.IndexTieringRequest;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.ArgumentCaptor;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.TieringState.HOT_TO_WARM;
import static org.opensearch.storage.common.tiering.TieringUtils.TIERING_CUSTOM_KEY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TieringServiceTests extends OpenSearchTestCase {

    private TestTieringService tieringService;
    private ClusterService clusterService;
    private ClusterState clusterState;
    private Index testIndex;
    private IndexMetadata indexMetadata;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private NodeEnvironment nodeEnvironment;
    private ShardLimitValidator shardLimitValidator;

    Setting<Integer> maxConcurrentTieringRequestsSetting = Setting.intSetting(
        "test_max_tiering_requests",
        50,
        0,
        1000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private class TestTieringService extends TieringService {
        public TestTieringService(
            Settings settings,
            ClusterService clusterService,
            ClusterInfoService clusterInfoService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            AllocationService allocationService,
            NodeEnvironment nodeEnvironment,
            ShardLimitValidator shardLimitValidator
        ) {
            super(
                settings,
                clusterService,
                clusterInfoService,
                indexNameExpressionResolver,
                allocationService,
                nodeEnvironment,
                shardLimitValidator
            );
        }

        @Override
        protected Settings getTieringStartSettingsToAdd(IndexMetadata indexMetadata) {
            return Settings.builder()
                .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
                .put(INDEX_TIERING_STATE.getKey(), HOT_TO_WARM)
                .build();
        }

        @Override
        protected org.opensearch.cluster.block.ClusterBlocks.Builder getTieringStartClusterBlocksToAdd(
            org.opensearch.cluster.block.ClusterBlocks.Builder blocksBuilder,
            String indexName,
            IndexMetadata indexMetadata
        ) {
            return blocksBuilder;
        }

        // getIndexTierClusterBlocksToRestoreAfterCancellation intentionally uses the base class no-op:
        // the write block is kept during cancel and removed later by removeWriteBlockForCancelledDfaIndices().

        @Override
        protected Settings getIndexTierSettingsToRestoreAfterCancellation(IndexMetadata indexMetadata) {
            return Settings.builder()
                .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false)
                .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT)
                .build();
        }

        @Override
        protected String getTieringStartTimeKey() {
            return "test_tiering_start_time";
        }

        @Override
        protected Setting<Integer> getMaxConcurrentTieringRequestsSetting() {
            return maxConcurrentTieringRequestsSetting;
        }

        @Override
        protected IndexModule.TieringState getTargetTieringState() {
            return IndexModule.TieringState.WARM;
        }

        @Override
        protected IndexModule.TieringState getTieringType() {
            return HOT_TO_WARM;
        }

        @Override
        protected void validateTieringRequest(
            ClusterState clusterState,
            ClusterInfoService service,
            Set<Index> tieringEntries,
            Integer maxConcurrentTieringRequests,
            Integer jvmActiveUsageThresholdPercent,
            Index index
        ) {}
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        nodeEnvironment = newNodeEnvironment();
        shardLimitValidator = mock(ShardLimitValidator.class);
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);

        testIndex = new Index("test-index", "uuid");
        Settings indexSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
            .put(INDEX_TIERING_STATE.getKey(), "HOT")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();

        indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(2).build();

        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(maxConcurrentTieringRequestsSetting);
        clusterSettingsToAdd.add(TieringUtils.JVM_USAGE_TIERING_THRESHOLD_PERCENT);
        clusterSettingsToAdd.add(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING);

        Settings defaultSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "300b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "200b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100b")
            .put(maxConcurrentTieringRequestsSetting.getKey(), 50)
            .put(TieringUtils.JVM_USAGE_TIERING_THRESHOLD_PERCENT.getKey(), 90)
            .put(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5.0)
            .build();

        ClusterSettings mockSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        when(clusterService.getClusterSettings()).thenReturn(mockSettings);

        tieringService = new TestTieringService(
            defaultSettings,
            clusterService,
            mock(ClusterInfoService.class),
            indexNameExpressionResolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );
    }

    @Override
    public void tearDown() throws Exception {
        IOUtils.close(nodeEnvironment);
        super.tearDown();
    }

    public void testProcessTieringInProgress_HandlesDeletedIndex() {
        tieringService.tieringIndices.add(testIndex);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(false);

        tieringService.processTieringInProgress(clusterState, "test_source");

        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        assertTrue("Index should be removed from tieringIndices", tieringService.tieringIndices.isEmpty());
    }

    public void testProcessTieringInProgress_HandlesIncompleteShardRelocation() {
        tieringService.tieringIndices.add(testIndex);
        RoutingTable routingTable = mock(RoutingTable.class);
        ShardRouting shard = mock(ShardRouting.class);
        List<ShardRouting> shardRoutings = Collections.singletonList(shard);

        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);
        when(routingTable.allShards(testIndex.getName())).thenReturn(shardRoutings);

        // Stub shard so isShardStateValidForTier doesn't NPE
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        // Shard is on a hot node, target is WARM → not in target tier
        when(node.isWarmNode()).thenReturn(false);

        tieringService.processTieringInProgress(clusterState, "test_source");

        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        assertTrue("Index should remain in tieringIndices", tieringService.tieringIndices.contains(testIndex));
    }

    public void testProcessTieringInProgress_CompletesSuccessfully() throws Exception {
        tieringService.tieringIndices.add(testIndex);
        RoutingTable routingTable = mock(RoutingTable.class);
        ShardRouting shard = mock(ShardRouting.class);
        List<ShardRouting> shardRoutings = Collections.singletonList(shard);
        Metadata metadata = mock(Metadata.class);

        when(clusterState.routingTable()).thenReturn(routingTable);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(testIndex)).thenReturn(indexMetadata);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);
        when(routingTable.allShards(testIndex.getName())).thenReturn(shardRoutings);

        // Stub shard as started on a warm node → in target tier (WARM)
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(true);

        tieringService.processTieringInProgress(clusterState, "test_source");

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        ClusterStateUpdateTask capturedTask = taskCaptor.getValue();
        assertEquals(Priority.NORMAL, capturedTask.priority());

        // Execute the task to cover the execute() method
        Metadata realMetadata = Metadata.builder().put(indexMetadata, false).build();
        RoutingTable realRt = RoutingTable.builder().addAsNew(realMetadata.index("test-index")).build();
        ClusterState realState = ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(realMetadata)
            .routingTable(realRt)
            .build();
        ClusterState result = capturedTask.execute(realState);
        assertNotNull(result);

        capturedTask.clusterStateProcessed("test_source", clusterState, clusterState);
        assertFalse("Index should be removed after processing", tieringService.tieringIndices.contains(testIndex));
    }

    public void testUpdateClusterStateTask_OnFailure() {
        tieringService.tieringIndices.add(testIndex);
        RoutingTable routingTable = mock(RoutingTable.class);
        ShardRouting shard = mock(ShardRouting.class);
        Metadata metadata = mock(Metadata.class);

        when(clusterState.routingTable()).thenReturn(routingTable);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(testIndex)).thenReturn(indexMetadata);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);
        when(routingTable.allShards(testIndex.getName())).thenReturn(Collections.singletonList(shard));
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(true);

        tieringService.processTieringInProgress(clusterState, "test_source");

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        // Cover onFailure — should not throw
        taskCaptor.getValue().onFailure("test_source", new RuntimeException("simulated"));
    }

    public void testUpdateClusterStateTask_IndexDeletedDuringExecution() throws Exception {
        tieringService.tieringIndices.add(testIndex);
        RoutingTable routingTable = mock(RoutingTable.class);
        ShardRouting shard = mock(ShardRouting.class);
        Metadata metadata = mock(Metadata.class);

        when(clusterState.routingTable()).thenReturn(routingTable);
        when(clusterState.metadata()).thenReturn(metadata);
        // Return null for index metadata during execute() → covers the null branch
        when(metadata.index(testIndex)).thenReturn(null);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);
        when(routingTable.allShards(testIndex.getName())).thenReturn(Collections.singletonList(shard));
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(true);

        tieringService.processTieringInProgress(clusterState, "test_source");

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        // Execute with null indexMetadata — covers the null check branch
        // Use a real ClusterState where the index doesn't exist in metadata
        Metadata emptyMetadata = Metadata.builder().build();
        ClusterState realState = ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT).metadata(emptyMetadata).build();
        ClusterState result = taskCaptor.getValue().execute(realState);
        assertNotNull(result);
        assertFalse("Index should be removed from tieringIndices", tieringService.tieringIndices.contains(testIndex));
    }

    public void testTier_InitiatesSuccessfully() throws Exception {
        IndexTieringRequest request = new IndexTieringRequest("WARM", "test-index");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AllocationService allocationSvc = mock(AllocationService.class);

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            allocationSvc,
            nodeEnvironment,
            shardLimitValidator
        );

        when(resolver.concreteIndices(any(), any(), eq("test-index"))).thenReturn(new Index[] { testIndex });

        // Build a real cluster state so execute() works
        Settings idxSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), "HOT")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();
        IndexMetadata idxMeta = IndexMetadata.builder("test-index").settings(idxSettings).numberOfShards(1).numberOfReplicas(1).build();
        Metadata meta = Metadata.builder().put(idxMeta, false).build();
        RoutingTable rt = RoutingTable.builder().addAsNew(meta.index("test-index")).build();
        ClusterState realState = ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT).metadata(meta).routingTable(rt).build();

        when(allocationSvc.reroute(any(ClusterState.class), anyString())).thenAnswer(inv -> inv.getArgument(0));

        service.tier(request, listener, realState);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        ClusterStateUpdateTask capturedTask = taskCaptor.getValue();
        assertEquals(Priority.URGENT, capturedTask.priority());

        // Cover execute()
        ClusterState result = capturedTask.execute(realState);
        assertNotNull(result);

        // Cover timeout()
        assertNotNull(capturedTask.timeout());

        capturedTask.clusterStateProcessed("test_source", realState, realState);
        assertTrue("Index should be added to tieringIndices", service.tieringIndices.contains(testIndex));
    }

    public void testTier_FailsOnResolveException() {
        IndexTieringRequest request = new IndexTieringRequest("WARM", "bad-index");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        when(resolver.concreteIndices(any(), any(), eq("bad-index"))).thenThrow(new IllegalArgumentException("Index not found"));

        service.tier(request, listener, clusterState);

        verify(listener).onFailure(any(IllegalArgumentException.class));
    }

    public void testTier_AlreadyBeingTiered_ReturnsCurrentState() throws Exception {
        IndexTieringRequest request = new IndexTieringRequest("WARM", "test-index");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        // Pre-add index so execute() hits the early return
        service.tieringIndices.add(testIndex);

        when(resolver.concreteIndices(any(), any(), eq("test-index"))).thenReturn(new Index[] { testIndex });

        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);

        service.tier(request, listener, clusterState);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        // execute() should return currentState unchanged
        ClusterState result = taskCaptor.getValue().execute(clusterState);
        assertSame(clusterState, result);
    }

    public void testTier_HandlesValidationFailure() {
        IndexTieringRequest request = new IndexTieringRequest("WARM", "test-index");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);

        TestTieringService spyService = spy(
            new TestTieringService(
                Settings.EMPTY,
                clusterService,
                mock(ClusterInfoService.class),
                resolver,
                mock(AllocationService.class),
                nodeEnvironment,
                shardLimitValidator
            )
        );

        when(resolver.concreteIndices(any(), any(), eq("test-index"))).thenReturn(new Index[] { testIndex });

        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(testIndex)).thenReturn(indexMetadata);

        spyService.tier(request, listener, clusterState);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        ClusterStateUpdateTask task = taskCaptor.getValue();
        doThrow(new IllegalArgumentException("Index is already in target tier")).when(spyService)
            .validateTieringRequest(any(), any(), any(), any(), any(), any());

        Exception thrown = expectThrows(IllegalArgumentException.class, () -> task.execute(clusterState));
        assertEquals("Index is already in target tier", thrown.getMessage());

        task.onFailure("test_source", thrown);
        verify(listener).onFailure(thrown);
        assertTrue("Index should not be in tieringIndices", spyService.tieringIndices.isEmpty());
    }

    public void testUpdateIndexMetadataForTieringStart_SetsNumberOfReplicasToOneAndUpdatesRoutingTable() {
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        RoutingTable.Builder routingTableBuilder = mock(RoutingTable.Builder.class);

        tieringService.updateIndexMetadataForTieringStart(metadataBuilder, routingTableBuilder, indexMetadata, testIndex);

        // number_of_replicas=1 is applied atomically via both metadataBuilder and routingTableBuilder
        verify(routingTableBuilder).updateNumberOfReplicas(eq(1), any(String[].class));
        verify(metadataBuilder).updateNumberOfReplicas(eq(1), any(String[].class));

        ArgumentCaptor<IndexMetadata.Builder> captor = ArgumentCaptor.forClass(IndexMetadata.Builder.class);
        verify(metadataBuilder).put(captor.capture());
        IndexMetadata updatedMetadata = captor.getValue().build();
        // Confirm number_of_replicas is set to 1 in metadata settings
        assertEquals("1", updatedMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
    }

    public void testUpdateIndexMetadataPostTiering_UpdatesCorrectly() {
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        when(metadataBuilder.put(any(IndexMetadata.Builder.class))).thenReturn(metadataBuilder);

        tieringService.updateIndexMetadataPostTiering(metadataBuilder, indexMetadata);

        ArgumentCaptor<IndexMetadata.Builder> captor = ArgumentCaptor.forClass(IndexMetadata.Builder.class);
        verify(metadataBuilder).put(captor.capture());

        IndexMetadata updatedMetadata = captor.getValue().build();
        assertEquals(tieringService.getTargetTieringState().toString(), updatedMetadata.getSettings().get(INDEX_TIERING_STATE.getKey()));
        assertNull("Tiering custom metadata should be removed", updatedMetadata.getCustomData(TIERING_CUSTOM_KEY));
    }

    public void testIsIndexBeingTiered() {
        assertFalse(tieringService.isIndexBeingTiered(testIndex));
        tieringService.tieringIndices.add(testIndex);
        assertTrue(tieringService.isIndexBeingTiered(testIndex));
    }

    public void testReconstructInProgressTieringRequests() {
        Settings tieringSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), HOT_TO_WARM.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();

        IndexMetadata tieringIndexMetadata = IndexMetadata.builder("tiering-index")
            .settings(tieringSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        Metadata metadata = Metadata.builder().put(tieringIndexMetadata, false).build();
        when(clusterState.metadata()).thenReturn(metadata);

        tieringService.reconstructInProgressTieringRequests(clusterState, HOT_TO_WARM, "test_source");

        assertEquals(1, tieringService.tieringIndices.size());
    }

    public void testReconstructInProgressTieringRequests_HandlesException() {
        IndexMetadata badMetadata = mock(IndexMetadata.class);
        when(badMetadata.getSettings()).thenThrow(new RuntimeException("simulated error"));
        when(badMetadata.getIndex()).thenReturn(new Index("bad-index", "bad-uuid"));

        Metadata metadata = mock(Metadata.class);
        when(metadata.indices()).thenReturn(Map.of("bad-index", badMetadata));
        when(clusterState.metadata()).thenReturn(metadata);

        // Should not throw — error is caught and logged
        tieringService.reconstructInProgressTieringRequests(clusterState, HOT_TO_WARM, "test_source");

        assertTrue("Bad index should not be added", tieringService.tieringIndices.isEmpty());
    }

    public void testValidateTieringCancelRequest_IndexNotBeingTiered() {
        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable()).thenReturn(routingTable);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> tieringService.validateTieringCancelRequest(testIndex, indexMetadata, clusterState)
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("not currently undergoing tiering operation"));
    }

    public void testValidateTieringCancelRequest_AlreadyReachedTargetTier() {
        tieringService.tieringIndices.add(testIndex);

        Settings warmSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();

        IndexMetadata warmIndexMetadata = IndexMetadata.builder("test-index")
            .settings(warmSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> tieringService.validateTieringCancelRequest(testIndex, warmIndexMetadata, clusterState)
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("already reached its target tier"));
    }

    public void testValidateTieringCancelRequest_IndexDeleted() {
        tieringService.tieringIndices.add(testIndex);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> tieringService.validateTieringCancelRequest(testIndex, indexMetadata, clusterState)
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("deleted before tiering cancellation"));
    }

    /**
     * Failover case: the in-memory tiering set was lost (e.g., cluster-manager change), but the
     * persisted state shows a HOT_TO_WARM migration in progress. Cancel must be accepted (no throw)
     * via the isMigrationInProgress fallback.
     */
    public void testValidateTieringCancelRequest_NotInMemoryButHotToWarm_Accepted() {
        Settings h2wSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), HOT_TO_WARM.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();
        IndexMetadata h2wMetadata = IndexMetadata.builder("test-index").settings(h2wSettings).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);

        assertFalse("Precondition: index must not be tracked in memory", tieringService.isIndexBeingTiered(testIndex));
        // Must NOT throw — the persisted HOT_TO_WARM state makes the index cancellable after failover.
        tieringService.validateTieringCancelRequest(testIndex, h2wMetadata, clusterState);
    }

    /**
     * Direction-agnostic fallback: a WARM_TO_HOT index not tracked in memory is also accepted.
     * (The target tier here is WARM, so the "already reached target" guard does not trip.)
     */
    public void testValidateTieringCancelRequest_NotInMemoryButWarmToHot_Accepted() {
        Settings w2hSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM_TO_HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();
        IndexMetadata w2hMetadata = IndexMetadata.builder("test-index").settings(w2hSettings).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);

        assertFalse("Precondition: index must not be tracked in memory", tieringService.isIndexBeingTiered(testIndex));
        // Must NOT throw.
        tieringService.validateTieringCancelRequest(testIndex, w2hMetadata, clusterState);
    }

    /**
     * Boundary: a terminal WARM index that is NOT tracked in memory must still be rejected —
     * isMigrationInProgress returns false for terminal states, so the fallback does not apply.
     */
    public void testValidateTieringCancelRequest_NotInMemoryAndTerminalWarm_Rejected() {
        Settings warmSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();
        IndexMetadata warmMetadata = IndexMetadata.builder("test-index")
            .settings(warmSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable()).thenReturn(routingTable);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> tieringService.validateTieringCancelRequest(testIndex, warmMetadata, clusterState)
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("not currently undergoing tiering operation"));
    }

    public void testCancelTiering_SubmitsClusterStateTask() {
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AllocationService allocationSvc = mock(AllocationService.class);
        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            allocationSvc,
            nodeEnvironment,
            shardLimitValidator
        );

        when(resolver.concreteIndices(any(), any(), eq("test-index"))).thenReturn(new Index[] { testIndex });

        CancelTieringRequest request = new CancelTieringRequest("test-index");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);

        service.cancelTiering(request, listener, clusterState);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());
        ClusterStateUpdateTask task = taskCaptor.getValue();

        // Cover timeout()
        assertNotNull(task.timeout());

        // Cover onFailure()
        task.onFailure("cancel_source", new RuntimeException("simulated"));
        verify(listener).onFailure(any(RuntimeException.class));

        // Cover clusterStateProcessed()
        service.tieringIndices.add(testIndex);
        task.clusterStateProcessed("cancel_source", clusterState, clusterState);
        assertFalse("Index should be removed after cancel", service.tieringIndices.contains(testIndex));
        verify(listener).onResponse(any(ClusterStateUpdateResponse.class));
    }

    public void testCancelTiering_ExecuteTask() throws Exception {
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AllocationService allocationSvc = mock(AllocationService.class);
        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            allocationSvc,
            nodeEnvironment,
            shardLimitValidator
        );

        service.tieringIndices.add(testIndex);
        when(resolver.concreteIndices(any(), any(), eq("test-index"))).thenReturn(new Index[] { testIndex });

        // Build a real cluster state with the index in HOT_TO_WARM state
        Settings tieringSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT_TO_WARM.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();

        IndexMetadata tieringMeta = IndexMetadata.builder("test-index")
            .settings(tieringSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        Metadata metadata = Metadata.builder().put(tieringMeta, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test-index")).build();
        ClusterState realState = ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        when(allocationSvc.reroute(any(ClusterState.class), anyString())).thenAnswer(inv -> inv.getArgument(0));

        CancelTieringRequest request = new CancelTieringRequest("test-index");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);

        service.cancelTiering(request, listener, realState);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        ClusterState result = taskCaptor.getValue().execute(realState);
        assertNotNull(result);
    }

    public void testCancelTiering_FailsOnResolveException() {
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        when(resolver.concreteIndices(any(), any(), eq("bad-index"))).thenThrow(new IllegalArgumentException("Index not found"));

        CancelTieringRequest request = new CancelTieringRequest("bad-index");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);

        service.cancelTiering(request, listener, clusterState);

        verify(listener).onFailure(any(IllegalArgumentException.class));
    }

    public void testUpdateIndexMetadataForTieringCancel() {
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        when(metadataBuilder.put(any(IndexMetadata.Builder.class))).thenReturn(metadataBuilder);

        tieringService.updateIndexMetadataForTieringCancel(metadataBuilder, indexMetadata);

        ArgumentCaptor<IndexMetadata.Builder> captor = ArgumentCaptor.forClass(IndexMetadata.Builder.class);
        verify(metadataBuilder).put(captor.capture());

        IndexMetadata updatedMetadata = captor.getValue().build();
        assertNull("Tiering custom metadata should be removed", updatedMetadata.getCustomData(TIERING_CUSTOM_KEY));
    }

    public void testUpdateIndexMetadataForTieringStart_ThrowsOnError() {
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        RoutingTable.Builder routingTableBuilder = mock(RoutingTable.Builder.class);
        IndexMetadata badMetadata = mock(IndexMetadata.class);
        when(badMetadata.getSettings()).thenThrow(new RuntimeException("simulated"));

        expectThrows(
            OpenSearchException.class,
            () -> tieringService.updateIndexMetadataForTieringStart(metadataBuilder, routingTableBuilder, badMetadata, testIndex)
        );
    }

    public void testUpdateIndexMetadataPostTiering_ThrowsOnError() {
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        IndexMetadata badMetadata = mock(IndexMetadata.class);
        when(badMetadata.getSettings()).thenThrow(new RuntimeException("simulated"));

        expectThrows(OpenSearchException.class, () -> tieringService.updateIndexMetadataPostTiering(metadataBuilder, badMetadata));
    }

    public void testUpdateIndexMetadataForTieringCancel_ThrowsOnError() {
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        IndexMetadata badMetadata = mock(IndexMetadata.class);
        when(badMetadata.getSettings()).thenThrow(new RuntimeException("simulated"));

        expectThrows(OpenSearchException.class, () -> tieringService.updateIndexMetadataForTieringCancel(metadataBuilder, badMetadata));
    }

    public void testListTieringStatus_Empty() {
        assertEquals(0, tieringService.listTieringStatus().size());
    }

    public void testGetTieringStatus_IndexNotBeingTiered() {
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        when(resolver.concreteIndices(any(), any(), eq("test-index"))).thenReturn(new Index[] { testIndex });
        when(clusterService.state()).thenReturn(clusterState);

        expectThrows(IllegalArgumentException.class, () -> service.getTieringStatus("test-index", false));
    }

    public void testGetTieringStatus_IndexBeingTiered() {
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        service.tieringIndices.add(testIndex);

        // Build index metadata with tiering custom data
        java.util.HashMap<String, String> tieringCustomData = new java.util.HashMap<>();
        tieringCustomData.put("test_tiering_start_time", "1234567");

        Settings idxSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), HOT_TO_WARM.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();

        IndexMetadata idxMeta = IndexMetadata.builder("test-index")
            .settings(idxSettings)
            .putCustom(TIERING_CUSTOM_KEY, tieringCustomData)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        Metadata metadata = mock(Metadata.class);
        when(metadata.getIndexSafe(testIndex)).thenReturn(idxMeta);
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(metadata);
        RoutingTable rt = mock(RoutingTable.class);
        when(state.routingTable()).thenReturn(rt);
        when(rt.allShards("test-index")).thenReturn(Collections.emptyList());
        when(clusterService.state()).thenReturn(state);
        when(resolver.concreteIndices(any(), any(), eq("test-index"))).thenReturn(new Index[] { testIndex });

        TieringStatus status = service.getTieringStatus("test-index", false);
        assertNotNull(status);
        assertEquals("test-index", status.getIndexName());
    }

    public void testListTieringStatus_WithTieringIndex() {
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        service.tieringIndices.add(testIndex);

        java.util.HashMap<String, String> tieringCustomData = new java.util.HashMap<>();
        tieringCustomData.put("test_tiering_start_time", "1234567");

        Settings idxSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), HOT_TO_WARM.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();

        IndexMetadata idxMeta = IndexMetadata.builder("test-index")
            .settings(idxSettings)
            .putCustom(TIERING_CUSTOM_KEY, tieringCustomData)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        Metadata metadata = mock(Metadata.class);
        when(metadata.getIndexSafe(testIndex)).thenReturn(idxMeta);
        ClusterState state = mock(ClusterState.class);
        when(state.getMetadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(state);

        List<TieringStatus> statuses = service.listTieringStatus();
        assertEquals(1, statuses.size());
        assertEquals("test-index", statuses.get(0).getIndexName());
    }

    public void testClusterChanged_NewClusterManager_ReconstructsRequests() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        ClusterState currentState = mock(ClusterState.class);
        ClusterState previousState = mock(ClusterState.class);
        DiscoveryNodes currentNodes = mock(DiscoveryNodes.class);
        DiscoveryNodes previousNodes = mock(DiscoveryNodes.class);
        Metadata metadata = mock(Metadata.class);

        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.state()).thenReturn(currentState);
        when(event.previousState()).thenReturn(previousState);
        when(previousState.nodes()).thenReturn(previousNodes);
        when(previousNodes.isLocalNodeElectedClusterManager()).thenReturn(false);
        when(event.routingTableChanged()).thenReturn(false);
        when(currentState.metadata()).thenReturn(metadata);
        when(metadata.indices()).thenReturn(Map.of());

        tieringService.clusterChanged(event);
        // No exception means reconstruction was called successfully
    }

    public void testClusterChanged_RoutingTableChanged_ProcessesTiering() {
        tieringService.tieringIndices.add(testIndex);

        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        ClusterState currentState = mock(ClusterState.class);
        ClusterState previousState = mock(ClusterState.class);
        DiscoveryNodes previousNodes = mock(DiscoveryNodes.class);
        RoutingTable routingTable = mock(RoutingTable.class);
        Metadata metadata = mock(Metadata.class);
        ShardRouting shard = mock(ShardRouting.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);

        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.state()).thenReturn(currentState);
        when(event.previousState()).thenReturn(previousState);
        when(previousState.nodes()).thenReturn(previousNodes);
        when(previousNodes.isLocalNodeElectedClusterManager()).thenReturn(true);
        when(event.routingTableChanged()).thenReturn(true);

        when(currentState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);
        when(routingTable.allShards(testIndex.getName())).thenReturn(Collections.singletonList(shard));

        // Shard on warm node → in target tier
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");
        when(currentState.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(true);
        when(currentState.metadata()).thenReturn(metadata);
        when(metadata.index(testIndex)).thenReturn(indexMetadata);
        // removeWriteBlockForCancelledDfaIndices iterates over all index metadata;
        // stub the iterator so the enhanced-for loop doesn't NPE on the mock.
        when(metadata.iterator()).thenReturn(Collections.emptyIterator());

        tieringService.clusterChanged(event);

        verify(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testClusterChanged_NotClusterManager_DoesNothing() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeClusterManager()).thenReturn(false);

        tieringService.clusterChanged(event);

        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testUpdateIndexMetadataForTieringStart_ZeroReplicas_SetsNumberOfReplicasToOne() {
        // When user has replicas: 0, number_of_replicas should be forced to 1 for routing table consistency.
        // The ReplicationTracker requires a replica to avoid assertion failures on warm nodes.
        Settings zeroReplicaSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(INDEX_TIERING_STATE.getKey(), "HOT")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid-zero-replicas")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();

        IndexMetadata zeroReplicaMetadata = IndexMetadata.builder("zero-replica-index")
            .settings(zeroReplicaSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        RoutingTable.Builder routingTableBuilder = mock(RoutingTable.Builder.class);

        tieringService.updateIndexMetadataForTieringStart(
            metadataBuilder,
            routingTableBuilder,
            zeroReplicaMetadata,
            new Index("zero-replica-index", "uuid-zero-replicas")
        );

        // Verify number_of_replicas=1 is set atomically in both metadata and routing table
        verify(routingTableBuilder).updateNumberOfReplicas(eq(1), any(String[].class));
        verify(metadataBuilder).updateNumberOfReplicas(eq(1), any(String[].class));

        ArgumentCaptor<IndexMetadata.Builder> captor = ArgumentCaptor.forClass(IndexMetadata.Builder.class);
        verify(metadataBuilder).put(captor.capture());
        IndexMetadata updatedMetadata = captor.getValue().build();
        assertEquals("1", updatedMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
    }

    public void testUpdateIndexMetadataForTieringStart_TwoReplicas_SetsNumberOfReplicasToOne() {
        // When user has replicas: 2, number_of_replicas is forced to 1 during tiering start
        // to ensure routing table consistency with the ReplicationTracker on warm nodes.
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        RoutingTable.Builder routingTableBuilder = mock(RoutingTable.Builder.class);

        tieringService.updateIndexMetadataForTieringStart(metadataBuilder, routingTableBuilder, indexMetadata, testIndex);

        // Verify number_of_replicas=1 is set atomically in both metadata and routing table
        verify(routingTableBuilder).updateNumberOfReplicas(eq(1), any(String[].class));
        verify(metadataBuilder).updateNumberOfReplicas(eq(1), any(String[].class));

        ArgumentCaptor<IndexMetadata.Builder> captor = ArgumentCaptor.forClass(IndexMetadata.Builder.class);
        verify(metadataBuilder).put(captor.capture());
        IndexMetadata updatedMetadata = captor.getValue().build();
        assertEquals("1", updatedMetadata.getSettings().get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
    }

    public void testUpdateIndexMetadataForTieringStart_OneReplica_DoesNotUpdateRoutingTable() {
        // When user already has replicas: 1, updateNumberOfReplicas must NOT be called —
        // the conditional branch `if (currentReplicas != 1)` is not entered.
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        RoutingTable.Builder routingTableBuilder = mock(RoutingTable.Builder.class);

        // Build index with 1 replica (already at target)
        Settings oneReplicaSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put(INDEX_TIERING_STATE.getKey(), "HOT")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata oneReplicaIndex = IndexMetadata.builder("test-index")
            .settings(oneReplicaSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        tieringService.updateIndexMetadataForTieringStart(metadataBuilder, routingTableBuilder, oneReplicaIndex, testIndex);

        // Routing table and metadata must NOT have updateNumberOfReplicas called
        verify(routingTableBuilder, never()).updateNumberOfReplicas(anyInt(), any(String[].class));
        verify(metadataBuilder, never()).updateNumberOfReplicas(anyInt(), any(String[].class));

        // IndexMetadata.Builder must still be put with the tiering settings applied
        verify(metadataBuilder).put(any(IndexMetadata.Builder.class));
    }

    public void testUpdateIndexMetadataPostTiering_DoesNotModifyAutoExpandReplicas() {
        // updateIndexMetadataPostTiering only updates INDEX_TIERING_STATE and removes tiering custom data.
        // It does NOT modify SETTING_AUTO_EXPAND_REPLICAS — that setting retains whatever value
        // was in the original index metadata (null if never explicitly set).
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        when(metadataBuilder.put(any(IndexMetadata.Builder.class))).thenReturn(metadataBuilder);

        tieringService.updateIndexMetadataPostTiering(metadataBuilder, indexMetadata);

        ArgumentCaptor<IndexMetadata.Builder> captor = ArgumentCaptor.forClass(IndexMetadata.Builder.class);
        verify(metadataBuilder).put(captor.capture());

        IndexMetadata updatedMetadata = captor.getValue().build();
        // auto_expand_replicas is not modified by updateIndexMetadataPostTiering —
        // it carries over from the source indexMetadata unchanged.
        assertNull(
            "updateIndexMetadataPostTiering must not set auto_expand_replicas",
            updatedMetadata.getSettings().get(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS)
        );
        // The tiering state must be updated to the target tier
        assertEquals(
            tieringService.getTargetTieringState().toString(),
            updatedMetadata.getSettings().get(org.opensearch.index.IndexModule.INDEX_TIERING_STATE.getKey())
        );
    }

    public void testWarmToHotTieringStart_DisablesAutoExpandReplicas() {
        // Verify that WarmToHotTieringService's getTieringStartSettingsToAdd includes
        // auto_expand_replicas: false. The base class updateIndexMetadataForTieringStart
        // overrides this with "0-{max(1, currentReplicas)}" for the tiering start phase,
        // but the setting is correctly applied in the final tiering completion step.
        TestWarmToHotTieringService warmToHotService = new TestWarmToHotTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            indexNameExpressionResolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        // Pass a DFA index metadata so the W2H subclass returns its full settings
        Settings tieringStartSettings = warmToHotService.getTieringStartSettingsToAdd(buildDfaIndexMetadata("w2h-check-idx", "w2h-uuid"));
        assertEquals(
            "Warm-to-hot getTieringStartSettingsToAdd should include auto_expand_replicas: false",
            "false",
            tieringStartSettings.get(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS)
        );
    }

    public void testWarmToHotTieringStart_RemovesReadOnlyAllowDeleteBlock() {
        // Create a WarmToHot-style tiering service that sets read_only_allow_delete: false on start
        TestWarmToHotTieringService warmToHotService = new TestWarmToHotTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            indexNameExpressionResolver,
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        RoutingTable.Builder routingTableBuilder = mock(RoutingTable.Builder.class);

        warmToHotService.updateIndexMetadataForTieringStart(metadataBuilder, routingTableBuilder, indexMetadata, testIndex);

        ArgumentCaptor<IndexMetadata.Builder> captor = ArgumentCaptor.forClass(IndexMetadata.Builder.class);
        verify(metadataBuilder).put(captor.capture());
        IndexMetadata updatedMetadata = captor.getValue().build();
        assertEquals(
            "Warm-to-hot tiering start should set blocks.write to false",
            "false",
            updatedMetadata.getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey())
        );
    }

    /**
     * Builds a DFA (pluggable dataformat enabled) IndexMetadata for block tests.
     * isDfaIndex() checks IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING = true.
     */
    private IndexMetadata buildDfaIndexMetadata(String indexName, String uuid) {
        Settings dfaSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put(INDEX_TIERING_STATE.getKey(), "HOT")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .build();
        return IndexMetadata.builder(indexName).settings(dfaSettings).numberOfShards(1).numberOfReplicas(1).build();
    }

    /**
     * Test: tier() for a DFA index hot→warm does NOT add write blocks to ClusterBlocks.
     *
     * The write blocks are added by TransportHotToWarmTierAction.addReadOnlyBlockAndPrepare()
     * BEFORE tier() is called. tier() only handles W2H block removal — H2W blocking is owned
     * entirely by TransportHotToWarmTierAction.
     *
     * This test verifies that tier() completes successfully for DFA H2W without touching ClusterBlocks.
     */
    public void testTier_DfaIndex_HotToWarm_DoesNotModifyClusterBlocks() throws Exception {
        String dfaIndexName = "dfa-index";
        String dfaUuid = "dfa-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);
        IndexMetadata dfaMeta = buildDfaIndexMetadata(dfaIndexName, dfaUuid);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AllocationService allocationSvc = mock(AllocationService.class);

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            allocationSvc,
            nodeEnvironment,
            shardLimitValidator
        );

        when(resolver.concreteIndices(any(), any(), eq(dfaIndexName))).thenReturn(new Index[] { dfaIndex });

        Metadata meta = Metadata.builder().put(dfaMeta, false).build();
        RoutingTable rt = RoutingTable.builder().addAsNew(meta.index(dfaIndexName)).build();
        ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(meta)
            .routingTable(rt)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        when(allocationSvc.reroute(any(ClusterState.class), anyString())).thenAnswer(inv -> inv.getArgument(0));

        IndexTieringRequest request = new IndexTieringRequest("WARM", dfaIndexName);
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);

        service.tier(request, listener, initialState);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        ClusterState resultState = taskCaptor.getValue().execute(initialState);

        // tier() does NOT add write blocks for H2W DFA — that's TransportHotToWarmTierAction's responsibility.
        // ClusterBlocks should remain unchanged (empty).
        assertFalse(
            "tier() must NOT add INDEX_WRITE_BLOCK for H2W DFA — TransportHotToWarmTierAction handles that",
            resultState.blocks().hasIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
        assertFalse(
            "tier() must NOT add INDEX_WRITE_BLOCK for H2W DFA — TransportHotToWarmTierAction handles that",
            resultState.blocks().hasIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
    }

    /**
     * Test: When cancelling H2W tiering for a DFA index, the cancelTiering() execute() method must
     * KEEP the INDEX_WRITE_BLOCK in ClusterBlocks and KEEP blocks.write=true in IndexMetadata settings.
     *
     * The write block is intentionally deferred — it must not be lifted during cancel because some
     * shards may still be on warm nodes running a read-only engine. The block is removed later by
     * TieringService.removeWriteBlockForCancelledDfaIndices() once all shards are confirmed started
     * on hot nodes (writable engine). Lifting it immediately would cause write errors on warm-engine shards.
     */
    public void testCancelTiering_DfaIndex_KeepsWriteBlocksDuringCancel() throws Exception {
        String dfaIndexName = "dfa-cancel-index";
        String dfaUuid = "dfa-cancel-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        // Build DFA index metadata in HOT_TO_WARM state WITH write blocks already set
        // (simulates the state after tiering started but before it completed)
        Settings dfaTieringSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT_TO_WARM.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaTieringMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaTieringSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AllocationService allocationSvc = mock(AllocationService.class);

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            allocationSvc,
            nodeEnvironment,
            shardLimitValidator
        );
        service.tieringIndices.add(dfaIndex);

        when(resolver.concreteIndices(any(), any(), eq(dfaIndexName))).thenReturn(new Index[] { dfaIndex });

        // Build cluster state with write blocks already present in ClusterBlocks
        Metadata meta = Metadata.builder().put(dfaTieringMeta, false).build();
        RoutingTable rt = RoutingTable.builder().addAsNew(meta.index(dfaIndexName)).build();
        ClusterBlocks blocksWithWriteBlock = ClusterBlocks.builder()
            .addIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
            .addIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
            .build();
        ClusterState stateWithBlocks = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(meta)
            .routingTable(rt)
            .blocks(blocksWithWriteBlock)
            .build();

        when(allocationSvc.reroute(any(ClusterState.class), anyString())).thenAnswer(inv -> inv.getArgument(0));

        CancelTieringRequest cancelRequest = new CancelTieringRequest(dfaIndexName);
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);

        service.cancelTiering(cancelRequest, listener, stateWithBlocks);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        ClusterState resultState = taskCaptor.getValue().execute(stateWithBlocks);

        // Verify ClusterBlocks: write block must be KEPT (deferred to removeWriteBlockForCancelledDfaIndices)
        assertTrue(
            "INDEX_WRITE_BLOCK must be KEPT in ClusterBlocks during H2W cancel — deferred removal until shards on hot",
            resultState.blocks().hasIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // Verify IndexMetadata settings: blocks.write must remain unchanged (not set to false during cancel)
        // The cancel task only sets IS_WARM=false and TIERING_STATE=HOT — it does not touch blocks.write.
        // The deferred cleanup reads blocks.write=true + TIERING_STATE=HOT to identify orphaned blocks.
        IndexMetadata updatedMeta = resultState.metadata().index(dfaIndexName);
        assertNotEquals(
            "blocks.write must NOT be set to false during cancel — deferred to removeWriteBlockForCancelledDfaIndices()",
            "false",
            updatedMeta.getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey())
        );
    }

    /**
     * Test: When tiering a DFA index warm→hot, the tier() execute() method must REMOVE
     * INDEX_WRITE_BLOCK and INDEX_WRITE_BLOCK from ClusterBlocks AND
     * set blocks.write=false and blocks.read_only_allow_delete=false in IndexMetadata settings.
     *
     * This ensures that after warm→hot migration, the index becomes fully writable again.
     */
    public void testTier_DfaIndex_WarmToHot_RemovesWriteBlocksFromClusterBlocksAndSettings() throws Exception {
        String dfaIndexName = "dfa-w2h-index";
        String dfaUuid = "dfa-w2h-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        // Build warm DFA index metadata WITH write blocks set (it was warm, writes blocked)
        Settings dfaWarmSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM.toString())
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaWarmMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaWarmSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AllocationService allocationSvc = mock(AllocationService.class);

        // Use a W2H-style tiering service (target = HOT)
        TestWarmToHotTieringService w2hService = new TestWarmToHotTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            allocationSvc,
            nodeEnvironment,
            shardLimitValidator
        );

        when(resolver.concreteIndices(any(), any(), eq(dfaIndexName))).thenReturn(new Index[] { dfaIndex });

        // Build cluster state with write blocks already present in ClusterBlocks
        Metadata meta = Metadata.builder().put(dfaWarmMeta, false).build();
        RoutingTable rt = RoutingTable.builder().addAsNew(meta.index(dfaIndexName)).build();
        ClusterBlocks blocksWithWriteBlock = ClusterBlocks.builder()
            .addIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
            .addIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
            .build();
        ClusterState warmState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(meta)
            .routingTable(rt)
            .blocks(blocksWithWriteBlock)
            .build();

        when(allocationSvc.reroute(any(ClusterState.class), anyString())).thenAnswer(inv -> inv.getArgument(0));

        IndexTieringRequest request = new IndexTieringRequest("HOT", dfaIndexName);
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);

        w2hService.tier(request, listener, warmState);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        ClusterState resultState = taskCaptor.getValue().execute(warmState);

        // Verify ClusterBlocks: write blocks must be REMOVED (index becomes writable on hot tier)
        assertFalse(
            "INDEX_WRITE_BLOCK must be REMOVED from ClusterBlocks after W2H tier start for DFA index",
            resultState.blocks().hasIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
        assertFalse(
            "INDEX_WRITE_BLOCK must be REMOVED from ClusterBlocks after W2H tier start for DFA index",
            resultState.blocks().hasIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // Verify IndexMetadata settings: blocks.write=false so index stays writable after restart
        IndexMetadata updatedMeta = resultState.metadata().index(dfaIndexName);
        assertNotEquals(
            "blocks.write must be false in IndexMetadata settings after W2H tier start for DFA index",
            "true",
            updatedMeta.getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey())
        );
        assertNotEquals(
            "blocks.blocks.write must be false in IndexMetadata settings after W2H tier start for DFA index",
            "true",
            updatedMeta.getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey())
        );
    }

    /**
     * Test: Non-DFA index should NOT have write blocks added to ClusterBlocks during tiering.
     * Only DFA (pluggable dataformat) indices get write-blocked during tiering.
     */
    public void testTier_NonDfaIndex_DoesNotAddWriteBlocksToClusterBlocks() throws Exception {
        // non-DFA index: PLUGGABLE_DATAFORMAT_ENABLED_SETTING = false (default)
        Index nonDfaIndex = new Index("non-dfa-index", "non-dfa-uuid");
        Settings nonDfaSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put(INDEX_TIERING_STATE.getKey(), "HOT")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "non-dfa-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            // PLUGGABLE_DATAFORMAT_ENABLED_SETTING not set → defaults to false
            .build();
        IndexMetadata nonDfaMeta = IndexMetadata.builder("non-dfa-index")
            .settings(nonDfaSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AllocationService allocationSvc = mock(AllocationService.class);

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            allocationSvc,
            nodeEnvironment,
            shardLimitValidator
        );

        when(resolver.concreteIndices(any(), any(), eq("non-dfa-index"))).thenReturn(new Index[] { nonDfaIndex });

        Metadata meta = Metadata.builder().put(nonDfaMeta, false).build();
        RoutingTable rt = RoutingTable.builder().addAsNew(meta.index("non-dfa-index")).build();
        ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(meta)
            .routingTable(rt)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        when(allocationSvc.reroute(any(ClusterState.class), anyString())).thenAnswer(inv -> inv.getArgument(0));

        IndexTieringRequest request = new IndexTieringRequest("WARM", "non-dfa-index");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);

        service.tier(request, listener, initialState);

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(anyString(), taskCaptor.capture());

        ClusterState resultState = taskCaptor.getValue().execute(initialState);

        // Non-DFA index should NOT have write blocks added
        assertFalse(
            "INDEX_WRITE_BLOCK must NOT be added for non-DFA index",
            resultState.blocks().hasIndexBlock("non-dfa-index", IndexMetadata.INDEX_WRITE_BLOCK)
        );
        assertFalse(
            "INDEX_WRITE_BLOCK must NOT be added for non-DFA index",
            resultState.blocks().hasIndexBlock("non-dfa-index", IndexMetadata.INDEX_WRITE_BLOCK)
        );
    }

    // ── removeWriteBlockForCancelledDfaIndices tests ──────────────────────────
    //
    // The method is private and triggered via clusterChanged() when
    // routingTableChanged() || metadataChanged() is true.
    // We trigger it by building a ClusterChangedEvent and calling clusterChanged().

    /**
     * Happy path: DFA index in HOT state with write block, all shards started on hot nodes.
     * Expects a cluster state task to be submitted to remove the write block.
     */
    public void testRemoveWriteBlock_DfaHotIndexWithBlock_AllShardsOnHot_SubmitsTask() throws Exception {
        String dfaIndexName = "dfa-cleanup-index";
        String dfaUuid = "dfa-cleanup-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        // DFA index in HOT state with write block still set (orphaned from H2W cancel)
        Settings dfaHotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        AllocationService allocationSvc = mock(AllocationService.class);
        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            resolver,
            allocationSvc,
            nodeEnvironment,
            shardLimitValidator
        );
        // index is NOT in tieringIndices (cancel completed)

        // Build cluster state: shard started on a hot node
        Metadata meta = Metadata.builder().put(dfaHotMeta, false).build();
        RoutingTable rt = RoutingTable.builder().addAsNew(meta.index(dfaIndexName)).build();
        ClusterBlocks blocks = ClusterBlocks.builder().addIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK).build();

        ShardRouting shard = mock(ShardRouting.class);
        DiscoveryNode hotNode = mock(DiscoveryNode.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("hot1");
        when(hotNode.isWarmNode()).thenReturn(false);
        when(nodes.get("hot1")).thenReturn(hotNode);

        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable rtMock = mock(RoutingTable.class);
        when(clusterState.metadata()).thenReturn(meta);
        when(clusterState.routingTable()).thenReturn(rtMock);
        when(clusterState.blocks()).thenReturn(blocks);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(rtMock.hasIndex(dfaIndex)).thenReturn(true);
        when(rtMock.allShards(dfaIndexName)).thenReturn(Collections.singletonList(shard));

        // Trigger via clusterChanged
        ClusterChangedEvent event = buildRoutingTableChangedEvent(clusterState);
        service.clusterChanged(event);

        // A task must have been submitted to remove the write block
        verify(clusterService, org.mockito.Mockito.atLeastOnce()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    /**
     * Shards are still relocating (not all started on hot) — no task submitted, block kept.
     */
    public void testRemoveWriteBlock_DfaHotIndexWithBlock_ShardsStillRelocating_NoTask() {
        String dfaIndexName = "dfa-relocating";
        String dfaUuid = "dfa-relocating-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        Settings dfaHotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        Metadata meta = Metadata.builder().put(dfaHotMeta, false).build();
        ShardRouting shard = mock(ShardRouting.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        // Shard is initializing (not started) — relocation not complete
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(false);

        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable rtMock = mock(RoutingTable.class);
        when(clusterState.metadata()).thenReturn(meta);
        when(clusterState.routingTable()).thenReturn(rtMock);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(rtMock.hasIndex(dfaIndex)).thenReturn(true);
        when(rtMock.allShards(dfaIndexName)).thenReturn(Collections.singletonList(shard));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(clusterState);
        service.clusterChanged(event);

        // No task submitted — shards not yet all on hot
        verify(clusterService, never()).submitStateUpdateTask(org.mockito.Mockito.contains("remove-write-block"), any());
    }

    /**
     * Replica shard is unassigned (e.g. no available node) while the primary is started on a hot node.
     * The condition {@code s.unassigned() && !s.primary()} must treat the unassigned replica as
     * "acceptable" so that {@code allOnHot=true} and the write-block removal task IS submitted.
     *
     * <p>This is the canonical scenario after H2W cancel with auto_expand_replicas: the primary
     * snaps back to hot quickly but one replica may remain unassigned until a node becomes available.
     * We must not block write-block removal waiting for those replicas.
     */
    public void testRemoveWriteBlock_UnassignedReplica_PrimaryOnHot_SubmitsTask() {
        String dfaIndexName = "dfa-unassigned-replica";
        String dfaUuid = "dfa-unassigned-replica-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        Settings dfaHotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );
        // index is NOT in tieringIndices (cancel completed)

        Metadata meta = Metadata.builder().put(dfaHotMeta, false).build();

        // Primary shard: started on a hot node
        ShardRouting primaryShard = mock(ShardRouting.class);
        when(primaryShard.unassigned()).thenReturn(false);
        when(primaryShard.started()).thenReturn(true);
        when(primaryShard.primary()).thenReturn(true);
        when(primaryShard.currentNodeId()).thenReturn("hot1");

        // Replica shard: unassigned and NOT primary — satisfies s.unassigned() && !s.primary()
        ShardRouting replicaShard = mock(ShardRouting.class);
        when(replicaShard.unassigned()).thenReturn(true);
        when(replicaShard.primary()).thenReturn(false);

        DiscoveryNode hotNode = mock(DiscoveryNode.class);
        when(hotNode.isWarmNode()).thenReturn(false);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.get("hot1")).thenReturn(hotNode);

        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable rtMock = mock(RoutingTable.class);
        when(clusterState.metadata()).thenReturn(meta);
        when(clusterState.routingTable()).thenReturn(rtMock);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(rtMock.hasIndex(dfaIndex)).thenReturn(true);
        when(rtMock.allShards(dfaIndexName)).thenReturn(java.util.Arrays.asList(primaryShard, replicaShard));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(clusterState);
        service.clusterChanged(event);

        // allOnHot=true (primary on hot + replica unassigned-non-primary) → task must be submitted
        verify(clusterService, org.mockito.Mockito.atLeastOnce()).submitStateUpdateTask(
            org.mockito.Mockito.contains("remove-write-block"),
            any(ClusterStateUpdateTask.class)
        );
    }

    /**
     * Primary shard itself is unassigned — {@code s.unassigned() && !s.primary()} is false
     * (primary IS a primary), and {@code s.started()} is also false.
     * Therefore {@code allOnHot=false} and no write-block removal task must be submitted.
     *
     * <p>This is the negative complement of the unassigned-replica case: we must NOT lift
     * the write block when the primary is not yet running on a hot node.
     */
    public void testRemoveWriteBlock_UnassignedPrimary_DoesNotSubmitTask() {
        String dfaIndexName = "dfa-unassigned-primary";
        String dfaUuid = "dfa-unassigned-primary-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        Settings dfaHotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        Metadata meta = Metadata.builder().put(dfaHotMeta, false).build();

        // Primary shard is unassigned — neither branch of the allMatch predicate is satisfied:
        // s.unassigned() && !s.primary() → false (it IS the primary)
        // s.started() && ... → false (not started)
        ShardRouting primaryShard = mock(ShardRouting.class);
        when(primaryShard.unassigned()).thenReturn(true);
        when(primaryShard.primary()).thenReturn(true);
        when(primaryShard.started()).thenReturn(false);

        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable rtMock = mock(RoutingTable.class);
        when(clusterState.metadata()).thenReturn(meta);
        when(clusterState.routingTable()).thenReturn(rtMock);
        when(clusterState.getNodes()).thenReturn(mock(DiscoveryNodes.class));
        when(rtMock.hasIndex(dfaIndex)).thenReturn(true);
        when(rtMock.allShards(dfaIndexName)).thenReturn(Collections.singletonList(primaryShard));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(clusterState);
        service.clusterChanged(event);

        // allOnHot=false → no task submitted
        verify(clusterService, never()).submitStateUpdateTask(org.mockito.Mockito.contains("remove-write-block"), any());
    }

    /**
     * Non-DFA index with write block in HOT state — must be completely ignored.
     */
    public void testRemoveWriteBlock_NonDfaIndex_Skipped() {
        String indexName = "non-dfa-hot";
        String uuid = "non-dfa-hot-uuid";
        Index idx = new Index(indexName, uuid);

        Settings hotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            // PLUGGABLE_DATAFORMAT_ENABLED_SETTING not set → non-DFA
            .build();
        IndexMetadata meta = IndexMetadata.builder(indexName).settings(hotSettings).numberOfShards(1).numberOfReplicas(0).build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        Metadata clusterMeta = Metadata.builder().put(meta, false).build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(clusterMeta);
        when(clusterState.routingTable()).thenReturn(mock(RoutingTable.class));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(clusterState);
        service.clusterChanged(event);

        verify(clusterService, never()).submitStateUpdateTask(org.mockito.Mockito.contains("remove-write-block"), any());
    }

    /**
     * DFA index in HOT state with write block, but it's still in tieringIndices
     * (actively tiering) — must be skipped.
     */
    public void testRemoveWriteBlock_IndexStillInTieringIndices_Skipped() {
        String dfaIndexName = "dfa-still-tiering";
        String dfaUuid = "dfa-still-tiering-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        Settings dfaHotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );
        // Index IS in tieringIndices — still tiering, must not be cleaned up
        service.tieringIndices.add(dfaIndex);

        Metadata meta = Metadata.builder().put(dfaHotMeta, false).build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(meta);
        when(clusterState.routingTable()).thenReturn(mock(RoutingTable.class));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(clusterState);
        service.clusterChanged(event);

        verify(clusterService, never()).submitStateUpdateTask(org.mockito.Mockito.contains("remove-write-block"), any());
    }

    /**
     * DFA index in HOT state but write block already removed — no task submitted.
     */
    public void testRemoveWriteBlock_DfaHotIndex_NoWriteBlock_Skipped() {
        String dfaIndexName = "dfa-hot-no-block";
        String dfaUuid = "dfa-hot-no-block-uuid";

        Settings dfaHotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            // INDEX_BLOCKS_WRITE not set → false (block already removed)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        Metadata meta = Metadata.builder().put(dfaHotMeta, false).build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(meta);
        when(clusterState.routingTable()).thenReturn(mock(RoutingTable.class));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(clusterState);
        service.clusterChanged(event);

        verify(clusterService, never()).submitStateUpdateTask(org.mockito.Mockito.contains("remove-write-block"), any());
    }

    /**
     * Happy-path execute(): task actually removes INDEX_WRITE_BLOCK from ClusterBlocks
     * and sets INDEX_BLOCKS_WRITE=false in IndexMetadata settings.
     */
    public void testRemoveWriteBlock_Execute_RemovesBlockAndUpdatesMetadata() throws Exception {
        String dfaIndexName = "dfa-execute-index";
        String dfaUuid = "dfa-execute-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        Settings dfaHotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Metadata meta = Metadata.builder().put(dfaHotMeta, false).build();
        RoutingTable rt = RoutingTable.builder().addAsNew(meta.index(dfaIndexName)).build();
        ClusterBlocks blocks = ClusterBlocks.builder().addIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK).build();
        ClusterState realState = ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(meta)
            .routingTable(rt)
            .blocks(blocks)
            .build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        // Capture the submitted task so we can execute it
        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);

        ShardRouting shard = mock(ShardRouting.class);
        DiscoveryNode hotNode = mock(DiscoveryNode.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("hot1");
        when(hotNode.isWarmNode()).thenReturn(false);
        when(nodes.get("hot1")).thenReturn(hotNode);

        ClusterState mockState = mock(ClusterState.class);
        RoutingTable rtMock = mock(RoutingTable.class);
        when(mockState.metadata()).thenReturn(meta);
        when(mockState.routingTable()).thenReturn(rtMock);
        when(mockState.blocks()).thenReturn(blocks);
        when(mockState.getNodes()).thenReturn(nodes);
        when(rtMock.hasIndex(dfaIndex)).thenReturn(true);
        when(rtMock.allShards(dfaIndexName)).thenReturn(Collections.singletonList(shard));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(mockState);
        service.clusterChanged(event);

        verify(clusterService, org.mockito.Mockito.atLeastOnce()).submitStateUpdateTask(anyString(), taskCaptor.capture());

        // Execute the captured task against a real cluster state that still has the block
        ClusterState result = taskCaptor.getValue().execute(realState);

        // INDEX_WRITE_BLOCK must be removed from ClusterBlocks
        assertFalse(
            "INDEX_WRITE_BLOCK must be absent from ClusterBlocks after execute()",
            result.blocks().hasIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
        // INDEX_BLOCKS_WRITE setting must be false in IndexMetadata
        assertEquals("false", result.metadata().index(dfaIndexName).getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()));
    }

    /**
     * TOCTOU guard inside execute(): index condition changed between scan and task execution
     * (write block already removed by another task) — execute() must skip and produce no change.
     */
    public void testRemoveWriteBlock_Execute_ToctouGuard_SkipsIfBlockAlreadyRemoved() throws Exception {
        String dfaIndexName = "dfa-toctou-index";
        String dfaUuid = "dfa-toctou-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        // Settings without write block (already removed before execute runs)
        Settings dfaHotSettingsNoBlock = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            // INDEX_BLOCKS_WRITE not set = false (block already removed)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettingsNoBlock)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Settings WITH block for the initial scan (so the task gets submitted)
        Settings dfaHotSettingsWithBlock = Settings.builder()
            .put(dfaHotSettingsNoBlock)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaHotMetaWithBlock = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettingsWithBlock)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Scan state has the block (triggers task submission)
        Metadata scanMeta = Metadata.builder().put(dfaHotMetaWithBlock, false).build();
        ClusterBlocks blocksWithBlock = ClusterBlocks.builder().addIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK).build();

        // Execute state: block already gone (TOCTOU)
        Metadata executeMeta = Metadata.builder().put(dfaHotMeta, false).build();
        RoutingTable rt = RoutingTable.builder().addAsNew(executeMeta.index(dfaIndexName)).build();
        ClusterState executeState = ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(executeMeta)
            .routingTable(rt)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);

        ShardRouting shard = mock(ShardRouting.class);
        DiscoveryNode hotNode = mock(DiscoveryNode.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("hot1");
        when(hotNode.isWarmNode()).thenReturn(false);
        when(nodes.get("hot1")).thenReturn(hotNode);

        ClusterState mockScanState = mock(ClusterState.class);
        RoutingTable rtMock = mock(RoutingTable.class);
        when(mockScanState.metadata()).thenReturn(scanMeta);
        when(mockScanState.routingTable()).thenReturn(rtMock);
        when(mockScanState.blocks()).thenReturn(blocksWithBlock);
        when(mockScanState.getNodes()).thenReturn(nodes);
        when(rtMock.hasIndex(dfaIndex)).thenReturn(true);
        when(rtMock.allShards(dfaIndexName)).thenReturn(Collections.singletonList(shard));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(mockScanState);
        service.clusterChanged(event);

        verify(clusterService, org.mockito.Mockito.atLeastOnce()).submitStateUpdateTask(anyString(), taskCaptor.capture());

        // Execute with the state where block is already gone (TOCTOU scenario)
        ClusterState result = taskCaptor.getValue().execute(executeState);

        // Result must equal the input state unchanged (no index block was found to remove)
        assertFalse(
            "No INDEX_WRITE_BLOCK should be present — TOCTOU guard must have skipped this index",
            result.blocks().hasIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
        // INDEX_BLOCKS_WRITE must remain null/false (was never set by the task)
        assertNotEquals("true", result.metadata().index(dfaIndexName).getSettings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()));
    }

    /**
     * Null guard inside execute(): index deleted between scan and task execution —
     * execute() must skip gracefully and not throw NPE.
     */
    public void testRemoveWriteBlock_Execute_NullGuard_IndexDeletedBetweenScanAndTask() throws Exception {
        String dfaIndexName = "dfa-deleted-index";
        String dfaUuid = "dfa-deleted-uuid";
        Index dfaIndex = new Index(dfaIndexName, dfaUuid);

        Settings dfaHotSettings = Settings.builder()
            .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, dfaUuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true)
            .build();
        IndexMetadata dfaHotMeta = IndexMetadata.builder(dfaIndexName)
            .settings(dfaHotSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Scan state: index exists with write block
        Metadata scanMeta = Metadata.builder().put(dfaHotMeta, false).build();
        ClusterBlocks blocksWithBlock = ClusterBlocks.builder().addIndexBlock(dfaIndexName, IndexMetadata.INDEX_WRITE_BLOCK).build();

        // Execute state: index has been deleted
        ClusterState executeState = ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(Metadata.EMPTY_METADATA)
            .routingTable(RoutingTable.EMPTY_ROUTING_TABLE)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        TestTieringService service = new TestTieringService(
            Settings.EMPTY,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            shardLimitValidator
        );

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);

        ShardRouting shard = mock(ShardRouting.class);
        DiscoveryNode hotNode = mock(DiscoveryNode.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("hot1");
        when(hotNode.isWarmNode()).thenReturn(false);
        when(nodes.get("hot1")).thenReturn(hotNode);

        ClusterState mockScanState = mock(ClusterState.class);
        RoutingTable rtMock = mock(RoutingTable.class);
        when(mockScanState.metadata()).thenReturn(scanMeta);
        when(mockScanState.routingTable()).thenReturn(rtMock);
        when(mockScanState.blocks()).thenReturn(blocksWithBlock);
        when(mockScanState.getNodes()).thenReturn(nodes);
        when(rtMock.hasIndex(dfaIndex)).thenReturn(true);
        when(rtMock.allShards(dfaIndexName)).thenReturn(Collections.singletonList(shard));

        ClusterChangedEvent event = buildRoutingTableChangedEvent(mockScanState);
        service.clusterChanged(event);

        verify(clusterService, org.mockito.Mockito.atLeastOnce()).submitStateUpdateTask(anyString(), taskCaptor.capture());

        // execute() with deleted-index state must not throw
        ClusterState result = taskCaptor.getValue().execute(executeState);
        assertNotNull("execute() must return a non-null state even when index was deleted", result);
    }

    /**
     * Helper: builds a ClusterChangedEvent where routingTableChanged()=true and
     * localNodeClusterManager()=true but previousNodes.isLocalNodeElectedClusterManager()=true
     * (so reconstruction is skipped and only the cleanup path runs).
     */
    private ClusterChangedEvent buildRoutingTableChangedEvent(ClusterState currentState) {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        ClusterState previousState = mock(ClusterState.class);
        DiscoveryNodes previousNodes = mock(DiscoveryNodes.class);

        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.state()).thenReturn(currentState);
        when(event.previousState()).thenReturn(previousState);
        when(previousState.nodes()).thenReturn(previousNodes);
        when(previousNodes.isLocalNodeElectedClusterManager()).thenReturn(true);
        when(event.routingTableChanged()).thenReturn(true);
        when(event.metadataChanged()).thenReturn(false);
        when(event.blocksChanged()).thenReturn(false);
        return event;
    }

    /**
     * A test tiering service that mimics WarmToHotTieringService behavior.
     * Sets auto_expand_replicas: false and read_only_allow_delete: false on tiering start.
     */
    private class TestWarmToHotTieringService extends TieringService {
        public TestWarmToHotTieringService(
            Settings settings,
            ClusterService clusterService,
            ClusterInfoService clusterInfoService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            AllocationService allocationService,
            NodeEnvironment nodeEnvironment,
            ShardLimitValidator shardLimitValidator
        ) {
            super(
                settings,
                clusterService,
                clusterInfoService,
                indexNameExpressionResolver,
                allocationService,
                nodeEnvironment,
                shardLimitValidator
            );
        }

        @Override
        protected Settings getTieringStartSettingsToAdd(IndexMetadata indexMetadata) {
            return Settings.builder()
                .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false)
                .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM_TO_HOT)
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false)
                .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false)
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "false")
                .build();
        }

        @Override
        protected org.opensearch.cluster.block.ClusterBlocks.Builder getTieringStartClusterBlocksToAdd(
            org.opensearch.cluster.block.ClusterBlocks.Builder blocksBuilder,
            String indexName,
            IndexMetadata indexMetadata
        ) {
            return blocksBuilder.removeIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
                .removeIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);
        }

        @Override
        protected Settings getIndexTierSettingsToRestoreAfterCancellation(IndexMetadata indexMetadata) {
            return Settings.builder()
                .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
                .put(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM)
                .build();
        }

        @Override
        protected String getTieringStartTimeKey() {
            return "w2h_tiering_start_time";
        }

        @Override
        protected Setting<Integer> getMaxConcurrentTieringRequestsSetting() {
            return maxConcurrentTieringRequestsSetting;
        }

        @Override
        protected IndexModule.TieringState getTargetTieringState() {
            return IndexModule.TieringState.HOT;
        }

        @Override
        protected IndexModule.TieringState getTieringType() {
            return IndexModule.TieringState.WARM_TO_HOT;
        }

        @Override
        protected void validateTieringRequest(
            ClusterState clusterState,
            ClusterInfoService clusterInfoService,
            Set<Index> tieringEntries,
            Integer maxConcurrentTieringRequests,
            Integer jvmActiveUsageThresholdPercent,
            Index index
        ) {}
    }
}
