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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
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
        protected Settings getTieringStartSettingsToAdd() {
            return Settings.builder()
                .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
                .put(INDEX_TIERING_STATE.getKey(), HOT_TO_WARM)
                .build();
        }

        @Override
        protected Settings getIndexTierSettingsToRestoreAfterCancellation() {
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

    public void testUpdateIndexMetadataForTieringStart_HandlesReplicaReduction() {
        Metadata.Builder metadataBuilder = mock(Metadata.Builder.class);
        RoutingTable.Builder routingTableBuilder = mock(RoutingTable.Builder.class);

        tieringService.updateIndexMetadataForTieringStart(metadataBuilder, routingTableBuilder, indexMetadata, testIndex);

        verify(routingTableBuilder).updateNumberOfReplicas(eq(1), any(String[].class));
        verify(metadataBuilder).updateNumberOfReplicas(eq(1), any(String[].class));
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

        tieringService.clusterChanged(event);

        verify(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testClusterChanged_NotClusterManager_DoesNothing() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeClusterManager()).thenReturn(false);

        tieringService.clusterChanged(event);

        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }
}
