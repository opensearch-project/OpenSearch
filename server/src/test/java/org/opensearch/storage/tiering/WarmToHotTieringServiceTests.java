/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule.TieringState;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.index.IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;
import static org.opensearch.index.IndexModule.TieringState.WARM_TO_HOT;
import static org.opensearch.index.store.remote.filecache.FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.opensearch.storage.common.tiering.TieringUtils.JVM_USAGE_TIERING_THRESHOLD_PERCENT;
import static org.opensearch.storage.common.tiering.TieringUtils.TIERED_COMPOSITE_INDEX_TYPE;
import static org.opensearch.storage.common.tiering.TieringUtils.W2H_MAX_CONCURRENT_TIERING_REQUESTS;
import static org.opensearch.storage.common.tiering.TieringUtils.W2H_MAX_CONCURRENT_TIERING_REQUESTS_KEY;
import static org.opensearch.storage.common.tiering.TieringUtils.W2H_TIERING_START_TIME_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarmToHotTieringServiceTests extends OpenSearchTestCase {

    private WarmToHotTieringService service;
    private ClusterService clusterService;
    private ClusterState clusterState;
    private Index testIndex;
    private IndexMetadata indexMetadata;
    private Settings indexSettings;
    private NodeEnvironment nodeEnvironment;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        nodeEnvironment = newNodeEnvironment();

        testIndex = new Index("test-index", "uuid");
        indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(1).build();

        Settings defaultSettings = Settings.builder()
            .put(W2H_MAX_CONCURRENT_TIERING_REQUESTS.getKey(), 50)
            .put(JVM_USAGE_TIERING_THRESHOLD_PERCENT.getKey(), 99)
            .build();

        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(W2H_MAX_CONCURRENT_TIERING_REQUESTS);
        clusterSettingsToAdd.add(JVM_USAGE_TIERING_THRESHOLD_PERCENT);
        clusterSettingsToAdd.add(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING);

        ClusterSettings mockSettings = new ClusterSettings(defaultSettings, clusterSettingsToAdd);
        when(clusterService.getClusterSettings()).thenReturn(mockSettings);

        service = new WarmToHotTieringService(
            defaultSettings,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            mock(ShardLimitValidator.class)
        );
    }

    @Override
    public void tearDown() throws Exception {
        IOUtils.close(nodeEnvironment);
        super.tearDown();
    }

    public void testGetTieringStartSettingsToAdd() {
        Settings settings = service.getTieringStartSettingsToAdd();
        assertEquals("false", settings.get(IS_WARM_INDEX_SETTING.getKey()));
        assertEquals(WARM_TO_HOT.toString(), settings.get(INDEX_TIERING_STATE.getKey()));
        assertEquals("default", settings.get(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey()));
    }

    public void testGetIndexTierSettingsToRestoreAfterCancellation() {
        Settings settings = service.getIndexTierSettingsToRestoreAfterCancellation();
        assertEquals("true", settings.get(IS_WARM_INDEX_SETTING.getKey()));
        assertEquals(TieringState.WARM.toString(), settings.get(INDEX_TIERING_STATE.getKey()));
        assertEquals(TIERED_COMPOSITE_INDEX_TYPE, settings.get(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey()));
    }

    public void testGetTieringStartTimeKey() {
        assertEquals(W2H_TIERING_START_TIME_KEY, service.getTieringStartTimeKey());
    }

    public void testGetMaxConcurrentTieringRequestsSetting() {
        assertEquals(W2H_MAX_CONCURRENT_TIERING_REQUESTS_KEY, service.getMaxConcurrentTieringRequestsSetting().getKey());
    }

    public void testGetTargetTieringState() {
        assertEquals(TieringState.HOT, service.getTargetTieringState());
    }

    public void testGetTieringType() {
        assertEquals(WARM_TO_HOT, service.getTieringType());
    }

    public void testIsShardInTargetTier() {
        ShardRouting shard = mock(ShardRouting.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(false);

        assertTrue(service.isShardInTargetTier(shard, clusterState));
    }

    public void testClusterChanged_WithRoutingTableChange_IndexDeleted() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        RoutingTable routingTable = mock(RoutingTable.class);
        ClusterState previousState = mock(ClusterState.class);
        DiscoveryNodes previousNodes = mock(DiscoveryNodes.class);

        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.previousState()).thenReturn(previousState);
        when(previousState.nodes()).thenReturn(previousNodes);
        when(previousNodes.isLocalNodeElectedClusterManager()).thenReturn(true);
        when(event.routingTableChanged()).thenReturn(true);
        when(event.state()).thenReturn(clusterState);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(false);

        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);

        service.tieringIndices.add(testIndex);
        service.clusterChanged(event);

        assertTrue(service.tieringIndices.isEmpty());
    }

    public void testClusterChanged_WithRoutingTableChange_ShardOnHotNode() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        RoutingTable routingTable = mock(RoutingTable.class);
        ClusterState previousState = mock(ClusterState.class);
        DiscoveryNodes previousNodes = mock(DiscoveryNodes.class);

        ShardRouting shard = createShardRouting(true, false, "node1", true);
        List<ShardRouting> shardRoutings = Collections.singletonList(shard);

        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.previousState()).thenReturn(previousState);
        when(previousState.nodes()).thenReturn(previousNodes);
        when(previousNodes.isLocalNodeElectedClusterManager()).thenReturn(true);
        when(event.routingTableChanged()).thenReturn(true);
        when(event.state()).thenReturn(clusterState);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);
        when(routingTable.allShards(testIndex.getName())).thenReturn(shardRoutings);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(false);

        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(testIndex)).thenReturn(indexMetadata);

        service.tieringIndices.add(testIndex);
        service.clusterChanged(event);

        verify(routingTable).allShards(testIndex.getName());
    }

    public void testClusterChanged_NonMasterNode() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeClusterManager()).thenReturn(false);

        service.clusterChanged(event);

        verify(event).localNodeClusterManager();
        verify(event, never()).previousState();
    }

    public void testClusterChanged_NewMaster() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        ClusterState previousState = mock(ClusterState.class);
        DiscoveryNodes previousNodes = mock(DiscoveryNodes.class);
        Metadata metadata = mock(Metadata.class);

        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.previousState()).thenReturn(previousState);
        when(previousState.nodes()).thenReturn(previousNodes);
        when(previousNodes.isLocalNodeElectedClusterManager()).thenReturn(false);
        when(event.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);

        service.clusterChanged(event);

        verify(previousNodes).isLocalNodeElectedClusterManager();
    }

    public void testReconstructInProgressTiering_WarmToHot() {
        IndexMetadata index1 = createIndexMetadata("index1", WARM_TO_HOT.toString());
        IndexMetadata index2 = createIndexMetadata("index2", WARM_TO_HOT.toString());
        IndexMetadata index3 = createIndexMetadata("index3", "other_state");

        Metadata metadata = Metadata.builder().put(index1, false).put(index2, false).put(index3, false).build();
        when(clusterState.metadata()).thenReturn(metadata);

        service.reconstructInProgressTieringRequests(clusterState, WARM_TO_HOT, "warm");

        assertEquals(2, service.tieringIndices.size());
        assertTrue(service.tieringIndices.contains(index1.getIndex()));
        assertTrue(service.tieringIndices.contains(index2.getIndex()));
        assertFalse(service.tieringIndices.contains(index3.getIndex()));
    }

    public void testProcessTieringInProgress_WithUnassignedReplica() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        RoutingTable routingTable = mock(RoutingTable.class);
        ClusterState previousState = mock(ClusterState.class);
        DiscoveryNodes previousNodes = mock(DiscoveryNodes.class);

        List<ShardRouting> shardRoutings = Arrays.asList(
            createShardRouting(true, false, "node1", true),
            createShardRouting(false, true, null, false)
        );

        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.previousState()).thenReturn(previousState);
        when(previousState.nodes()).thenReturn(previousNodes);
        when(previousNodes.isLocalNodeElectedClusterManager()).thenReturn(true);
        when(event.routingTableChanged()).thenReturn(true);
        when(event.state()).thenReturn(clusterState);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(testIndex)).thenReturn(true);
        when(routingTable.allShards(testIndex.getName())).thenReturn(shardRoutings);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(false);

        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(testIndex)).thenReturn(indexMetadata);

        service.tieringIndices.add(testIndex);
        service.clusterChanged(event);

        verify(routingTable).allShards(testIndex.getName());
    }

    private ShardRouting createShardRouting(boolean primary, boolean unassigned, String nodeId, boolean started) {
        ShardRouting shard = mock(ShardRouting.class);
        when(shard.primary()).thenReturn(primary);
        when(shard.unassigned()).thenReturn(unassigned);
        when(shard.currentNodeId()).thenReturn(nodeId);
        when(shard.started()).thenReturn(started);
        return shard;
    }

    private IndexMetadata createIndexMetadata(String name, String tieringState) {
        Settings settings = Settings.builder().put(indexSettings).put(INDEX_TIERING_STATE.getKey(), tieringState).build();
        return IndexMetadata.builder(name).settings(settings).numberOfShards(1).numberOfReplicas(1).build();
    }
}
