/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.index.IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;
import static org.opensearch.index.IndexModule.TieringState;
import static org.opensearch.index.IndexModule.TieringState.HOT_TO_WARM;
import static org.opensearch.index.store.remote.filecache.FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.opensearch.storage.common.tiering.TieringUtils.FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT;
import static org.opensearch.storage.common.tiering.TieringUtils.H2W_MAX_CONCURRENT_TIERING_REQUESTS;
import static org.opensearch.storage.common.tiering.TieringUtils.H2W_MAX_CONCURRENT_TIERING_REQUESTS_KEY;
import static org.opensearch.storage.common.tiering.TieringUtils.H2W_TIERING_START_TIME_KEY;
import static org.opensearch.storage.common.tiering.TieringUtils.JVM_USAGE_TIERING_THRESHOLD_PERCENT;
import static org.opensearch.storage.common.tiering.TieringUtils.TIERED_COMPOSITE_INDEX_TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HotToWarmTieringServiceTests extends OpenSearchTestCase {

    private HotToWarmTieringService service;
    private ClusterService clusterService;
    private ClusterState clusterState;
    private NodeEnvironment nodeEnvironment;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        nodeEnvironment = newNodeEnvironment();

        Settings defaultSettings = Settings.builder()
            .put(H2W_MAX_CONCURRENT_TIERING_REQUESTS.getKey(), 50)
            .put(JVM_USAGE_TIERING_THRESHOLD_PERCENT.getKey(), 99)
            .put(FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT.getKey(), 90)
            .build();

        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(H2W_MAX_CONCURRENT_TIERING_REQUESTS);
        clusterSettingsToAdd.add(JVM_USAGE_TIERING_THRESHOLD_PERCENT);
        clusterSettingsToAdd.add(FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT);
        clusterSettingsToAdd.add(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING);

        ClusterSettings mockSettings = new ClusterSettings(defaultSettings, clusterSettingsToAdd);
        when(clusterService.getClusterSettings()).thenReturn(mockSettings);

        service = new HotToWarmTieringService(
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
        assertEquals("true", settings.get(IS_WARM_INDEX_SETTING.getKey()));
        assertEquals(HOT_TO_WARM.toString(), settings.get(INDEX_TIERING_STATE.getKey()));
        assertEquals(TIERED_COMPOSITE_INDEX_TYPE, settings.get(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey()));
    }

    public void testGetIndexTierSettingsToRestoreAfterCancellation() {
        Settings settings = service.getIndexTierSettingsToRestoreAfterCancellation();
        assertEquals("false", settings.get(IS_WARM_INDEX_SETTING.getKey()));
        assertEquals(TieringState.HOT.toString(), settings.get(INDEX_TIERING_STATE.getKey()));
        assertEquals("default", settings.get(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey()));
    }

    public void testGetTieringStartTimeKey() {
        assertEquals(H2W_TIERING_START_TIME_KEY, service.getTieringStartTimeKey());
    }

    public void testGetMaxConcurrentTieringRequestsSetting() {
        assertEquals(H2W_MAX_CONCURRENT_TIERING_REQUESTS_KEY, service.getMaxConcurrentTieringRequestsSetting().getKey());
    }

    public void testGetTargetTieringState() {
        assertEquals(TieringState.WARM, service.getTargetTieringState());
    }

    public void testGetTieringType() {
        assertEquals(HOT_TO_WARM, service.getTieringType());
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
        when(node.isWarmNode()).thenReturn(true);

        assertTrue(service.isShardInTargetTier(shard, clusterState));
    }
}
