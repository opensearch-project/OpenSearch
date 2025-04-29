/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autoforcemerge;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsService;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.node.Node;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;

public class AutoForceMergeManagerTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private IndicesService indicesService;
    private OsService osService;
    private JvmService jvmService;
    private ThreadPool threadPool;
    private OsStats.Cpu cpu;
    private JvmStats.Mem mem;
    private Settings settings;

    private final String DATA_NODE_1 = "DATA_NODE_1";
    private final String DATA_NODE_2 = "DATA_NODE_2";
    private final String WARM_NODE_1 = "WARM_NODE_1";
    private final String WARM_NODE_2 = "WARM_NODE_2";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        indicesService = mock(IndicesService.class);
        osService = mock(OsService.class);
        jvmService = mock(JvmService.class);

        OsStats osStats = mock(OsStats.class);
        cpu = mock(OsStats.Cpu.class);
        when(osService.stats()).thenReturn(osStats);
        when(osStats.getCpu()).thenReturn(cpu);

        JvmStats jvmStats = mock(JvmStats.class);
        mem = mock(JvmStats.Mem.class);
        when(jvmService.stats()).thenReturn(jvmStats);
        when(jvmStats.getMem()).thenReturn(mem);

    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    // ConfigurationValidator Tests
    public void testConfigurationValidatorWithDataNodeAndNonRemoteStore() {
        DiscoveryNode dataNode = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), false)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "1s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode);

        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getConfigurationValidator().validate().isAllowed());
    }

    public void testConfigurationValidatorWithDataNodeAndRemoteStore() {
        DiscoveryNode dataNode = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "1s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode);

        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        assertTrue(autoForceMergeManager.getConfigurationValidator().validate().isAllowed());
    }

    public void testConfigurationValidatorWithNonDataNode() {
        DiscoveryNode warmNode = getNodeWithRoles(WARM_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.WARM_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "1s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(warmNode);

        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getConfigurationValidator().validate().isAllowed());
    }

    // NodeValidator Tests
    public void testNodeValidatorWithHealthyResources() {
        when(cpu.getPercent()).thenReturn((short) 50);
        when(mem.getHeapUsedPercent()).thenReturn((short) 60);
        ThreadPoolStats stats = new ThreadPoolStats(
            Arrays.asList(new ThreadPoolStats.Stats(
                ThreadPool.Names.FORCE_MERGE, 1, 0, 0, 0, 1, 0, 0
            ))
        );
        when(threadPool.stats()).thenReturn(stats);

        DiscoveryNode dataNode = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "1s")
            .put(ForceMergeManagerSettings.CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 80)
            .put(ForceMergeManagerSettings.JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 70)
            .put(ForceMergeManagerSettings.FORCE_MERGE_THREADS_THRESHOLD_COUNT_FOR_AUTO_FORCE_MERGE.getKey(), 1)
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode);

        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        assertTrue(autoForceMergeManager.getNodeValidator().validate().isAllowed());
    }

    public void testNodeValidatorWithHighCPU() {
        when(cpu.getPercent()).thenReturn((short) 90);
        DiscoveryNode dataNode = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "1s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode);

        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
    }

    public void testNodeValidatorWithHighJVMUsage() {
        when(cpu.getPercent()).thenReturn((short) 50);
        when(mem.getHeapUsedPercent()).thenReturn((short) 90);

        DiscoveryNode dataNode = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "1s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode);

        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
    }

    public void testNodeValidatorWithInsufficientForceMergeThreads() {
        when(cpu.getPercent()).thenReturn((short) 50);
        when(mem.getHeapUsedPercent()).thenReturn((short) 50);

        ThreadPoolStats stats = new ThreadPoolStats(
            Arrays.asList(new ThreadPoolStats.Stats(
                ThreadPool.Names.FORCE_MERGE, 1, 1, 1, 0, 1, 0, -1
            ))
        );
        when(threadPool.stats()).thenReturn(stats);

        DiscoveryNode dataNode = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "1s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode);

        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
    }

    // ShardValidator Tests
    public void testShardValidatorWithValidShard() {
        clusterSetup();
        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId("test_index", "_na_", 0);
        when(shard.shardId()).thenReturn(shardId);
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(6).getMillis());
        when(shard.translogStats()).thenReturn(translogStats);
        SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.add(2);
        when(shard.segmentStats(false, false)).thenReturn(segmentsStats);
        assertTrue(autoForceMergeManager.getShardValidator().validate(shard).isAllowed());
    }

    public void testShardValidatorWithLowSegmentCount() {
        clusterSetup();
        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId("test_index", "_na_", 0);
        when(shard.shardId()).thenReturn(shardId);
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(5).getMillis());
        when(shard.translogStats()).thenReturn(translogStats);
        SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.add(1);
        when(shard.segmentStats(false, false)).thenReturn(segmentsStats);
        assertFalse(autoForceMergeManager.getShardValidator().validate(shard).isAllowed());
    }

    public void testShardValidatorWithRecentTranslog() {
        clusterSetup();
        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId("test_index", "_na_", 0);
        when(shard.shardId()).thenReturn(shardId);
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(1).getMillis());
        when(shard.translogStats()).thenReturn(translogStats);
        SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.add(2);
        when(shard.segmentStats(false, false)).thenReturn(segmentsStats);
        assertFalse(autoForceMergeManager.getShardValidator().validate(shard).isAllowed());
    }

    public void testShardValidatorWithoutShard() {
        clusterSetup();
        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getShardValidator().validate().isAllowed());
    }

    public void testForceMergeOperationOnWarmDisabledCluster() {
        DiscoveryNode dataNode1 = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode dataNode2 = getNodeWithRoles(DATA_NODE_2, Set.of(DiscoveryNodeRole.DATA_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "5s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode1);
        ClusterState clusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(dataNode1).add(dataNode2).localNodeId(dataNode1.getId()).clusterManagerNodeId(dataNode1.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        autoForceMergeManager.getTask().runInternal();
        verify(cpu, never()).getPercent();
    }

    public void testForceMergeOperationOnDataNodeOfWarmEnabledCluster() throws IOException, InterruptedException {
        DiscoveryNode dataNode1 = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode dataNode2 = getNodeWithRoles(DATA_NODE_2, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode warmNode1 = getNodeWithRoles(WARM_NODE_1, Set.of(DiscoveryNodeRole.WARM_ROLE));
        DiscoveryNode warmNode2 = getNodeWithRoles(WARM_NODE_2, Set.of(DiscoveryNodeRole.WARM_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "3s")
            .put(ForceMergeManagerSettings.MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE.getKey(), "1s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode1);
        ClusterState clusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(dataNode1).add(dataNode2).add(warmNode1).add(warmNode2).localNodeId(dataNode1.getId()).clusterManagerNodeId(dataNode1.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(cpu.getPercent()).thenReturn((short) 50);
        when(mem.getHeapUsedPercent()).thenReturn((short) 50);
        int forceMergeThreads = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(forceMergeThreads);
        when(threadPool.executor(ThreadPool.Names.FORCE_MERGE)).thenReturn(executorService);
        ThreadPoolStats stats = new ThreadPoolStats(
            Arrays.asList(new ThreadPoolStats.Stats(
                ThreadPool.Names.FORCE_MERGE, forceMergeThreads, 0, 0, 0, forceMergeThreads, 0, -1
            ))
        );
        when(threadPool.stats()).thenReturn(stats);
        IndexService indexService1 = mock(IndexService.class);
        IndexShard shard1 = getShard("Index1");
        IndexShard shard2 = getShard("Index1");
        List<IndexShard> indexShards1 = Arrays.asList(shard1, shard2);
        when(indexService1.iterator()).thenReturn(indexShards1.iterator());
        IndexService indexService2 = mock(IndexService.class);
        IndexShard shard3 = getShard("Index2");
        IndexShard shard4 = getShard("Index2");
        List<IndexShard> indexShards2 = Arrays.asList(shard3, shard4);
        when(indexService2.iterator()).thenReturn(indexShards2.iterator());
        List<IndexService> indexServices = Arrays.asList(indexService1, indexService2);
        when(indicesService.iterator()).thenReturn(indexServices.iterator());

        AutoForceMergeManager autoForceMergeManager = new AutoForceMergeManager(threadPool,
            osService, jvmService, indicesService, clusterService);
        autoForceMergeManager.start();
        ByteSizeValue cacheSize = new ByteSizeValue(16, ByteSizeUnit.GB);

        clusterService.getClusterSettings().applySettings(Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "3s")
            .put(ForceMergeManagerSettings.MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE.getKey(), "1s")
            .build());
        autoForceMergeManager.getTask().runInternal();
        Thread.sleep(TimeValue.timeValueSeconds(1).getMillis());
        verify(shard1, atLeastOnce()).forceMerge(any());
        verify(shard2, atLeastOnce()).forceMerge(any());
        verify(shard3, atLeastOnce()).forceMerge(any());
        verify(shard4, atLeastOnce()).forceMerge(any());

        executorService.shutdown();
    }

    private DiscoveryNode getNodeWithRoles(String name, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode(
            name,
            buildNewFakeTransportAddress(),
            new HashMap<>(),
            Sets.newHashSet(roles),
            Version.CURRENT
        );
    }

    private void clusterSetup() {
        DiscoveryNode dataNode = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "5s")
            .build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(dataNode);
    }

    private IndexShard getShard(String indexName) {
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId1 = new ShardId(indexName, "_na_", 0);
        when(shard.shardId()).thenReturn(shardId1);
        TranslogStats translogStats1 = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(6).getMillis());
        when(shard.translogStats()).thenReturn(translogStats1);
        SegmentsStats segmentsStats1 = new SegmentsStats();
        segmentsStats1.add(2);
        when(shard.segmentStats(false, false)).thenReturn(segmentsStats1);
        ShardRouting shardRouting = mock(ShardRouting.class);
        when(shard.routingEntry()).thenReturn(shardRouting);
        when(shardRouting.primary()).thenReturn(true);
        return shard;
    }
}

