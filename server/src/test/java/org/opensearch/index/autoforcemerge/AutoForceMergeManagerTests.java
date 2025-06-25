/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.autoforcemerge;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsService;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoForceMergeManagerTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private IndicesService indicesService;
    private MonitorService monitorService;
    private OsService osService;
    private FsService fsService;
    private JvmService jvmService;
    private ThreadPool threadPool;
    private OsStats.Cpu cpu;
    private FsInfo.Path disk;
    private JvmStats.Mem jvm;

    private final String DATA_NODE_1 = "DATA_NODE_1";
    private final String DATA_NODE_2 = "DATA_NODE_2";
    private final String WARM_NODE_1 = "WARM_NODE_1";
    private final String WARM_NODE_2 = "WARM_NODE_2";
    private final String TEST_INDEX_1 = "TEST_INDEX_1";
    private final String TEST_INDEX_2 = "TEST_INDEX_2";

    private final String SCHEDULER_INTERVAL = "1s";
    private final String TRANSLOG_AGE = "1s";
    private final String MERGE_DELAY = "1s";
    private Integer allocatedProcessors;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        indicesService = mock(IndicesService.class);
        monitorService = mock(MonitorService.class);
        osService = mock(OsService.class);
        fsService = mock(FsService.class);
        jvmService = mock(JvmService.class);

        when(monitorService.osService()).thenReturn(osService);
        when(monitorService.fsService()).thenReturn(fsService);
        when(monitorService.jvmService()).thenReturn(jvmService);

        OsStats osStats = mock(OsStats.class);
        cpu = mock(OsStats.Cpu.class);
        when(osService.stats()).thenReturn(osStats);
        when(osStats.getCpu()).thenReturn(cpu);

        FsInfo fsInfo = mock(FsInfo.class);
        disk = mock(FsInfo.Path.class);
        when(fsService.stats()).thenReturn(fsInfo);
        when(fsInfo.getTotal()).thenReturn(disk);
        when(disk.getTotal()).thenReturn(new ByteSizeValue(100));
        when(disk.getAvailable()).thenReturn(new ByteSizeValue(50));

        JvmStats jvmStats = mock(JvmStats.class);
        jvm = mock(JvmStats.Mem.class);
        when(jvmService.stats()).thenReturn(jvmStats);
        when(jvmStats.getMem()).thenReturn(jvm);

        allocatedProcessors = OpenSearchExecutors.allocatedProcessors(Settings.EMPTY);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    // ConfigurationValidator Tests
    public void testConfigurationValidatorWithFeatureDisabled() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(false, false, Collections.emptyMap()),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getConfigurationValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    public void testConfigurationValidatorWithDataNodeAndNonRemoteStore() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, false, Collections.emptyMap()),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        autoForceMergeManager.getTask().runInternal();
        assertFalse(autoForceMergeManager.getConfigurationValidator().validate().isAllowed());
    }

    public void testConfigurationValidatorWithDataNodeAndRemoteStore() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        assertTrue(autoForceMergeManager.getConfigurationValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    public void testConfigurationValidatorWithNonDataNode() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            getNodeWithRoles(WARM_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.WARM_ROLE))
        );
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getConfigurationValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    // NodeValidator Tests
    public void testNodeValidatorWithHealthyResources() {
        when(cpu.getPercent()).thenReturn((short) 50);
        when(cpu.getLoadAverage()).thenReturn(new double[]{0.7 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors});
        when(jvm.getHeapUsedPercent()).thenReturn((short) 60);
        ThreadPoolStats stats = new ThreadPoolStats(
            Arrays.asList(new ThreadPoolStats.Stats(
                ThreadPool.Names.FORCE_MERGE, 1, 0, 0, 0, 1, 0, 0
            ))
        );
        when(threadPool.stats()).thenReturn(stats);

        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(getConfiguredClusterSettings(true, true, Collections.emptyMap()), getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE)));
        autoForceMergeManager.start();
        assertTrue(autoForceMergeManager.getNodeValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    public void testNodeValidatorWithHighCPU() {
        when(cpu.getPercent()).thenReturn((short) 95);
        when(cpu.getLoadAverage()).thenReturn(new double[]{0.7 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors});
        DiscoveryNode dataNode1 = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode warmNode1 = getNodeWithRoles(WARM_NODE_1, Set.of(DiscoveryNodeRole.WARM_ROLE));
        ClusterState clusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(
                DiscoveryNodes.builder()
                    .add(dataNode1)
                    .add(warmNode1)
                    .localNodeId(dataNode1.getId())
                    .clusterManagerNodeId(dataNode1.getId())
            )
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(getConfiguredClusterSettings(true, true, Collections.emptyMap()), dataNode1);
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
        when(cpu.getPercent()).thenReturn((short) 50);
        when(cpu.getLoadAverage()).thenReturn(new double[]{0.9 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors});
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
        when(cpu.getLoadAverage()).thenReturn(new double[]{0.7 * allocatedProcessors, 0.9 * allocatedProcessors, 0.5 * allocatedProcessors});
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    public void testNodeValidatorWithHighDiskUsage() {
        when(cpu.getPercent()).thenReturn((short) 50);
        when(cpu.getLoadAverage()).thenReturn(new double[]{0.7 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors});
        when(disk.getAvailable()).thenReturn(new ByteSizeValue(5));
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(getConfiguredClusterSettings(true, true, Collections.emptyMap()), getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE)));
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    public void testNodeValidatorWithHighJVMUsage() {
        when(cpu.getPercent()).thenReturn((short) 50);
        when(cpu.getLoadAverage()).thenReturn(new double[]{0.7 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors});
        when(jvm.getHeapUsedPercent()).thenReturn((short) 90);
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(getConfiguredClusterSettings(true, true, Collections.emptyMap()), getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE)));
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    public void testNodeValidatorWithInsufficientForceMergeThreads() {
        when(cpu.getPercent()).thenReturn((short) 50);
        when(cpu.getLoadAverage()).thenReturn(new double[]{0.7 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors});
        when(jvm.getHeapUsedPercent()).thenReturn((short) 50);
        ThreadPoolStats stats = new ThreadPoolStats(
            Arrays.asList(new ThreadPoolStats.Stats(
                ThreadPool.Names.FORCE_MERGE, 1, 1, 1, 0, 1, 0, -1
            ))
        );
        when(threadPool.stats()).thenReturn(stats);
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(getConfiguredClusterSettings(true, true, Collections.emptyMap()), getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE)));
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
        ThreadPoolStats emptyStats = new ThreadPoolStats(Collections.emptyList());
        when(threadPool.stats()).thenReturn(emptyStats);
        assertFalse(autoForceMergeManager.getNodeValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    // ShardValidator Tests
    public void testShardValidatorWithValidShard() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(6).getMillis());
        IndexShard shard = getShard(TEST_INDEX_1, translogStats, 2);
        assertTrue(autoForceMergeManager.getShardValidator().validate(shard).isAllowed());
        autoForceMergeManager.close();
    }

    public void testShardValidatorWithShardNotInStartedState() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(6).getMillis());
        IndexShard shard = getShard(TEST_INDEX_1, translogStats, 2);
        when(shard.state()).thenReturn(IndexShardState.RECOVERING);
        assertFalse(autoForceMergeManager.getShardValidator().validate(shard).isAllowed());
        autoForceMergeManager.close();
    }

    public void testShardValidatorWithForbiddenAutoForceMergesSetting() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        IndexShard shard = getShard(TEST_INDEX_1, mock(TranslogStats.class), 1);
        IndexSettings indexSettings = getNewIndexSettings(TEST_INDEX_1);
        indexSettings.setAutoForcemergeEnabled(false);
        when(shard.indexSettings()).thenReturn(indexSettings);
        assertFalse(autoForceMergeManager.getShardValidator().validate(shard).isAllowed());
        autoForceMergeManager.close();
    }

    public void testShardValidatorWithLowSegmentCount() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(5).getMillis());
        IndexShard shard = getShard(TEST_INDEX_1, translogStats, 1);
        assertFalse(autoForceMergeManager.getShardValidator().validate(shard).isAllowed());
        autoForceMergeManager.close();
    }

    public void testShardValidatorWithRecentTranslog() {
        Map<String, Object> additionalSettings = new HashMap<>();
        additionalSettings.put(ForceMergeManagerSettings.TRANSLOG_AGE_AUTO_FORCE_MERGE.getKey(), "2s");
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, additionalSettings),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(1).getMillis());
        IndexShard shard = getShard(TEST_INDEX_1, translogStats, 2);
        assertFalse(autoForceMergeManager.getShardValidator().validate(shard).isAllowed());
        autoForceMergeManager.close();
    }

    public void testShardValidatorWithoutShard() {
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE))
        );
        autoForceMergeManager.start();
        assertFalse(autoForceMergeManager.getShardValidator().validate().isAllowed());
        autoForceMergeManager.close();
    }

    public void testForceMergeOperationOnWarmDisabledCluster() {
        DiscoveryNode dataNode1 = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode dataNode2 = getNodeWithRoles(DATA_NODE_2, Set.of(DiscoveryNodeRole.DATA_ROLE));
        ClusterState clusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(
                DiscoveryNodes.builder()
                    .add(dataNode1)
                    .add(dataNode2)
                    .localNodeId(dataNode1.getId())
                    .clusterManagerNodeId(dataNode1.getId())
            )
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            dataNode1
        );
        autoForceMergeManager.start();
        autoForceMergeManager.getTask().runInternal();
        verify(cpu, never()).getPercent();
        autoForceMergeManager.close();
    }

    public void testForceMergeOperationOnDataNodeWithFailingMerges() throws IOException {
        DiscoveryNode dataNode1 = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode dataNode2 = getNodeWithRoles(DATA_NODE_2, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode warmNode1 = getNodeWithRoles(WARM_NODE_1, Set.of(DiscoveryNodeRole.WARM_ROLE));
        DiscoveryNode warmNode2 = getNodeWithRoles(WARM_NODE_2, Set.of(DiscoveryNodeRole.WARM_ROLE));
        ClusterState clusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(
                DiscoveryNodes.builder()
                    .add(dataNode1)
                    .add(dataNode2)
                    .add(warmNode1)
                    .add(warmNode2)
                    .localNodeId(dataNode1.getId())
                    .clusterManagerNodeId(dataNode1.getId())
            )
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(cpu.getPercent()).thenReturn((short) 50);
        when(cpu.getLoadAverage()).thenReturn(
            new double[] { 0.7 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors }
        );
        when(jvm.getHeapUsedPercent()).thenReturn((short) 50);

        int forceMergeThreads = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(forceMergeThreads);
        when(threadPool.executor(ThreadPool.Names.FORCE_MERGE)).thenReturn(executorService);
        ThreadPoolStats stats = new ThreadPoolStats(
            Arrays.asList(new ThreadPoolStats.Stats(ThreadPool.Names.FORCE_MERGE, forceMergeThreads, 0, 0, 0, forceMergeThreads, 0, -1))
        );
        when(threadPool.stats()).thenReturn(stats);

        IndexService indexService1 = mock(IndexService.class);
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(6).getMillis());
        IndexShard shard1 = getShard(TEST_INDEX_1, translogStats, 2);
        List<IndexShard> indexShards1 = List.of(shard1);
        when(indexService1.spliterator()).thenReturn(indexShards1.spliterator());
        List<IndexService> indexServices = List.of(indexService1);
        when(indicesService.spliterator()).thenReturn(indexServices.spliterator());
        when(shard1.indexSettings()).thenReturn(getNewIndexSettings(TEST_INDEX_1));
        when(shard1.state()).thenReturn(IndexShardState.STARTED);

        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            dataNode1
        );
        autoForceMergeManager.start();
        doThrow(new IOException("Testing")).when(shard1).forceMerge(any());
        autoForceMergeManager.getTask().runInternal();
        verify(shard1, times(1)).forceMerge(any());
        autoForceMergeManager.close();
        executorService.shutdown();
    }

    public void testForceMergeOperationOnDataNodeOfWarmEnabledCluster() throws IOException {
        DiscoveryNode dataNode1 = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode dataNode2 = getNodeWithRoles(DATA_NODE_2, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode warmNode1 = getNodeWithRoles(WARM_NODE_1, Set.of(DiscoveryNodeRole.WARM_ROLE));
        DiscoveryNode warmNode2 = getNodeWithRoles(WARM_NODE_2, Set.of(DiscoveryNodeRole.WARM_ROLE));
        ClusterState clusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(
                DiscoveryNodes.builder()
                    .add(dataNode1)
                    .add(dataNode2)
                    .add(warmNode1)
                    .add(warmNode2)
                    .localNodeId(dataNode1.getId())
                    .clusterManagerNodeId(dataNode1.getId())
            )
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(cpu.getPercent()).thenReturn((short) 50);
        when(cpu.getLoadAverage()).thenReturn(
            new double[] { 0.7 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors }
        );
        when(jvm.getHeapUsedPercent()).thenReturn((short) 50);
        int forceMergeThreads = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(forceMergeThreads);
        when(threadPool.executor(ThreadPool.Names.FORCE_MERGE)).thenReturn(executorService);
        ThreadPoolStats stats = new ThreadPoolStats(
            Arrays.asList(new ThreadPoolStats.Stats(ThreadPool.Names.FORCE_MERGE, forceMergeThreads, 0, 0, 0, forceMergeThreads, 0, -1))
        );
        when(threadPool.stats()).thenReturn(stats);
        IndexService indexService1 = mock(IndexService.class);
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(1).getMillis());
        IndexShard shard1 = getShard(TEST_INDEX_1, translogStats, 2);
        List<IndexShard> indexShards1 = List.of(shard1);
        when(indexService1.spliterator()).thenReturn(indexShards1.spliterator());
        IndexService indexService2 = mock(IndexService.class);
        IndexShard shard2 = getShard(TEST_INDEX_2, translogStats, 2);
        List<IndexShard> indexShards2 = List.of(shard2);
        when(indexService2.spliterator()).thenReturn(indexShards2.spliterator());
        List<IndexService> indexServices = Arrays.asList(indexService1, indexService2);
        when(indicesService.spliterator()).thenReturn(indexServices.spliterator());
        when(shard1.indexSettings()).thenReturn(getNewIndexSettings(TEST_INDEX_1));
        when(shard1.state()).thenReturn(IndexShardState.STARTED);
        when(shard2.indexSettings()).thenReturn(getNewIndexSettings(TEST_INDEX_2));
        when(shard2.state()).thenReturn(IndexShardState.STARTED);

        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            dataNode1
        );
        autoForceMergeManager.start();
        autoForceMergeManager.getTask().runInternal();
        verify(shard1, atLeastOnce()).forceMerge(any());
        verify(shard2, atLeastOnce()).forceMerge(any());
        autoForceMergeManager.close();
        executorService.shutdown();
    }

    public void testForceMergeOperationOnDataNodeWithThreadInterruption() throws InterruptedException {
        DiscoveryNode dataNode1 = getNodeWithRoles(DATA_NODE_1, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode dataNode2 = getNodeWithRoles(DATA_NODE_2, Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode warmNode1 = getNodeWithRoles(WARM_NODE_1, Set.of(DiscoveryNodeRole.WARM_ROLE));
        DiscoveryNode warmNode2 = getNodeWithRoles(WARM_NODE_2, Set.of(DiscoveryNodeRole.WARM_ROLE));
        ClusterState clusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(
                DiscoveryNodes.builder()
                    .add(dataNode1)
                    .add(dataNode2)
                    .add(warmNode1)
                    .add(warmNode2)
                    .localNodeId(dataNode1.getId())
                    .clusterManagerNodeId(dataNode1.getId())
            )
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(cpu.getPercent()).thenReturn((short) 50);
        when(cpu.getLoadAverage()).thenReturn(
            new double[] { 0.7 * allocatedProcessors, 0.6 * allocatedProcessors, 0.5 * allocatedProcessors }
        );
        when(jvm.getHeapUsedPercent()).thenReturn((short) 50);

        int forceMergeThreads = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(forceMergeThreads);
        when(threadPool.executor(ThreadPool.Names.FORCE_MERGE)).thenReturn(executorService);
        ThreadPoolStats stats = new ThreadPoolStats(
            Arrays.asList(new ThreadPoolStats.Stats(ThreadPool.Names.FORCE_MERGE, forceMergeThreads, 0, 0, 0, forceMergeThreads, 0, -1))
        );
        when(threadPool.stats()).thenReturn(stats);

        IndexService indexService1 = mock(IndexService.class);
        TranslogStats translogStats = new TranslogStats(0, 0, 0, 0, TimeValue.timeValueSeconds(1).getMillis());
        List<IndexShard> indexShards1 = List.of(getShard(TEST_INDEX_1, translogStats, 2));
        when(indexService1.spliterator()).thenReturn(indexShards1.spliterator());
        List<IndexService> indexServices = List.of(indexService1);
        when(indicesService.spliterator()).thenReturn(indexServices.spliterator());

        AutoForceMergeManager autoForceMergeManager = clusterSetupWithNode(
            getConfiguredClusterSettings(true, true, Collections.emptyMap()),
            dataNode1
        );
        autoForceMergeManager.start();
        Thread testThread = new Thread(() -> autoForceMergeManager.getTask().runInternal());
        testThread.start();
        Thread.sleep(1000L);
        testThread.interrupt();
        assertTrue(testThread.isInterrupted());
        autoForceMergeManager.close();
        executorService.shutdown();
    }

    private DiscoveryNode getNodeWithRoles(String name, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode(name, buildNewFakeTransportAddress(), new HashMap<>(), Sets.newHashSet(roles), Version.CURRENT);
    }

    private AutoForceMergeManager clusterSetupWithNode(Settings settings, DiscoveryNode node) {
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.localNode()).thenReturn(node);

        return new AutoForceMergeManager(
            threadPool,
            monitorService,
            indicesService,
            clusterService
        );
    }

    private IndexShard getShard(String indexName, TranslogStats translogStats, Integer segmentCount) {
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId1 = new ShardId(indexName, "_na_", 0);
        SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.add(segmentCount);
        ShardRouting shardRouting = mock(ShardRouting.class);
        when(shard.shardId()).thenReturn(shardId1);
        when(shard.translogStats()).thenReturn(translogStats);
        when(shard.segmentStats(false, false)).thenReturn(segmentsStats);
        when(shard.routingEntry()).thenReturn(shardRouting);
        when(shardRouting.primary()).thenReturn(true);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        when(shard.indexSettings()).thenReturn(getNewIndexSettings(TEST_INDEX_1));
        return shard;
    }

    private IndexSettings getNewIndexSettings(String indexName) {
        return new IndexSettings(
            newIndexMeta(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
                    .put(IndexSettings.INDEX_AUTO_FORCE_MERGES_ENABLED.getKey(), true)
                    .build()
            ),
            Settings.EMPTY
        );
    }

    private Settings getConfiguredClusterSettings(Boolean featureEnabled, Boolean remoteEnabled, Map<String, Object> additionalSettings) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SETTING.getKey(), featureEnabled)
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), remoteEnabled)
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), SCHEDULER_INTERVAL)
            .put(ForceMergeManagerSettings.TRANSLOG_AGE_AUTO_FORCE_MERGE.getKey(), TRANSLOG_AGE)
            .put(ForceMergeManagerSettings.MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE.getKey(), MERGE_DELAY);
        if (additionalSettings != null) {
            additionalSettings.forEach((key, value) -> {
                if (value != null) {
                    switch (value) {
                        case Boolean b -> settingsBuilder.put(key, b);
                        case Integer i -> settingsBuilder.put(key, i);
                        case Long l -> settingsBuilder.put(key, l);
                        case Double v -> settingsBuilder.put(key, v);
                        case String s -> settingsBuilder.put(key, s);
                        case TimeValue timeValue -> settingsBuilder.put(key, timeValue);
                        default -> settingsBuilder.put(key, value.toString());
                    }
                }
            });
        }
        return settingsBuilder.build();
    }
}
