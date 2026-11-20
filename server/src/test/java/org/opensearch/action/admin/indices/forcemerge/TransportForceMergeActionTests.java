/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.IoUsageStats;
import org.opensearch.node.Node;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the admission control integration of {@link TransportForceMergeAction#shardOperation}.
 * Uses a real {@link AdmissionControlService} (and therefore the real controllers and settings) backed by a mocked
 * {@link ResourceUsageCollectorService}, so the CPU utilization seen by the controllers is fully deterministic.
 * Only the CPU controller is asserted on because it is the only one registered on every platform.
 */
public class TransportForceMergeActionTests extends OpenSearchTestCase {

    private static final Index INDEX = new Index("test", "_na_");

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ResourceUsageCollectorService resourceUsageCollectorService;
    private IndicesService indicesService;
    private IndexService indexService;
    private IndexShard indexShard;
    private AdmissionControlService admissionControlService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        // sets an initial cluster state with a local node, which the controllers need to resolve local resource usage
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        resourceUsageCollectorService = mock(ResourceUsageCollectorService.class);
        indicesService = mock(IndicesService.class);
        indexService = mock(IndexService.class);
        indexShard = mock(IndexShard.class);
        when(indicesService.indexServiceSafe(INDEX)).thenReturn(indexService);
        when(indexService.getShard(anyInt())).thenReturn(indexShard);
        // default: a primary of a document replication index on a non remote node, i.e. admission controlled
        localShard(true, ReplicationType.DOCUMENT, Settings.EMPTY);
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.close();
        terminate(threadPool);
        super.tearDown();
    }

    public void testAdmittedWhenCpuBelowForceMergeLimit() throws IOException {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 80));
        setLocalCpu(50);
        ForceMergeRequest request = new ForceMergeRequest("test");

        action.shardOperation(request, shard(0));

        verify(indexShard, times(1)).forceMerge(request);
        assertEquals(0, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testRejectedWhenCpuAtOrAboveForceMergeLimit() throws IOException {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 80));
        setLocalCpu(randomFrom(80, 95, 100));

        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> action.shardOperation(new ForceMergeRequest("test"), shard(0))
        );

        assertTrue(e.getMessage(), e.getMessage().contains(ForceMergeAction.NAME));
        assertTrue(e.getMessage(), e.getMessage().contains(AdmissionControlActionType.FORCE_MERGE.name()));
        verify(indexShard, never()).forceMerge(any());
        AdmissionController controller = cpuController();
        assertEquals(1, controller.getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
        // force merge rejections must not be attributed to other workloads
        assertEquals(0, controller.getRejectionCount(AdmissionControlActionType.INDEXING.getType()));
        assertEquals(0, controller.getRejectionCount(AdmissionControlActionType.SEARCH.getType()));
    }

    public void testMonitorModeCountsButDoesNotReject() throws IOException {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.MONITOR, 10));
        setLocalCpu(90);
        ForceMergeRequest request = new ForceMergeRequest("test");

        action.shardOperation(request, shard(0));

        verify(indexShard, times(1)).forceMerge(request);
        assertEquals(1, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testDisabledByDefaultPreservesExistingBehavior() throws IOException {
        // No admission control settings at all: the default mode is disabled.
        TransportForceMergeAction action = createAction(Settings.EMPTY);
        ForceMergeRequest request = new ForceMergeRequest("test");

        action.shardOperation(request, shard(0));

        verify(indexShard, times(1)).forceMerge(request);
        // disabled controllers must not even read resource usage
        verifyNoInteractions(resourceUsageCollectorService);
        assertEquals(0, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testDynamicLimitUpdateIsAppliedToNextShard() throws IOException {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 100));
        setLocalCpu(60);
        ForceMergeRequest request = new ForceMergeRequest("test");
        action.shardOperation(request, shard(0));

        clusterService.getClusterSettings()
            .applySettings(
                Settings.builder()
                    .put(cpuSettings(AdmissionControlMode.ENFORCED, 100))
                    .put(CpuBasedAdmissionControllerSettings.FORCE_MERGE_CPU_USAGE_LIMIT.getKey(), 50)
                    .build()
            );

        expectThrows(OpenSearchRejectedExecutionException.class, () -> action.shardOperation(request, shard(1)));
        verify(indexShard, times(1)).forceMerge(request);
        assertEquals(1, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testEachShardOnSameNodeIsAdmittedIndependently() throws IOException {
        // Shards of a node request run sequentially; usage rising while shard 0 merges must reject shard 1.
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 80));
        String localNodeId = clusterService.state().nodes().getLocalNodeId();
        when(resourceUsageCollectorService.getNodeStatistics(localNodeId)).thenReturn(Optional.of(cpuStats(localNodeId, 40)))
            .thenReturn(Optional.of(cpuStats(localNodeId, 90)))
            .thenReturn(Optional.of(cpuStats(localNodeId, 40)));
        ForceMergeRequest request = new ForceMergeRequest("test");

        action.shardOperation(request, shard(0));
        expectThrows(OpenSearchRejectedExecutionException.class, () -> action.shardOperation(request, shard(1)));
        action.shardOperation(request, shard(2));

        verify(indexShard, times(2)).forceMerge(request);
        verify(resourceUsageCollectorService, times(3)).getNodeStatistics(localNodeId);
        assertEquals(1, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testAdmittedWhenNoResourceStatsAvailable() throws IOException {
        // Matches the existing controllers: no stats sample yet means the request is admitted.
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 0));
        when(resourceUsageCollectorService.getNodeStatistics(anyString())).thenReturn(Optional.empty());
        ForceMergeRequest request = new ForceMergeRequest("test");

        action.shardOperation(request, shard(0));

        verify(indexShard, times(1)).forceMerge(request);
        assertEquals(0, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testMergeFailureAfterAdmissionPropagatesWithoutSideEffects() throws IOException {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 80));
        setLocalCpu(10);
        ForceMergeRequest request = new ForceMergeRequest("test");
        IOException mergeFailure = new IOException("simulated merge failure");
        doThrow(mergeFailure).when(indexShard).forceMerge(request);

        IOException e = expectThrows(IOException.class, () -> action.shardOperation(request, shard(0)));

        assertSame(mergeFailure, e);
        // admission is a stateless check: a failed merge leaves nothing to release and is not a rejection
        assertEquals(0, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
        // and a subsequent shard is evaluated normally
        doThrow(new AssertionError("should not be called")).when(indexShard).forceMerge(any());
        setLocalCpu(90);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> action.shardOperation(request, shard(1)));
        assertEquals(1, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testMissingIndexFailsBeforeAdmission() {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 0));
        setLocalCpu(90);
        Index missing = new Index("missing", "_na_");
        when(indicesService.indexServiceSafe(missing)).thenThrow(new IndexNotFoundException(missing));
        ShardRouting routing = TestShardRouting.newShardRouting(new ShardId(missing, 0), "node", true, ShardRoutingState.STARTED);

        expectThrows(IndexNotFoundException.class, () -> action.shardOperation(new ForceMergeRequest("missing"), routing));

        // the real shard error is reported, and no admission rejection is recorded for a shard that does not exist
        assertEquals(0, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testConcurrentShardOperationsAreAllRejectedAndCounted() throws Exception {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 50));
        setLocalCpu(99);
        final int threads = randomIntBetween(4, 16);
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final CountDownLatch done = new CountDownLatch(threads);
        final AtomicInteger rejected = new AtomicInteger();
        final List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < threads; i++) {
            final int shardId = i;
            threadPool.generic().execute(() -> {
                try {
                    barrier.await();
                    action.shardOperation(new ForceMergeRequest("test"), shard(shardId));
                } catch (OpenSearchRejectedExecutionException e) {
                    rejected.incrementAndGet();
                } catch (Throwable t) {
                    unexpected.add(t);
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertTrue("unexpected failures: " + unexpected, unexpected.isEmpty());
        assertEquals(threads, rejected.get());
        verify(indexShard, never()).forceMerge(any());
        // every concurrent first-time rejection of the new action type must be counted exactly once
        assertEquals(threads, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testConcurrentShardOperationsAreAllAdmittedBelowLimit() throws Exception {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 90));
        setLocalCpu(20);
        final int threads = randomIntBetween(4, 16);
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final CountDownLatch done = new CountDownLatch(threads);
        final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        final ForceMergeRequest request = new ForceMergeRequest("test");
        for (int i = 0; i < threads; i++) {
            final int shardId = i;
            threadPool.generic().execute(() -> {
                try {
                    barrier.await();
                    action.shardOperation(request, shard(shardId));
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertTrue("unexpected failures: " + failures, failures.isEmpty());
        verify(indexShard, times(threads)).forceMerge(request);
        assertEquals(0, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testSegmentReplicationReplicaSkipsAdmissionControl() throws IOException {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 0));
        setLocalCpu(100);
        localShard(false, ReplicationType.SEGMENT, Settings.EMPTY);
        ForceMergeRequest request = new ForceMergeRequest("test");

        action.shardOperation(request, shard(0));

        // the replica's engine force merge is a no-op, so it is not gated and never reported as rejected
        verify(indexShard, times(1)).forceMerge(request);
        verifyNoInteractions(resourceUsageCollectorService);
        assertEquals(0, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testRemoteNodeReplicaSkipsAdmissionControl() throws IOException {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 0));
        setLocalCpu(100);
        // document replication index, but the shard is on a remote store node: it behaves as segment replication
        localShard(false, ReplicationType.DOCUMENT, remoteStoreNodeSettings());
        assertTrue(indexShard.indexSettings().isSegRepEnabledOrRemoteNode());
        ForceMergeRequest request = new ForceMergeRequest("test");

        action.shardOperation(request, shard(0));

        verify(indexShard, times(1)).forceMerge(request);
        verifyNoInteractions(resourceUsageCollectorService);
        assertEquals(0, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testSegmentReplicationPrimaryIsAdmissionControlled() throws IOException {
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 50));
        setLocalCpu(90);
        localShard(true, randomBoolean() ? ReplicationType.SEGMENT : ReplicationType.DOCUMENT, randomRemoteNodeSettings());

        expectThrows(OpenSearchRejectedExecutionException.class, () -> action.shardOperation(new ForceMergeRequest("test"), shard(0)));

        verify(indexShard, never()).forceMerge(any());
        assertEquals(1, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    public void testDocumentReplicationReplicaIsAdmissionControlled() throws IOException {
        // document replication replicas run their own Lucene force merge, so they must stay protected
        TransportForceMergeAction action = createAction(cpuSettings(AdmissionControlMode.ENFORCED, 50));
        setLocalCpu(90);
        localShard(false, ReplicationType.DOCUMENT, Settings.EMPTY);

        expectThrows(OpenSearchRejectedExecutionException.class, () -> action.shardOperation(new ForceMergeRequest("test"), shard(0)));

        verify(indexShard, never()).forceMerge(any());
        assertEquals(1, cpuController().getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType()));
    }

    /**
     * Stubs the local shard's current routing entry and index settings, which decide whether admission control applies.
     */
    private void localShard(boolean primary, ReplicationType replicationType, Settings nodeSettings) {
        ShardRouting routing = TestShardRouting.newShardRouting(new ShardId(INDEX, 0), "node", primary, ShardRoutingState.STARTED);
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, replicationType)
            .build();
        when(indexShard.routingEntry()).thenReturn(routing);
        when(indexShard.indexSettings()).thenReturn(IndexSettingsModule.newIndexSettings(INDEX, indexSettings, nodeSettings));
    }

    private static Settings remoteStoreNodeSettings() {
        return Settings.builder()
            .put(Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "seg-repo")
            .build();
    }

    private static Settings randomRemoteNodeSettings() {
        return randomBoolean() ? remoteStoreNodeSettings() : Settings.EMPTY;
    }

    private TransportForceMergeAction createAction(Settings settings) {
        clusterService.getClusterSettings().applySettings(settings);
        admissionControlService = new AdmissionControlService(settings, clusterService, threadPool, resourceUsageCollectorService, null);
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        return new TransportForceMergeAction(
            clusterService,
            transportService,
            indicesService,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            admissionControlService
        );
    }

    private static Settings cpuSettings(AdmissionControlMode mode, long forceMergeCpuLimit) {
        return Settings.builder()
            .put(CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), mode.getMode())
            .put(CpuBasedAdmissionControllerSettings.FORCE_MERGE_CPU_USAGE_LIMIT.getKey(), forceMergeCpuLimit)
            .build();
    }

    private void setLocalCpu(double cpuPercent) {
        String localNodeId = clusterService.state().nodes().getLocalNodeId();
        when(resourceUsageCollectorService.getNodeStatistics(localNodeId)).thenReturn(Optional.of(cpuStats(localNodeId, cpuPercent)));
    }

    private static NodeResourceUsageStats cpuStats(String nodeId, double cpuPercent) {
        return new NodeResourceUsageStats(nodeId, System.currentTimeMillis(), 0, cpuPercent, new IoUsageStats(0), 0);
    }

    private static ShardRouting shard(int id) {
        return TestShardRouting.newShardRouting(new ShardId(INDEX, id), "node", true, ShardRoutingState.STARTED);
    }

    private AdmissionController cpuController() {
        return admissionControlService.getAdmissionController(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
    }
}
