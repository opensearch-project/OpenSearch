/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetryOrFailOnClusterManagerFailOverTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private SnapshotsService snapshotsService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        Settings settings = Settings.builder().put("node.name", "test").build();

        snapshotsService = new SnapshotsService(
            settings,
            clusterService,
            mock(org.opensearch.cluster.metadata.IndexNameExpressionResolver.class),
            mock(org.opensearch.repositories.RepositoriesService.class),
            transportService,
            mock(org.opensearch.action.support.ActionFilters.class),
            null,
            new org.opensearch.indices.RemoteStoreSettings(Settings.EMPTY, clusterSettings),
            null
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testNotClusterManagerExceptionRunsFallback() {
        AtomicBoolean fallbackCalled = new AtomicBoolean(false);

        snapshotsService.retryOrFailOnClusterManagerFailOver(
            new NotClusterManagerException("test"),
            0,
            "test-source",
            () -> mock(ClusterStateUpdateTask.class),
            () -> fallbackCalled.set(true)
        );

        assertTrue(fallbackCalled.get());
    }

    public void testFailedToCommitRetriesWhenAttemptsRemain() throws Exception {
        CountDownLatch taskSubmitted = new CountDownLatch(1);
        AtomicBoolean fallbackCalled = new AtomicBoolean(false);

        snapshotsService.retryOrFailOnClusterManagerFailOver(new FailedToCommitClusterStateException("test"), 0, "test-source", () -> {
            taskSubmitted.countDown();
            return mock(ClusterStateUpdateTask.class);
        }, () -> fallbackCalled.set(true));

        assertTrue("Task factory should be invoked for retry", taskSubmitted.await(5, TimeUnit.SECONDS));
        assertFalse("Fallback should not be called when retries remain", fallbackCalled.get());
    }

    public void testFailedToCommitExhaustsFallback() {
        AtomicBoolean fallbackCalled = new AtomicBoolean(false);

        snapshotsService.retryOrFailOnClusterManagerFailOver(
            new FailedToCommitClusterStateException("test"),
            SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getDefault(Settings.EMPTY),
            "test-source",
            () -> mock(ClusterStateUpdateTask.class),
            () -> fallbackCalled.set(true)
        );

        assertTrue(fallbackCalled.get());
    }

    public void testUnexpectedExceptionRunsFallback() {
        AtomicBoolean fallbackCalled = new AtomicBoolean(false);

        AssertionError ae = expectThrows(
            AssertionError.class,
            () -> snapshotsService.retryOrFailOnClusterManagerFailOver(
                new RuntimeException("unexpected"),
                0,
                "test-source",
                () -> mock(ClusterStateUpdateTask.class),
                () -> fallbackCalled.set(true)
            )
        );
        assertTrue(ae.getMessage().contains("Unexpected failure during cluster state update"));
        assertTrue("Fallback should run before the assert fires", fallbackCalled.get());
    }

    public void testRetryCreatesNewTaskInstance() throws Exception {
        CountDownLatch taskSubmitted = new CountDownLatch(1);
        AtomicInteger factoryCalls = new AtomicInteger(0);

        snapshotsService.retryOrFailOnClusterManagerFailOver(new FailedToCommitClusterStateException("test"), 0, "test-source", () -> {
            factoryCalls.incrementAndGet();
            taskSubmitted.countDown();
            return mock(ClusterStateUpdateTask.class);
        }, () -> {});

        assertTrue(taskSubmitted.await(5, TimeUnit.SECONDS));
        assertEquals(1, factoryCalls.get());
    }

    public void testWrappedFailedToCommitRetries() throws Exception {
        CountDownLatch taskSubmitted = new CountDownLatch(1);
        AtomicBoolean fallbackCalled = new AtomicBoolean(false);
        Exception wrapped = new RuntimeException("wrapper", new FailedToCommitClusterStateException("inner"));

        snapshotsService.retryOrFailOnClusterManagerFailOver(wrapped, 0, "test-source", () -> {
            taskSubmitted.countDown();
            return mock(ClusterStateUpdateTask.class);
        }, () -> fallbackCalled.set(true));

        assertTrue(taskSubmitted.await(5, TimeUnit.SECONDS));
        assertFalse(fallbackCalled.get());
    }

    public void testWrappedNotClusterManagerRunsFallback() {
        AtomicBoolean fallbackCalled = new AtomicBoolean(false);
        Exception wrapped = new RuntimeException("wrapper", new NotClusterManagerException("inner"));

        snapshotsService.retryOrFailOnClusterManagerFailOver(
            wrapped,
            0,
            "test-source",
            () -> mock(ClusterStateUpdateTask.class),
            () -> fallbackCalled.set(true)
        );

        assertTrue(fallbackCalled.get());
    }

    public void testRetryAttemptOneHasCorrectBackoff() throws Exception {
        CountDownLatch taskSubmitted = new CountDownLatch(1);

        long start = System.currentTimeMillis();
        snapshotsService.retryOrFailOnClusterManagerFailOver(new FailedToCommitClusterStateException("test"), 0, "test-source", () -> {
            taskSubmitted.countDown();
            return mock(ClusterStateUpdateTask.class);
        }, () -> {});

        assertTrue(taskSubmitted.await(5, TimeUnit.SECONDS));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue("Backoff should be at least ~1s, was " + elapsed + "ms", elapsed >= 900L);
    }

    public void testComputeBackoffIsExponential() {
        TimeValue base = TimeValue.timeValueSeconds(1);
        assertEquals(TimeValue.timeValueSeconds(1), SnapshotsService.computeBackoff(base, 0));
        assertEquals(TimeValue.timeValueSeconds(2), SnapshotsService.computeBackoff(base, 1));
        assertEquals(TimeValue.timeValueSeconds(4), SnapshotsService.computeBackoff(base, 2));
    }

    public void testComputeBackoffDoesNotOverflowAndIsClamped() {
        TimeValue base = TimeValue.timeValueSeconds(1);
        TimeValue delay = SnapshotsService.computeBackoff(base, 1000);
        assertTrue("Delay must be positive, was " + delay, delay.millis() > 0);
        assertTrue("Delay must be clamped to <= 1 day, was " + delay, delay.millis() <= TimeValue.timeValueDays(1).millis());
    }

    /**
     * Regression test: verifies createStateWithoutSnapshotV2Task reads from currentState (not captured outer state).
     */
    public void testStateWithoutSnapshotV2ReadsLiveState() throws Exception {
        Snapshot v2Snapshot = new Snapshot("repo", new SnapshotId("v2-snap", UUIDs.randomBase64UUID()));
        Snapshot normalSnapshot = new Snapshot("repo", new SnapshotId("normal-snap", UUIDs.randomBase64UUID()));

        SnapshotsInProgress.Entry v2Entry = SnapshotsInProgress.startedEntry(
            v2Snapshot,
            true,
            false,
            Collections.emptyList(),
            Collections.emptyList(),
            1L,
            1L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Version.CURRENT,
            false,
            true
        );
        SnapshotsInProgress.Entry normalEntry = SnapshotsInProgress.startedEntry(
            normalSnapshot,
            true,
            false,
            Collections.emptyList(),
            Collections.emptyList(),
            1L,
            1L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Version.CURRENT,
            false
        );

        String localNodeId = UUIDs.randomBase64UUID();
        ClusterState currentState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNodeId).clusterManagerNodeId(localNodeId).build())
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(v2Entry, normalEntry)))
            .build();

        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 0);

        ClusterState result = task.execute(currentState);

        SnapshotsInProgress resultSnapshots = result.custom(SnapshotsInProgress.TYPE);
        assertNotNull(resultSnapshots);
        assertEquals(1, resultSnapshots.entries().size());
        assertFalse(resultSnapshots.entries().get(0).remoteStoreIndexShallowCopyV2());
        assertEquals(normalSnapshot.getSnapshotId(), resultSnapshots.entries().get(0).snapshot().getSnapshotId());
    }
}
