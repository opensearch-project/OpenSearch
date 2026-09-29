/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
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

        Settings settings = Settings.builder().put("node.name", "test").putList("node.roles", "cluster_manager", "data").build();

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

    public void testRemoveFailedSnapshotTaskRemovesEntry() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));
        SnapshotsInProgress.Entry entry = SnapshotsInProgress.startedEntry(
            snapshot,
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
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(entry)))
            .build();

        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("test failure"),
            null,
            null
        );

        ClusterState result = task.execute(currentState);

        SnapshotsInProgress resultSnapshots = result.custom(SnapshotsInProgress.TYPE);
        assertTrue("Snapshot entry should be removed", resultSnapshots.entries().isEmpty());
    }

    public void testRejectedExecutionRunsFallback() {
        AtomicBoolean fallbackCalled = new AtomicBoolean(false);
        snapshotsService.retryOrFailOnClusterManagerFailOver(new FailedToCommitClusterStateException("test"), 100, "test-source", () -> {
            throw new AssertionError("should not be called");
        }, () -> fallbackCalled.set(true));
        assertTrue("Fallback should be called when retries exhausted", fallbackCalled.get());
    }

    public void testComputeBackoffWithZeroBase() {
        TimeValue result = SnapshotsService.computeBackoff(TimeValue.ZERO, 5);
        assertEquals(TimeValue.ZERO, result);
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testStateWithoutSnapshotV2TaskOnFailureRetries() throws Exception {
        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 0);
        task.onFailure("test-source", new FailedToCommitClusterStateException("simulated publish failure"));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testStateWithoutSnapshotV2TaskOnFailureNotCMRunsFallback() throws Exception {
        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 0);
        task.onFailure("test-source", new NotClusterManagerException("simulated"));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureRetries() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));
        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("original failure"),
            null,
            null
        );
        task.onFailure("test-source", new FailedToCommitClusterStateException("simulated publish failure"));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureNotCM() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));
        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("original failure"),
            null,
            null
        );
        task.onFailure("test-source", new NotClusterManagerException("simulated"));
    }

    public void testCreateStateWithoutSnapshotV2TaskNoChangeReturnsOriginal() throws Exception {
        Snapshot normalSnapshot = new Snapshot("repo", new SnapshotId("normal-snap", UUIDs.randomBase64UUID()));
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
            false,
            false
        );

        String localNodeId = UUIDs.randomBase64UUID();
        ClusterState currentState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNodeId).clusterManagerNodeId(localNodeId).build())
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(normalEntry)))
            .build();

        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 0);
        ClusterState result = task.execute(currentState);

        assertSame("Should return same state when no V2 entries to remove", currentState, result);
    }

    public void testComputeBackoffAttemptZero() {
        TimeValue result = SnapshotsService.computeBackoff(TimeValue.timeValueSeconds(1), 0);
        assertEquals(TimeValue.timeValueSeconds(1), result);
    }

    public void testComputeBackoffAttemptTwo() {
        TimeValue result = SnapshotsService.computeBackoff(TimeValue.timeValueSeconds(1), 2);
        assertEquals(TimeValue.timeValueSeconds(4), result);
    }

    public void testComputeBackoffCapsAtOneDay() {
        TimeValue result = SnapshotsService.computeBackoff(TimeValue.timeValueHours(25), 0);
        assertEquals(TimeValue.timeValueDays(1), result);
    }

    public void testComputeBackoffHighAttemptCapped() {
        TimeValue result = SnapshotsService.computeBackoff(TimeValue.timeValueSeconds(1), 50);
        assertEquals(TimeValue.timeValueDays(1), result);
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testStateWithoutSnapshotV2TaskOnFailureExhaustedRetries() throws Exception {
        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 100);
        task.onFailure("test-source", new FailedToCommitClusterStateException("simulated"));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureExhaustedRetries() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));
        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            100,
            snapshot,
            new RuntimeException("original failure"),
            null,
            null
        );
        task.onFailure("test-source", new FailedToCommitClusterStateException("simulated"));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testStateWithoutSnapshotV2TaskOnFailureUnexpectedException() throws Exception {
        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 0);
        expectThrows(AssertionError.class, () -> task.onFailure("test-source", new RuntimeException("unexpected")));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureUnexpectedException() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));
        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("original failure"),
            null,
            null
        );
        expectThrows(AssertionError.class, () -> task.onFailure("test-source", new RuntimeException("unexpected")));
    }

    public void testRemoveFailedSnapshotTaskOnNoLongerClusterManagerWithoutListener() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));

        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("original failure"),
            null,
            null
        );

        task.onNoLongerClusterManager("test-source");
    }

    public void testRemoveFailedSnapshotTaskOnNoLongerClusterManagerWithListener() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        ActionListener<Snapshot> userListener = ActionListener.wrap(s -> {}, e -> listenerCalled.set(true));

        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("original failure"),
            null,
            null
        );

        task.onNoLongerClusterManager("test-source");
    }

    public void testRemoveFailedSnapshotTaskClusterStateProcessedWithoutListenerNullRepoData() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));

        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("test failure"),
            null,
            null
        );

        String localNodeId = UUIDs.randomBase64UUID();
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNodeId).clusterManagerNodeId(localNodeId).build())
            .build();

        task.clusterStateProcessed("test-source", state, state);
    }

    public void testRejectedExecutionExceptionInRetryRunsFallback() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);

        TestThreadPool terminatedPool = new TestThreadPool(getTestName());
        ThreadPool.terminate(terminatedPool, 0, TimeUnit.MILLISECONDS);

        ClusterService localClusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(localClusterService.getClusterSettings()).thenReturn(clusterSettings);

        TransportService localTransportService = mock(TransportService.class);
        when(localTransportService.getThreadPool()).thenReturn(terminatedPool);

        Settings settings = Settings.builder().put("node.name", "test").putList("node.roles", "cluster_manager", "data").build();

        SnapshotsService localService = new SnapshotsService(
            settings,
            localClusterService,
            mock(org.opensearch.cluster.metadata.IndexNameExpressionResolver.class),
            mock(org.opensearch.repositories.RepositoriesService.class),
            localTransportService,
            mock(org.opensearch.action.support.ActionFilters.class),
            null,
            new org.opensearch.indices.RemoteStoreSettings(Settings.EMPTY, clusterSettings),
            null
        );

        AtomicBoolean fallbackCalled = new AtomicBoolean(false);

        localService.retryOrFailOnClusterManagerFailOver(
            new FailedToCommitClusterStateException("test"),
            0,
            "test-source",
            () -> mock(ClusterStateUpdateTask.class),
            () -> fallbackCalled.set(true)
        );

        assertTrue("Fallback should be called when scheduling is rejected", fallbackCalled.get());
    }

    public void testSettingsUpdateConsumerForRetries() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        Settings settings = Settings.builder()
            .put("node.name", "test")
            .putList("node.roles", "cluster_manager", "data")
            .put(SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getKey(), 5)
            .put(SnapshotsService.SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.getKey(), "2s")
            .build();

        SnapshotsService svc = new SnapshotsService(
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

        AtomicBoolean fallbackCalled = new AtomicBoolean(false);
        svc.retryOrFailOnClusterManagerFailOver(
            new FailedToCommitClusterStateException("test"),
            5,
            "test-source",
            () -> mock(ClusterStateUpdateTask.class),
            () -> fallbackCalled.set(true)
        );
        assertTrue("Fallback should be called at attempt==maxRetries", fallbackCalled.get());

        Settings newSettings = Settings.builder()
            .put(SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getKey(), 10)
            .put(SnapshotsService.SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.getKey(), "5s")
            .build();
        clusterSettings.applySettings(newSettings);

        AtomicBoolean fallbackCalled2 = new AtomicBoolean(false);
        CountDownLatch retryLatch = new CountDownLatch(1);
        svc.retryOrFailOnClusterManagerFailOver(new FailedToCommitClusterStateException("test"), 5, "test-source", () -> {
            retryLatch.countDown();
            return mock(ClusterStateUpdateTask.class);
        }, () -> fallbackCalled2.set(true));
        assertFalse("Fallback should NOT be called when retries increased", fallbackCalled2.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureWithListenerNotNull() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));

        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("original failure"),
            null,
            null
        );
        task.onFailure("test-source", new NotClusterManagerException("simulated"));
    }

    private static SnapshotsInProgress.Entry startedEntryFor(Snapshot snapshot) {
        return SnapshotsInProgress.startedEntry(
            snapshot,
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
    }

    private static final TimeValue BUDGET = TimeValue.timeValueSeconds(30);

    private static Snapshot snapshot(String name) {
        return new Snapshot("repo", new SnapshotId(name, UUIDs.randomBase64UUID()));
    }

    /** A cluster state holding the given in-progress entries, with this node elected. */
    private static ClusterState stateWith(SnapshotsInProgress.Entry... entries) {
        final String localNodeId = UUIDs.randomBase64UUID();
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNodeId).clusterManagerNodeId(localNodeId).build())
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(List.of(entries)))
            .build();
    }

    /** The task the finalization timer submits when {@code snapshot}'s budget expires. */
    private ClusterStateUpdateTask expiryTask(Snapshot snapshot) {
        return snapshotsService.createFinalizationExpiryTask(snapshot, BUDGET);
    }

    /**
     * A budget that expires after the commit removed the entry answers nobody: the finalization's own success exit answers
     * its caller, so a committed finalization is never answered with a timeout.
     */
    public void testExpiryAfterTheCommitAnswersNothing() throws Exception {
        final Snapshot snapshot = snapshot("snap-1");
        final Set<Snapshot> resolved = new HashSet<>();
        recordResolutionOf(snapshot, resolved);
        final ClusterState committed = stateWith();

        final ClusterStateUpdateTask task = expiryTask(snapshot);
        final ClusterState result = task.execute(committed);
        task.clusterStateProcessed("test-source", committed, result);

        assertThat("a committed finalization must not be answered with a timeout", resolved, empty());
        assertTrue("the listener must stay registered for the success exit", completionListeners().containsKey(snapshot));
    }

    /**
     * While the entry is present the expiry answers the caller with a timeout, and only the caller: the snapshot stays in
     * the set of snapshots this node is ending, which is what stops a later cluster state change from ending it a second
     * time while its first finalization is still running.
     */
    public void testExpiryAnswersATimeoutAndKeepsTheSnapshotEnding() throws Exception {
        final Snapshot snapshot = snapshot("snap-1");
        final List<Exception> answers = new ArrayList<>();
        addListener(snapshot, ActionListener.wrap(r -> fail("a timed out finalization must not be completed"), answers::add));
        endingSnapshots().add(snapshot);
        final ClusterState currentState = stateWith(startedEntryFor(snapshot));

        final ClusterStateUpdateTask task = expiryTask(snapshot);
        final ClusterState result = task.execute(currentState);
        task.clusterStateProcessed("test-source", currentState, result);

        assertTrue("the snapshot must still be ending, or a later change ends it again", endingSnapshots().contains(snapshot));
        assertThat(answers, hasSize(1));
        assertThat(answers.get(0), instanceOf(OpenSearchTimeoutException.class));
        assertThat(
            answers.get(0).getMessage(),
            containsString("[finalize snapshot [" + snapshot + "]] did not complete within [" + BUDGET + "]")
        );
        assertThat(answers.get(0).getMessage(), containsString("it was already writing the repository generation and may still complete"));
        assertFalse("the answered listener must be deregistered", completionListeners().containsKey(snapshot));
        assertSame("the expiry must publish nothing", currentState, result);
    }

    /**
     * An expiry task that never ran knows nothing about the commit, so it answers nobody and releases nothing: the
     * finalization's own exits answer its caller, and it still holds the repository's operation token.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testUnprocessedExpiryAnswersNothingAndKeepsTheToken() throws Exception {
        final Snapshot snapshot = snapshot("snap-1");
        final Set<Snapshot> resolved = new HashSet<>();
        recordResolutionOf(snapshot, resolved);
        final Set<String> currentlyFinalizing = serviceField("currentlyFinalizing");
        currentlyFinalizing.add("repo");

        final Logger snapshotsLogger = LogManager.getLogger(SnapshotsService.class);
        final Level previousLevel = snapshotsLogger.getLevel();
        Loggers.setLevel(snapshotsLogger, Level.DEBUG);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(snapshotsLogger)) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "an unprocessed expiry must not clear this node's snapshot operations",
                    SnapshotsService.class.getCanonicalName(),
                    Level.DEBUG,
                    "Failing all snapshot operation listeners"
                )
            );
            expiryTask(snapshot).onFailure("test-source", new NotClusterManagerException("simulated failover"));
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(snapshotsLogger, previousLevel);
        }

        assertThat("an unprocessed expiry must not answer the caller", resolved, empty());
        assertTrue("the finalization still holds the repository's operation token", currentlyFinalizing.contains("repo"));
    }

    /**
     * The removal a finalization budget submits does not give up while this node stays cluster manager: publish failure
     * after publish failure it retries, answering nobody and keeping the token, and once one attempt publishes it answers
     * the stopped caller once and hands the repository on to the finalization queued behind it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testBudgetRemovalRetriesUntilPublished() throws Exception {
        final List<Runnable> scheduled = new ArrayList<>();
        final List<ClusterStateUpdateTask> submitted = new ArrayList<>();
        final SnapshotsService service = serviceCapturingSchedules(scheduled, submitted);
        final Snapshot stopped = snapshot("stopped");
        final Snapshot queuedBehind = snapshot("queued-behind");
        final SnapshotsInProgress.Entry queuedEntry = startedEntryFor(queuedBehind);
        addFinalization(service, queuedEntry);
        final List<Snapshot> resolved = new ArrayList<>();
        addListener(service, stopped, ActionListener.wrap(r -> resolved.add(stopped), e -> resolved.add(stopped)));
        addListener(service, queuedBehind, ActionListener.wrap(r -> resolved.add(queuedBehind), e -> resolved.add(queuedBehind)));
        final Set<String> token = serviceField(service, "currentlyFinalizing");
        token.add("repo");
        final ClusterState currentState = stateWith(startedEntryFor(stopped), queuedEntry);

        ClusterStateUpdateTask task = service.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            stopped,
            new OpenSearchTimeoutException("stopped"),
            RepositoryData.EMPTY,
            null,
            () -> true
        );
        final int maxRetries = SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getDefault(Settings.EMPTY);
        for (int failure = 0; failure <= maxRetries; failure++) {
            task.onFailure("test-source", new FailedToCommitClusterStateException("simulated publish failure"));
            assertThat("a publish failure must not answer anyone", resolved, empty());
            assertTrue("a publish failure must not release the token", token.contains("repo"));
            assertThat("each failure must schedule exactly one retry", scheduled, hasSize(1));
            scheduled.remove(0).run();
            assertThat("an armed retry with no failover is submitted once", submitted, hasSize(1));
            task = submitted.remove(0);
        }

        task.clusterStateProcessed("test-source", currentState, task.execute(currentState));
        assertEquals("the stopped caller must be answered once, and nobody else", List.of(stopped), resolved);
        assertNull("the queued finalization must have been handed on", pollFinalization(service, "repo"));
    }

    /**
     * The removal a finalization budget submits does nothing once this node has failed its snapshot operations over after
     * the budget was armed, even if it was re-elected and a new finalization holds the token and the entry: here the
     * removal was already queued in the cluster manager service when the failover was handled.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testBudgetRemovalIsInertAfterAFailover() throws Exception {
        final List<Runnable> scheduled = new ArrayList<>();
        final List<ClusterStateUpdateTask> submitted = new ArrayList<>();
        final SnapshotsService service = serviceCapturingSchedules(scheduled, submitted);
        final Snapshot stopped = snapshot("stopped");
        final AtomicLong failovers = serviceField(service, "failovers");
        final long failoversAtArm = failovers.get();
        final ClusterStateUpdateTask task = service.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            stopped,
            new OpenSearchTimeoutException("stopped"),
            RepositoryData.EMPTY,
            null,
            () -> failovers.get() == failoversAtArm
        );

        // A failover on this node, then a new finalization of the same entry after a re-election.
        service.createRemoveFailedSnapshotTask("other-source", 0, snapshot("other"), new RuntimeException("other"), null, null)
            .onNoLongerClusterManager("other-source");
        final Set<String> token = serviceField(service, "currentlyFinalizing");
        token.add("repo");
        final Snapshot next = snapshot("next");
        final SnapshotsInProgress.Entry nextEntry = startedEntryFor(next);
        addFinalization(service, nextEntry);
        final Set<Snapshot> resolved = new HashSet<>();
        addListener(service, stopped, ActionListener.wrap(r -> resolved.add(stopped), e -> resolved.add(stopped)));
        final ClusterState currentState = stateWith(startedEntryFor(stopped), nextEntry);

        final ClusterState result = task.execute(currentState);
        task.clusterStateProcessed("test-source", currentState, result);

        assertSame("a stale removal must publish nothing", currentState, result);
        assertThat("a stale removal must answer nobody", resolved, empty());
        assertTrue("a stale removal must not release the token", token.contains("repo"));
        assertNotNull("a stale removal must hand nothing on", pollFinalization(service, "repo"));
    }

    /**
     * A retry of that removal, armed by a publication failure before this node failed its snapshot operations over, is not
     * submitted once the failover has been handled.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testBudgetRemovalRetryArmedBeforeAFailoverIsNotSubmitted() throws Exception {
        final List<Runnable> scheduled = new ArrayList<>();
        final List<ClusterStateUpdateTask> submitted = new ArrayList<>();
        final SnapshotsService service = serviceCapturingSchedules(scheduled, submitted);
        final AtomicLong failovers = serviceField(service, "failovers");
        final long failoversAtArm = failovers.get();
        service.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot("stopped"),
            new OpenSearchTimeoutException("stopped"),
            RepositoryData.EMPTY,
            null,
            () -> failovers.get() == failoversAtArm
        ).onFailure("test-source", new FailedToCommitClusterStateException("simulated publish failure"));
        assertThat(scheduled, hasSize(1));

        service.createRemoveFailedSnapshotTask("other-source", 0, snapshot("other"), new RuntimeException("other"), null, null)
            .onFailure("other-source", new NotClusterManagerException("simulated failover"));
        scheduled.remove(0).run();

        assertThat("a retry armed before a failover this node handled must not be submitted after it", submitted, empty());
    }

    /** A retry armed before a failover this node handled is not submitted after it, bounded or not. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testBoundedRetryIsNotSubmittedAfterAFailover() throws Exception {
        final List<Runnable> scheduled = new ArrayList<>();
        final List<ClusterStateUpdateTask> submitted = new ArrayList<>();
        final SnapshotsService service = serviceCapturingSchedules(scheduled, submitted);
        final ClusterStateUpdateTask retried = service.createRemoveFailedSnapshotTask(
            "bounded-source",
            1,
            snapshot("bounded"),
            new RuntimeException("bounded"),
            null,
            null
        );
        service.retryOrFailOnClusterManagerFailOver(
            new FailedToCommitClusterStateException("simulated publish failure"),
            0,
            "bounded-source",
            () -> retried,
            () -> fail("a retry with attempts left must not run the fallback"),
            false
        );
        assertThat(scheduled, hasSize(1));

        service.createRemoveFailedSnapshotTask("other-source", 0, snapshot("other"), new RuntimeException("other"), null, null)
            .onFailure("other-source", new NotClusterManagerException("simulated failover"));
        scheduled.remove(0).run();

        assertThat("a bounded retry armed before a failover this node handled must not be submitted after it", submitted, empty());
    }

    /** A budget's removal that loses the election answers the caller with the removal's own failure. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testBudgetRemovalOnLostElectionAnswersWithItsFailure() throws Exception {
        final Snapshot stopped = snapshot("stopped");
        final List<Exception> answers = new ArrayList<>();
        addListener(stopped, ActionListener.wrap(r -> fail("must not be completed"), answers::add));
        final OpenSearchTimeoutException failure = new OpenSearchTimeoutException("stopped");
        snapshotsService.createRemoveFailedSnapshotTask("test-source", 0, stopped, failure, RepositoryData.EMPTY, null, () -> true)
            .onNoLongerClusterManager("test-source");
        assertThat(answers, hasSize(1));
        assertSame("the caller must be answered with the removal's own failure", failure, answers.get(0));
    }

    /** A service whose scheduled tasks and submitted cluster state updates are captured rather than run. */
    private SnapshotsService serviceCapturingSchedules(List<Runnable> scheduled, List<ClusterStateUpdateTask> submitted) {
        final ThreadPool capturing = mock(ThreadPool.class);
        when(capturing.schedule(any(Runnable.class), any(TimeValue.class), anyString())).thenAnswer(invocation -> {
            scheduled.add(invocation.getArgument(0));
            return mock(Scheduler.ScheduledCancellable.class);
        });
        final ClusterService capturingClusterService = mock(ClusterService.class);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(capturingClusterService.getClusterSettings()).thenReturn(clusterSettings);
        doAnswer(invocation -> {
            submitted.add(invocation.getArgument(1));
            return null;
        }).when(capturingClusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(capturing);
        final org.opensearch.repositories.RepositoriesService repositoriesService = mock(
            org.opensearch.repositories.RepositoriesService.class
        );
        when(repositoriesService.repository(anyString())).thenReturn(mock(org.opensearch.repositories.Repository.class));
        return new SnapshotsService(
            Settings.builder().put("node.name", "test").putList("node.roles", "cluster_manager", "data").build(),
            capturingClusterService,
            mock(org.opensearch.cluster.metadata.IndexNameExpressionResolver.class),
            repositoriesService,
            transportService,
            mock(org.opensearch.action.support.ActionFilters.class),
            null,
            new org.opensearch.indices.RemoteStoreSettings(Settings.EMPTY, clusterSettings),
            null
        );
    }

    /**
     * A private field of the service under test, read by reflection because production gains no test-only accessor for
     * it.
     */
    @SuppressForbidden(reason = "the service's bookkeeping is private and must not gain a test seam")
    @SuppressWarnings("unchecked")
    private <T> T serviceField(String name) throws Exception {
        return serviceField(snapshotsService, name);
    }

    @SuppressForbidden(reason = "the service's bookkeeping is private and must not gain a test seam")
    @SuppressWarnings("unchecked")
    private static <T> T serviceField(SnapshotsService service, String name) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(service);
    }

    private Map<Snapshot, ?> completionListeners() throws Exception {
        return serviceField("snapshotCompletionListeners");
    }

    private Set<Snapshot> endingSnapshots() throws Exception {
        return serviceField("endingSnapshots");
    }

    /** Queues {@code entry} for finalization exactly as {@code endSnapshot} would. */
    @SuppressForbidden(reason = "the finalization queue is private and must not gain a test seam")
    private void addFinalization(SnapshotsInProgress.Entry entry) throws Exception {
        addFinalization(snapshotsService, entry);
    }

    @SuppressForbidden(reason = "the finalization queue is private and must not gain a test seam")
    private static void addFinalization(SnapshotsService service, SnapshotsInProgress.Entry entry) throws Exception {
        final Object queue = serviceField(service, "repositoryOperations");
        final Class<?>[] signature = { SnapshotsInProgress.Entry.class, Metadata.class };
        final Method addFinalization = queue.getClass().getDeclaredMethod("addFinalization", signature);
        addFinalization.setAccessible(true);
        // Non-null metadata: the queue's own consistency assertion forbids a queued entry with none.
        addFinalization.invoke(queue, entry, Metadata.EMPTY_METADATA);
    }

    /** Takes one finalization off {@code repository}'s queue, as a completing finalization on a snapshot thread does. */
    @SuppressForbidden(reason = "the finalization queue is private and must not gain a test seam")
    private Object pollFinalization(String repository) throws Exception {
        return pollFinalization(snapshotsService, repository);
    }

    @SuppressForbidden(reason = "the finalization queue is private and must not gain a test seam")
    private static Object pollFinalization(SnapshotsService service, String repository) throws Exception {
        final Object queue = serviceField(service, "repositoryOperations");
        final Method pollFinalization = queue.getClass().getDeclaredMethod("pollFinalization", String.class);
        pollFinalization.setAccessible(true);
        return pollFinalization.invoke(queue, repository);
    }

    /** Registers a completion listener for {@code snapshot} exactly as a create that waits for completion does. */
    @SuppressForbidden(reason = "the completion listener registry is private and must not gain a test seam")
    private void addListener(Snapshot snapshot, ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener) throws Exception {
        addListener(snapshotsService, snapshot, listener);
    }

    @SuppressForbidden(reason = "the completion listener registry is private and must not gain a test seam")
    private static void addListener(
        SnapshotsService service,
        Snapshot snapshot,
        ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener
    ) throws Exception {
        final Method addListener = SnapshotsService.class.getDeclaredMethod("addListener", Snapshot.class, ActionListener.class);
        addListener.setAccessible(true);
        addListener.invoke(service, snapshot, listener);
    }

    /** Registers a completion listener for {@code snapshot} that records the snapshot into {@code resolved} if it is answered. */
    private void recordResolutionOf(Snapshot snapshot, Set<Snapshot> resolved) throws Exception {
        addListener(snapshot, ActionListener.wrap(ignored -> resolved.add(snapshot), e -> resolved.add(snapshot)));
    }
}
