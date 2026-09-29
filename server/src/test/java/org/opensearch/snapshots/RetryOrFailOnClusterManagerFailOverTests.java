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
import org.opensearch.cluster.SnapshotDeletionsInProgress;
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
import org.opensearch.repositories.RepositoryException;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryOrFailOnClusterManagerFailOverTests extends OpenSearchTestCase {

    // The lowest value snapshot.cleanup.retry_backoff accepts. A bounded "no retry was scheduled" wait is only
    // evidence if it outlasts the delay a real retry would use, and at the 1s default it does not.
    private static final String MIN_RETRY_BACKOFF = "100ms";
    private static final long NO_RETRY_WAIT_MILLIS = 500L;

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private SnapshotsService snapshotsService;

    // The settings instance the service under test registered its dynamic-settings consumers on. Two tests below
    // deliberately build a second service over their own instance; those locals shadow this field.
    private ClusterSettings clusterSettings;

    // Records what the retry helper resubmits. It submits from inside threadPool.schedule(..., GENERIC) after a
    // backoff, so a submit is only ever observable asynchronously: a verification taken straight after onFailure
    // returns cannot fail in either direction, whether or not a retry was scheduled.
    private CountDownLatch retrySubmitted;
    private final AtomicInteger retrySubmits = new AtomicInteger(0);
    private final AtomicReference<String> retrySource = new AtomicReference<>();
    private final AtomicReference<ClusterStateUpdateTask> retryTask = new AtomicReference<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        retrySubmitted = new CountDownLatch(1);
        doAnswer(invocation -> {
            retrySource.set(invocation.getArgument(0));
            retryTask.set(invocation.getArgument(1));
            retrySubmits.incrementAndGet();
            retrySubmitted.countDown();
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));

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

    /**
     * Retunes the service built by setUp to the minimum backoff the setting allows, through the same dynamic
     * update consumer production uses. Call this before driving a failure whose outcome is "nothing was
     * scheduled": the wait below is 5x this backoff, so a retry that does happen is seen.
     */
    private void useMinimumRetryBackoff() {
        clusterSettings.applySettings(
            Settings.builder().put(SnapshotsService.SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.getKey(), MIN_RETRY_BACKOFF).build()
        );
    }

    /**
     * Asserts that exactly one retry was resubmitted, under the same source string, as a task instance distinct
     * from the one that failed.
     */
    private void assertRetryScheduled(String source, ClusterStateUpdateTask failedTask) throws InterruptedException {
        assertTrue("a retry must be submitted", retrySubmitted.await(5, TimeUnit.SECONDS));
        assertEquals("the retry must resubmit under the source it was given", source, retrySource.get());
        assertEquals("exactly one retry per failure", 1, retrySubmits.get());
        assertNotSame("a retry must be a fresh instance, not the task that just failed", failedTask, retryTask.get());
    }

    /**
     * Asserts that no cluster state task was submitted. Only meaningful after {@link #useMinimumRetryBackoff()},
     * which is what makes the wait longer than a scheduled retry's delay rather than shorter.
     */
    private void assertNoTaskSubmitted(String why) throws InterruptedException {
        assertFalse(why, retrySubmitted.await(NO_RETRY_WAIT_MILLIS, TimeUnit.MILLISECONDS));
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

    public void testExhaustedRetriesFarPastMaxRunsFallback() {
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
        useMinimumRetryBackoff();
        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 0);
        task.onFailure("test-source", new FailedToCommitClusterStateException("simulated publish failure"));
        // Catches a v2 onFailure that logs and gives up on a publish failure instead of retrying. This site's
        // fallback only logs, so the resubmit is the one observable it has.
        assertRetryScheduled("test-source", task);
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testStateWithoutSnapshotV2TaskOnFailureNotCMRunsFallback() throws Exception {
        useMinimumRetryBackoff();
        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 0);
        task.onFailure("test-source", new NotClusterManagerException("simulated"));
        // Catches making NotClusterManagerException retryable - moving that check below the retryable one, say -
        // which would resubmit a cluster state task on a node that has already lost the election.
        assertNoTaskSubmitted("a demoted node must not resubmit");
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureRetries() throws Exception {
        useMinimumRetryBackoff();
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
        // Catches the same fall-through on the create-side task, and one more: a retry supplier that hands back the
        // instance that just failed rather than calling the factory again. The task accumulates per-attempt state
        // in its own fields, so a reused instance re-runs against the previous attempt's contents.
        assertRetryScheduled("test-source", task);
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureNotCM() throws Exception {
        useMinimumRetryBackoff();
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
        // As the v2 sibling above, on the create-side task.
        assertNoTaskSubmitted("a demoted node must not resubmit");
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
        useMinimumRetryBackoff();
        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 100);
        task.onFailure("test-source", new FailedToCommitClusterStateException("simulated"));
        // Catches an off-by-one that turns attempt >= maxRetries into attempt > maxRetries: one attempt more than
        // configured.
        assertNoTaskSubmitted("no retry once the attempt budget is spent");
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureExhaustedRetries() throws Exception {
        useMinimumRetryBackoff();
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
        // As above, on the create-side task.
        assertNoTaskSubmitted("no retry once the attempt budget is spent");
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testStateWithoutSnapshotV2TaskOnFailureUnexpectedException() throws Exception {
        ClusterStateUpdateTask task = snapshotsService.createStateWithoutSnapshotV2Task("test-source", 0);
        AssertionError ae = expectThrows(AssertionError.class, () -> task.onFailure("test-source", new RuntimeException("unexpected")));
        // A message-less expectThrows(AssertionError.class, ...) passes on an AssertionError raised anywhere in the
        // call, including one that has nothing to do with this branch. The error this path must produce is the
        // retry helper's own, which is reachable here only because this site's fallback does nothing but log.
        assertTrue(ae.getMessage(), ae.getMessage().contains("Unexpected failure during cluster state update"));
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
        AssertionError ae = expectThrows(AssertionError.class, () -> task.onFailure("test-source", new RuntimeException("unexpected")));
        // Deliberately NOT the message its v2 sibling asserts, and that difference is the point: this site's
        // fallback calls failAllListenersOnMasterFailOver, whose else arm asserts on an exception that is neither a
        // publish failure nor a demotion, and the helper runs the fallback before reaching its own assert. So the
        // error that escapes is the failover handler's, and asserting the sibling's string here would be red.
        assertTrue(
            ae.getMessage(),
            ae.getMessage().contains("Modifying snapshot state should only ever fail because we failed to publish new state")
        );
    }

    public void testRemoveFailedSnapshotTaskOnNoLongerClusterManagerWithoutListener() throws Exception {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));
        RuntimeException failure = new RuntimeException("original failure");

        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask("test-source", 0, snapshot, failure, null, null);

        task.onNoLongerClusterManager("test-source");

        // Catches removing or reordering onNoLongerClusterManager's addSuppressed, which is the only record of why
        // the snapshot failed on a node that lost the election: the exception handed to the completion listeners is
        // the caller's own, and the demotion is written into it.
        assertEquals("the demotion must be recorded on the failure the caller supplied", 1, failure.getSuppressed().length);
        assertTrue(failure.getSuppressed()[0].getMessage(), failure.getSuppressed()[0].getMessage().contains("no longer cluster-manager"));
    }

    public void testRemoveFailedSnapshotTaskClusterStateProcessedWithoutListenerNullRepoData() throws Exception {
        useMinimumRetryBackoff();
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

        // With no listener and no repository data this callback resolves the completion listeners and stops; it has
        // no repository data to hand the repo loop on with. The first assertion catches a cluster state task
        // dispatched from this terminal callback, the second that it started an operation - entered the repo loop,
        // or registered a completion or delete listener - that nothing would ever resolve. Note what neither
        // catches, because it needs no assertion: dropping the repositoryData != null guard trips the precondition
        // at the head of runNextQueuedOperation, which propagates straight out of the call above.
        assertNoTaskSubmitted("a terminal callback with no repository data must not dispatch");
        assertTrue("no in-memory operation state may be left behind", snapshotsService.assertAllListenersResolved());
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

    // Catches an onFailure that switches on the exception's own type instead of unwrapping its causes, which would
    // schedule a retry on a node that has already lost the election.
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRemoveFailedSnapshotTaskOnFailureWrappedNotCMWithoutListener() throws Exception {
        useMinimumRetryBackoff();
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap-1", UUIDs.randomBase64UUID()));

        ClusterStateUpdateTask task = snapshotsService.createRemoveFailedSnapshotTask(
            "test-source",
            0,
            snapshot,
            new RuntimeException("original failure"),
            null,
            null
        );
        task.onFailure("test-source", new RuntimeException("wrapper", new NotClusterManagerException("simulated")));
        assertNoTaskSubmitted("a demotion wrapped in another exception is still a demotion");
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

    /**
     * The delete-side release of the per-delete bookkeeping is inside the fallback Runnable the retry helper owns, so
     * "released exactly once" and "the fallback ran exactly once" are the same proposition, and the second is countable here
     * because the helper takes that Runnable from its caller.
     * <p>
     * This drives all four branches of the helper with a counting fallback. The mistake it catches is a release that
     * fires per attempt: on the retryable branch the count would be 1 instead of 0. Each branch counts into its own
     * counter, so a 1 also excludes a fallback that runs twice inside one branch, which an AtomicBoolean cannot see.
     */
    public void testDeleteFallbackRunsOnceOnTerminalBranchesOnly() throws Exception {
        useMinimumRetryBackoff();
        final int maxRetries = SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getDefault(Settings.EMPTY);

        final AtomicInteger onDemotion = new AtomicInteger();
        snapshotsService.retryOrFailOnClusterManagerFailOver(new NotClusterManagerException("demoted"), 0, "test-source", () -> {
            throw new AssertionError("a demoted node must not build a retry task");
        }, onDemotion::incrementAndGet);
        assertEquals("a demotion is terminal, so the release runs exactly once", 1, onDemotion.get());

        final AtomicInteger onExhausted = new AtomicInteger();
        snapshotsService.retryOrFailOnClusterManagerFailOver(
            new FailedToCommitClusterStateException("publish failed"),
            maxRetries,
            "test-source",
            () -> {
                throw new AssertionError("a spent attempt budget must not build a retry task");
            },
            onExhausted::incrementAndGet
        );
        assertEquals("a spent attempt budget is terminal, so the release runs exactly once", 1, onExhausted.get());

        final AtomicInteger onUnexpected = new AtomicInteger();
        AssertionError ae = expectThrows(
            AssertionError.class,
            () -> snapshotsService.retryOrFailOnClusterManagerFailOver(
                new RuntimeException("neither a publish failure nor a demotion"),
                0,
                "test-source",
                () -> {
                    throw new AssertionError("an unexpected failure must not build a retry task");
                },
                onUnexpected::incrementAndGet
            )
        );
        assertTrue(ae.getMessage(), ae.getMessage().contains("Unexpected failure during cluster state update"));
        assertEquals("an unexpected failure is terminal, and releases before its assert fires", 1, onUnexpected.get());

        // The retryable branch goes last on purpose: it is the only one of the four that reaches
        // submitStateUpdateTask, and the latch this class observes submits through is armed once in setUp. Waiting for
        // that submit is what makes the count below evidence rather than a reading taken too early - the fallback is
        // invoked synchronously on every branch the helper has, so a zero once the retry has landed is a zero for good.
        final AtomicInteger onRetryable = new AtomicInteger();
        snapshotsService.retryOrFailOnClusterManagerFailOver(
            new FailedToCommitClusterStateException("publish failed"),
            0,
            "test-source",
            () -> mock(ClusterStateUpdateTask.class),
            onRetryable::incrementAndGet
        );
        assertTrue("a publish failure below the attempt cap must schedule a retry", retrySubmitted.await(5, TimeUnit.SECONDS));
        assertEquals("a retry must not release the delete, since only the terminal give-up path releases it", 0, onRetryable.get());
    }

    /**
     * A publish failure at attempt 0 with the flag on resubmits the delete-removal task, under the same source, as a fresh
     * instance, so the delete entry is not stranded in the cluster state;
     * {@code SnapshotResiliencyTests#testDeleteReleasesBookkeepingOnlyOnTerminalFailure} shows the retry releases nothing.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testConvertedDeleteOnFailureSchedulesRetryAndDoesNotFallBack() throws Exception {
        useMinimumRetryBackoff();
        // The literal the dispatcher hoists into a local, and therefore the string a retry has to resubmit under.
        final String source = "remove snapshot deletion metadata";
        final SnapshotDeletionsInProgress.Entry deleteEntry = new SnapshotDeletionsInProgress.Entry(
            List.of(new SnapshotId("snap-1", UUIDs.randomBase64UUID())),
            "repo",
            0L,
            1L,
            SnapshotDeletionsInProgress.State.STARTED
        );

        // The null-failure variant is the one where the delete itself succeeded on the repository and only the publish
        // of its removal failed, which is the case a retry saves.
        final ClusterStateUpdateTask task = snapshotsService.createRemoveSnapshotDeletionTask(
            source,
            0,
            deleteEntry,
            null,
            RepositoryData.EMPTY,
            null,
            true
        );

        task.onFailure(source, new FailedToCommitClusterStateException("simulated publish failure"));

        assertRetryScheduled(source, task);
        // Two calls with the same arguments: a factory keyed on attempt would pass a 0-versus-1 comparison and fail
        // this one.
        assertNotSame(
            "the factory must build a new task per call rather than hand back a cached one",
            snapshotsService.createRemoveSnapshotDeletionTask(source, 1, deleteEntry, null, RepositoryData.EMPTY, null, true),
            snapshotsService.createRemoveSnapshotDeletionTask(source, 1, deleteEntry, null, RepositoryData.EMPTY, null, true)
        );
        assertTrue("the retry path must leave no snapshot operation state behind", snapshotsService.assertAllListenersResolved());
    }

    /**
     * A give-up on one repository's publication must not fail a create parked behind a <em>different</em> repository's
     * abandoned delete. The give-up arm's fallback is {@code failAllListenersOnMasterFailOver}, which fails every completion
     * listener on the node rather than only this repository's, so the condition that suppresses the give-up has to be read
     * node-wide. Here repository {@code repo-a} is owed a rebind and the publication that fails belongs to {@code repo-b}.
     * <p>
     * A predicate scoped to this task's own repository would answer "nothing owed" for {@code repo-b}, the give-up would run at
     * the attempt ceiling, and the node-wide fallback would take {@code repo-a}'s create with it.
     * <p>
     * The arm is observed at the point it asks for a schedule rather than at the point the schedule fires. Its period is its own
     * cap -- eight seconds at this attempt -- so the suite's five-second submit latch would time out on a retry that was armed
     * perfectly correctly. Intercepting the schedule makes the test deterministic instead of merely faster.
     * <p>
     * The second case is a second delete on a repository that is <em>already</em> owed a rebind: a condition narrowed to "this
     * task recorded the debt" would conclude nothing is parked, when the debt an earlier delete recorded is exactly what leaves a
     * create parked, and the give-up would fail it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testExhaustedRetriesDoNotFailCreatesParkedForAnotherRepository() throws Exception {
        for (Tuple<String, String> owedAndDeleted : List.of(Tuple.tuple("repo-a", "repo-b"), Tuple.tuple("repo", "repo"))) {
            interceptedSchedule.set(null);
            interceptedFallbackRan.set(false);
            final SnapshotsService service = newInterceptedService();
            reconciliationOwed(service).add(owedAndDeleted.v1());
            final String source = "remove snapshot deletion metadata";
            final ClusterStateUpdateTask task = service.createRemoveSnapshotDeletionTask(
                source,
                SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getDefault(Settings.EMPTY),
                deletionOf(owedAndDeleted.v2()),
                null,
                RepositoryData.EMPTY,
                null,
                true
            );

            task.onFailure(source, new FailedToCommitClusterStateException("simulated publish failure"));

            assertNotNull(
                "a debt owed by ["
                    + owedAndDeleted.v1()
                    + "] must suppress the give-up for a delete on ["
                    + owedAndDeleted.v2()
                    + "], so a retry must be armed rather than the fallback run",
                interceptedSchedule.get()
            );
            assertFalse("and the fallback must not have run", interceptedFallbackRan.get());
        }
    }

    /**
     * The period of the arm that never gives up is an operator-safety property, not a tuning knob, so it does not follow
     * {@code snapshot.cleanup.retry_backoff}. At that setting's 100 ms minimum the configured ladder would put an unbounded
     * loop at 3.2 seconds for ever; this arm caps at 30 seconds, as the reconciliation retry does.
     * <p>
     * The delay is read off the schedule the helper asks for rather than timed, because timing a 30-second delay means waiting
     * for it.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheUncappedRetryPeriodDoesNotFollowTheConfiguredBackoff() {
        final SnapshotsService service = newInterceptedService();
        interceptedSettings.applySettings(
            Settings.builder().put(SnapshotsService.SNAPSHOT_CLEANUP_RETRY_BACKOFF_SETTING.getKey(), MIN_RETRY_BACKOFF).build()
        );

        service.retryOrFailOnClusterManagerFailOver(
            new FailedToCommitClusterStateException("test"),
            9,
            "test-source",
            () -> mock(ClusterStateUpdateTask.class),
            () -> interceptedFallbackRan.set(true),
            true
        );

        assertEquals(
            "the uncapped arm's period is its own cap, not the configured backoff's ladder",
            TimeValue.timeValueSeconds(30L),
            interceptedDelay.get()
        );
        assertFalse("the arm that never gives up must not run the fallback", interceptedFallbackRan.get());
    }

    /** A retry armed before a failover this node handled is not submitted after it, even though the node is cluster manager. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testARetryArmedBeforeAFailoverIsNotSubmittedAfterIt() throws Exception {
        final SnapshotsService service = newInterceptedService();
        givenUpDeleteRemoval(service).onFailure(DELETE_REMOVAL, new FailedToCommitClusterStateException("simulated publish failure"));
        assertNotNull("the removal of a delete this node gave up on keeps retrying its publication", interceptedSchedule.get());

        failOver(service);
        interceptedSchedule.get().run();

        verify(
            interceptedClusterService,
            never().description("a retry armed before a failover this node handled must not be submitted after it")
        ).submitStateUpdateTask(eq(DELETE_REMOVAL), any(ClusterStateUpdateTask.class));
    }

    /** With no failover in between, an armed retry is submitted once. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAnArmedRetryWithNoFailoverIsSubmittedOnce() throws Exception {
        final SnapshotsService service = newInterceptedService();
        givenUpDeleteRemoval(service).onFailure(DELETE_REMOVAL, new FailedToCommitClusterStateException("simulated publish failure"));

        interceptedSchedule.get().run();

        verify(interceptedClusterService, times(1).description("an armed retry with no failover is submitted once")).submitStateUpdateTask(
            eq(DELETE_REMOVAL),
            any(ClusterStateUpdateTask.class)
        );
    }

    /** A retry armed before a failover this node handled is not submitted after it, bounded or not. */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTheBoundedArmIsNotSubmittedAfterAFailover() throws Exception {
        final SnapshotsService service = newInterceptedService();
        service.retryOrFailOnClusterManagerFailOver(
            new FailedToCommitClusterStateException("test"),
            0,
            "bounded-source",
            () -> mock(ClusterStateUpdateTask.class),
            () -> interceptedFallbackRan.set(true),
            false
        );
        failOver(service);
        interceptedSchedule.get().run();

        verify(
            interceptedClusterService,
            never().description("a bounded retry armed before a failover this node handled must not be submitted after it")
        ).submitStateUpdateTask(eq("bounded-source"), any(ClusterStateUpdateTask.class));
    }

    private static final String DELETE_REMOVAL = "remove snapshot deletion metadata";

    /** The removal of a failed delete this node recorded as given up on, at its last bounded attempt, with no debt owed. */
    private static ClusterStateUpdateTask givenUpDeleteRemoval(SnapshotsService service) throws Exception {
        final SnapshotDeletionsInProgress.Entry delete = deletionOf("repo");
        abandonedDeletes(service).add(delete.uuid());
        return service.createRemoveSnapshotDeletionTask(
            DELETE_REMOVAL,
            SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getDefault(Settings.EMPTY),
            delete,
            new RepositoryException("repo", "the delete timed out"),
            RepositoryData.EMPTY,
            null,
            true
        );
    }

    /** Has this node fail its snapshot operations over, as any update that finds it no longer cluster manager does. */
    private static void failOver(SnapshotsService service) {
        service.createRemoveFailedSnapshotTask(
            "remove snapshot metadata",
            0,
            new Snapshot("repo", new SnapshotId("other", UUIDs.randomBase64UUID())),
            new RepositoryException("repo", "failed"),
            null,
            null
        ).onFailure("remove snapshot metadata", new NotClusterManagerException("no longer cluster manager"));
    }

    @SuppressForbidden(reason = "the service's abandoned-delete record has no test seam")
    @SuppressWarnings("unchecked")
    private static Set<String> abandonedDeletes(SnapshotsService service) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("abandonedDeletes");
        field.setAccessible(true);
        return (Set<String>) field.get(service);
    }

    /**
     * The removal of a failed snapshot keeps retrying its publication past the bounded attempts while a reconciliation debt parks
     * creates, so that its give-up does not fail them.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testAFailedSnapshotRemovalKeepsRetryingWhileAReconciliationIsOwed() throws Exception {
        final SnapshotsService service = newInterceptedService();
        reconciliationOwed(service).add("repo-a");
        final AtomicReference<Object> parked = new AtomicReference<>();
        listenForSnapshot(service, new Snapshot("repo-a", new SnapshotId("parked", UUIDs.randomBase64UUID())), parked);
        final ClusterStateUpdateTask task = service.createRemoveFailedSnapshotTask(
            "remove snapshot metadata",
            SnapshotsService.SNAPSHOT_CLEANUP_RETRIES_SETTING.getDefault(Settings.EMPTY),
            new Snapshot("repo-b", new SnapshotId("failed", UUIDs.randomBase64UUID())),
            new RepositoryException("repo-b", "the snapshot failed"),
            null,
            null
        );

        task.onFailure("remove snapshot metadata", new FailedToCommitClusterStateException("simulated publish failure"));

        assertNotNull("a failed-snapshot removal must keep retrying while a reconciliation is owed", interceptedSchedule.get());
        assertNull("and the parked create's caller is not answered", parked.get());
    }

    @SuppressForbidden(reason = "the service's snapshot completion listeners have no test seam")
    @SuppressWarnings("unchecked")
    private static void listenForSnapshot(SnapshotsService service, Snapshot snapshot, AtomicReference<Object> outcome) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("snapshotCompletionListeners");
        field.setAccessible(true);
        final Map<Snapshot, List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>> listeners = (Map<
            Snapshot,
            List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>>) field.get(service);
        listeners.computeIfAbsent(snapshot, ignored -> new ArrayList<>()).add(ActionListener.wrap(outcome::set, outcome::set));
    }

    /** A started deletion of one snapshot of the named repository. Only the repository name matters to these tests. */
    private static SnapshotDeletionsInProgress.Entry deletionOf(String repository) {
        return new SnapshotDeletionsInProgress.Entry(
            List.of(new SnapshotId("snap-1", UUIDs.randomBase64UUID())),
            repository,
            0L,
            1L,
            SnapshotDeletionsInProgress.State.STARTED
        );
    }

    /**
     * A repository's record of being owed an identity rebind. Written directly because the only production entry point that
     * records one also does repository work, and the node-wide reader under test is private.
     */
    @SuppressForbidden(reason = "the service's reconciliation debt has no test seam")
    @SuppressWarnings("unchecked")
    private static Set<String> reconciliationOwed(SnapshotsService service) throws Exception {
        final Field field = SnapshotsService.class.getDeclaredField("reconciliationOwed");
        field.setAccessible(true);
        return (Set<String>) field.get(service);
    }

    private ClusterSettings interceptedSettings;
    private ClusterService interceptedClusterService;
    private final AtomicReference<Runnable> interceptedSchedule = new AtomicReference<>();
    private final AtomicReference<TimeValue> interceptedDelay = new AtomicReference<>();
    private final AtomicBoolean interceptedFallbackRan = new AtomicBoolean();

    /**
     * A second service whose generic-pool schedules are captured rather than run, so that "a retry was armed" is an assertion
     * on a reference instead of a wait. The suite's own service is handed its pool at construction and cannot be intercepted
     * afterwards.
     */
    private SnapshotsService newInterceptedService() {
        final ThreadPool interceptedPool = spy(threadPool);
        doAnswer(invocation -> {
            if (ThreadPool.Names.GENERIC.equals(invocation.getArgument(2))) {
                interceptedDelay.set(invocation.getArgument(1));
                interceptedSchedule.set(invocation.getArgument(0));
                return null;
            }
            return invocation.callRealMethod();
        }).when(interceptedPool).schedule(any(), any(), anyString());

        interceptedSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ClusterService ownClusterService = mock(ClusterService.class);
        interceptedClusterService = ownClusterService;
        when(ownClusterService.getClusterSettings()).thenReturn(interceptedSettings);
        final TransportService ownTransport = mock(TransportService.class);
        when(ownTransport.getThreadPool()).thenReturn(interceptedPool);
        return new SnapshotsService(
            Settings.builder().put("node.name", "test").putList("node.roles", "cluster_manager", "data").build(),
            ownClusterService,
            mock(org.opensearch.cluster.metadata.IndexNameExpressionResolver.class),
            mock(org.opensearch.repositories.RepositoriesService.class),
            ownTransport,
            mock(org.opensearch.action.support.ActionFilters.class),
            null,
            new org.opensearch.indices.RemoteStoreSettings(Settings.EMPTY, interceptedSettings),
            null
        );
    }
}
