/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the per-repository I/O time budget: {@link SnapshotsService#withIoTimeout} and the settings
 * plumbing behind {@link SnapshotsService#repositoryIoTimeout()}.
 */
public class SnapshotRepositoryIoTimeoutTests extends OpenSearchTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueMillis(10);
    private static final String DESCRIPTION = "get repository data for [test-repo]";

    private DeterministicTaskQueue taskQueue;
    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        taskQueue = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    /**
     * Catches a helper built on the {@code listenerName} overload of
     * {@link org.opensearch.action.support.ListenerTimeouts#wrapWithTimeout}, which hardcodes its own expiry body
     * and exposes no hook: the caller's consumer would never run and the delegate would see a library-generic
     * exception instead of the site-specific one. Also catches a hook parameter that is wired but never invoked, and,
     * through a listener completed before its budget, a helper that runs the hook eagerly or unconditionally.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTimeoutRunsCallersHook() {
        final AtomicReference<Exception> delegateFailure = new AtomicReference<>();
        final AtomicInteger hookRuns = new AtomicInteger();
        final ActionListener<Void> delegate = ActionListener.wrap(v -> fail("delegate must not succeed"), delegateFailure::set);
        final OpenSearchTimeoutException callersException = new OpenSearchTimeoutException("[" + DESCRIPTION + "] timed out");
        final AtomicInteger healthyResponses = new AtomicInteger();
        SnapshotsService.withIoTimeout(
            taskQueue.getThreadPool(),
            TIMEOUT,
            DESCRIPTION,
            ActionListener.<Void>wrap(v -> healthyResponses.incrementAndGet(), e -> fail("healthy delegate must not fail: " + e)),
            ignored -> fail("hook must not run after a healthy completion")
        ).onResponse(null);

        final ActionListener<Void> wrapped = SnapshotsService.withIoTimeout(
            taskQueue.getThreadPool(),
            TIMEOUT,
            DESCRIPTION,
            delegate,
            ignored -> {
                hookRuns.incrementAndGet();
                delegate.onFailure(callersException);
            }
        );

        assertNotSame("flag on must interpose a wrapper", delegate, wrapped);
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertEquals("healthy completion must reach its delegate exactly once", 1, healthyResponses.get());
        assertEquals("caller's hook must run exactly once", 1, hookRuns.get());
        assertSame("delegate must see the caller's own exception instance", callersException, delegateFailure.get());
    }

    /**
     * The flag-off seam. The helper returns the caller's own listener and arms no timer, so a cluster on default settings
     * gets no new behaviour and no extra scheduled task per finalization. The constructor neither seeds nor registers the
     * setting: Setting#get validates the resolved default and the validator rejects every value while the flag is off, so a
     * seed outside the flag guard would stop every flag-off cluster-manager-eligible node from starting, and with no
     * consumer registered neither validating nor applying settings that carry the key runs its validator on this node.
     */
    public void testFlagOffWrapsNothingAndRunsNoValidator() {
        assertFalse(FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING));
        final ActionListener<Void> listener = ActionListener.wrap(() -> {});
        final ActionListener<Void> returned = SnapshotsService.withIoTimeout(
            taskQueue.getThreadPool(),
            TIMEOUT,
            DESCRIPTION,
            listener,
            ignored -> fail("nothing may be scheduled with the flag off")
        );
        assertSame("flag off must return the caller's own listener", listener, returned);
        assertFalse("flag off must arm no timer", taskQueue.hasDeferredTasks());

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final SnapshotsService service = newClusterManagerSnapshotsService(clusterSettings, Settings.EMPTY);
        assertEquals(TimeValue.timeValueMinutes(30), service.repositoryIoTimeout());
        final Settings update = Settings.builder().put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), "10m").build();
        clusterSettings.validateUpdate(update);
        clusterSettings.applySettings(update);
    }

    /**
     * Catches the token leak on a rejected schedule. {@code ThreadPool.schedule} propagates the rejection and
     * {@code ListenerTimeouts} does not catch it, so without the helper's own catch it escapes {@code endSnapshot}
     * after the per-repository token was already taken -- {@code leaveRepoLoop} never runs and the repository is
     * wedged for the life of the node, during a shutdown when nobody is watching. A real terminated pool is used
     * rather than a stub so the actual throw site runs.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRejectedScheduleReturnsUnwrappedListener() throws Exception {
        final TestThreadPool terminatedPool = new TestThreadPool(getTestName() + "-terminated");
        ThreadPool.terminate(terminatedPool, 0, TimeUnit.MILLISECONDS);
        final ActionListener<Void> listener = ActionListener.wrap(() -> {});

        final ActionListener<Void> returned = SnapshotsService.withIoTimeout(
            terminatedPool,
            TIMEOUT,
            DESCRIPTION,
            listener,
            ignored -> fail("a rejected schedule can never fire")
        );

        assertSame("a rejected schedule must degrade to the unbudgeted listener, not throw", listener, returned);
    }

    /**
     * Covers both halves of the seed-then-register pair, which must ship together. Seed missing: a node-level
     * value in opensearch.yml is silently ignored until the first dynamic update. Register missing, registered outside
     * the cluster-manager guard, or assigning to the wrong field: the budget is frozen for the life of the node and an
     * operator cannot shorten it mid-incident, which is the one moment the setting exists for.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testUpdateConsumerAppliesNewTimeout() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final Settings nodeSettings = Settings.builder()
            .put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), "15m")
            .build();

        final SnapshotsService service = newClusterManagerSnapshotsService(clusterSettings, nodeSettings);
        assertEquals("a node-level value must be seeded, not ignored", TimeValue.timeValueMinutes(15), service.repositoryIoTimeout());

        clusterSettings.applySettings(
            Settings.builder().put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), "1s").build()
        );

        assertEquals("a dynamic update must reach the field", TimeValue.timeValueSeconds(1), service.repositoryIoTimeout());
    }

    /**
     * Catches a timer scheduled on the snapshot pool. The reasons are width and what each pool already carries:
     * SNAPSHOT is boundedBy((vCPU + 1) / 2, 1, 5) threads and is where finalization's 2 + indices.size() fan-out and
     * its serial tail run, so a timer queued there waits behind work with no ceiling; GENERIC is
     * 4..boundedBy(4 * processors, 128, 512) and carries none of it. Force-queueing does not distinguish them --
     * OpenSearchExecutors#newScaling installs ForceQueuePolicy for every scaling pool. Both pool names are plain
     * strings, so nothing in the type system catches the substitution.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTimerIsScheduledOnGenericPoolNotSnapshotPool() {
        final ThreadPool mockThreadPool = mock(ThreadPool.class);

        SnapshotsService.withIoTimeout(mockThreadPool, TIMEOUT, DESCRIPTION, ActionListener.<Void>wrap(() -> {}), ignored -> {});

        verify(mockThreadPool).schedule(any(Runnable.class), any(TimeValue.class), eq(ThreadPool.Names.GENERIC));
    }

    /**
     * Builds a live cluster-manager-role {@link SnapshotsService} over the given cluster settings, following
     * {@code RetryOrFailOnClusterManagerFailOverTests#setUp}. Only the settings tests need an instance; the
     * {@code withIoTimeout} tests need none.
     */
    private SnapshotsService newClusterManagerSnapshotsService(ClusterSettings clusterSettings, Settings nodeSettings) {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        final Settings settings = Settings.builder()
            .put(nodeSettings)
            .put("node.name", "test")
            .putList("node.roles", "cluster_manager", "data")
            .build();

        return new SnapshotsService(
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
}
