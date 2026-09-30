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
        assertSame(
            "flag off, the instance seam must return the caller's own listener",
            listener,
            service.withRepositoryIoTimeout(DESCRIPTION, listener)
        );
        final Settings update = Settings.builder().put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), "10m").build();
        clusterSettings.validateUpdate(update);
        clusterSettings.applySettings(update);
    }

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

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testTimerIsScheduledOnGenericPoolNotSnapshotPool() {
        final ThreadPool mockThreadPool = mock(ThreadPool.class);

        SnapshotsService.withIoTimeout(mockThreadPool, TIMEOUT, DESCRIPTION, ActionListener.<Void>wrap(() -> {}), ignored -> {});

        verify(mockThreadPool).schedule(any(Runnable.class), any(TimeValue.class), eq(ThreadPool.Names.GENERIC));
    }

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
