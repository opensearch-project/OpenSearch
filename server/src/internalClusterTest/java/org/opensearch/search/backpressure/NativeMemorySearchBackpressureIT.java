/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.apache.lucene.util.Constants;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.backpressure.settings.NodeDuressSettings;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.opensearch.wlm.WorkloadGroupTask;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for the native-memory path of {@link SearchBackpressureService}.
 *
 * <p>Native-memory tracking depends on three things being true at SBP-construction time:
 * <ol>
 *   <li>The host is Linux (so {@code OsProbe.getProcessNativeMemoryBytes()} is non-negative).</li>
 *   <li>A backend has installed a snapshot supplier on
 *       {@link org.opensearch.search.backpressure.NativeMemoryUsageService} before SBP is built.</li>
 *   <li>{@code OsProbe.getInstance().getTotalPhysicalMemorySize() &gt; 0}.</li>
 * </ol>
 *
 * <p>{@link NativeMemoryTestPlugin} satisfies the second condition by installing fake snapshot
 * and budget suppliers from {@code createComponents}, mirroring the production wiring path
 * {@code DataFusionPlugin} uses. The first and third are checked via {@code assumeTrue} so the
 * suite is skipped on macOS/Windows CI rather than failing.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class NativeMemorySearchBackpressureIT extends OpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(10, TimeUnit.SECONDS);
    private static final long DEFAULT_BUDGET_BYTES = 1024L * 1024L * 1024L; // 1 GiB

    /**
     * Per-task-id native bytes published into {@link NativeMemoryUsageTracker}'s snapshot
     * supplier. Tests mutate this map between requests; the supplier installed by
     * {@link NativeMemoryTestPlugin} reads the live reference on every refresh.
     */
    private static final Map<Long, Long> SNAPSHOT = new ConcurrentHashMap<>();
    private static final AtomicLong BUDGET_BYTES = new AtomicLong(DEFAULT_BUDGET_BYTES);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(NativeMemoryTestPlugin.class);
        return plugins;
    }

    @Before
    public final void setupNodeSettings() {
        // Native-memory duress + tracking only engages on Linux; skip otherwise.
        assumeTrue("native-memory backpressure path is Linux-only (OsProbe.getProcessNativeMemoryBytes)", Constants.LINUX);

        // Force native-memory duress to trip after a single observation by setting the threshold
        // to 0.0 — any usedFraction above zero counts as a breach. Combined with a tiny
        // numSuccessiveBreaches the duress flips on the next tick deterministically.
        Settings request = Settings.builder()
            .put(NodeDuressSettings.SETTING_CPU_THRESHOLD.getKey(), 1.0) // disable CPU duress
            .put(NodeDuressSettings.SETTING_HEAP_THRESHOLD.getKey(), 1.0) // disable heap duress
            .put(NodeDuressSettings.SETTING_NATIVE_MEMORY_THRESHOLD.getKey(), 0.0)
            .put(NodeDuressSettings.SETTING_NUM_SUCCESSIVE_BREACHES.getKey(), 1)
            // node.native_memory.limit must be > 0 or the duress lambda short-circuits
            // ("totalNative is zero"). 1 byte is enough — the probe reports a non-zero RssAnon.
            .put(NodeDuressSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.getKey(), "1b")
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        SNAPSHOT.clear();
        BUDGET_BYTES.set(DEFAULT_BUDGET_BYTES);
    }

    @After
    public final void cleanupNodeSettings() {
        SNAPSHOT.clear();
        BUDGET_BYTES.set(DEFAULT_BUDGET_BYTES);
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    /**
     * The shard task's reported native bytes exceed {@code budget * fraction}; SBP must cancel
     * with a "native memory usage exceeded" reason when running in {@code enforced} mode.
     */
    public void testSearchShardTaskCancellationWithHighNativeMemory() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            // Threshold = 0.5 of 1 GiB = 512 MiB. Snapshot publishes 768 MiB for the task —
            // strictly above, so the evaluator must produce a cancellation reason.
            .put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 0.5)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestTransportAction.ACTION, new TestRequest(taskId -> SNAPSHOT.put(taskId, 768L * 1024L * 1024L)), listener);
        assertTrue("task should have been cancelled within " + TIMEOUT, listener.latch.await(TIMEOUT.getSeconds() * 2, TimeUnit.SECONDS));

        Exception caughtException = listener.getException();
        assertNotNull("SearchShardTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        MatcherAssert.assertThat(caughtException.getMessage(), containsString("native memory usage exceeded"));
    }

    /**
     * Per-task threshold is set, but the published bytes are below the evaluator's cutoff. SBP
     * must NOT cancel even though native-memory duress is active. This pins the contract that
     * duress alone doesn't kill tasks — the evaluator decides per task.
     */
    public void testSearchShardTaskNotCancelledBelowThreshold() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            // Threshold = 0.9 of 1 GiB ≈ 922 MiB. Snapshot publishes 100 MiB — well under.
            .put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 0.9)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestTransportAction.ACTION, new TestRequest(taskId -> SNAPSHOT.put(taskId, 100L * 1024L * 1024L)), listener);
        // Task self-completes after TIMEOUT; allow extra room.
        assertTrue(
            "task should have completed within " + (TIMEOUT.getSeconds() * 3) + "s",
            listener.latch.await(TIMEOUT.getSeconds() * 3, TimeUnit.SECONDS)
        );
        assertNull("SearchShardTask under threshold must not be cancelled by the native-memory tracker", listener.getException());
    }

    /**
     * Native-memory threshold setting explicitly set to {@code 0.0} (the rollout-gate inert state).
     * Even with heavy reported bytes the tracker must stay silent, because {@code fraction == 0}
     * disables evaluation. Pins the spec's "operator can opt out" contract.
     */
    public void testTrackerInertWhenPerTaskFractionIsZero() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            // Explicitly set to 0.0 to override the production default and force the inert path.
            .put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 0.0)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(
            TestTransportAction.ACTION,
            new TestRequest(taskId -> SNAPSHOT.put(taskId, 4L * 1024L * 1024L * 1024L)), // 4 GiB
            listener
        );
        assertTrue("task should have completed without cancellation", listener.latch.await(TIMEOUT.getSeconds() * 3, TimeUnit.SECONDS));
        assertNull("Per-task fraction explicitly set to 0 — the tracker must not cancel", listener.getException());
    }

    /**
     * In {@code monitor_only} mode, SBP must log breaches but never invoke
     * {@code TaskManager.cancelTaskAndDescendants}. The task therefore self-completes.
     */
    public void testMonitorOnlyModeDoesNotCancel() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "monitor_only")
            .put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 0.5)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestTransportAction.ACTION, new TestRequest(taskId -> SNAPSHOT.put(taskId, 768L * 1024L * 1024L)), listener);
        assertTrue(
            "task should have completed without cancellation in monitor_only",
            listener.latch.await(TIMEOUT.getSeconds() * 3, TimeUnit.SECONDS)
        );
        assertNull("monitor_only must not cancel", listener.getException());
    }

    /**
     * {@code search_backpressure.mode = disabled} short-circuits the entire {@code doRun} loop
     * before any tracker is consulted. The task must complete normally even with a heavy
     * snapshot reading.
     */
    public void testDisabledModeShortCircuits() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "disabled")
            .put(SearchShardTaskSettings.SETTING_NATIVE_MEMORY_PERCENT_THRESHOLD.getKey(), 0.5)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestTransportAction.ACTION, new TestRequest(taskId -> SNAPSHOT.put(taskId, 999L * 1024L * 1024L)), listener);
        assertTrue(
            "task should have completed without cancellation in disabled mode",
            listener.latch.await(TIMEOUT.getSeconds() * 3, TimeUnit.SECONDS)
        );
        assertNull("disabled mode must not cancel", listener.getException());
    }

    // -----------------------------------------------------------------------
    // Test plugin + transport action
    // -----------------------------------------------------------------------

    /**
     * Installs the snapshot + budget suppliers on {@link NativeMemoryUsageTracker} during
     * {@code createComponents} — same boot-time hook the production DataFusion plugin uses.
     * SBP is constructed in {@code Node} <em>after</em> {@code createComponents} runs, so the
     * suppliers are visible when {@code isNativeTrackingSupported()} is evaluated.
     */
    public static class NativeMemoryTestPlugin extends Plugin implements ActionPlugin {
        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            org.opensearch.core.common.io.stream.NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            // Live-reading the static maps so test methods can mutate them between requests.
            NativeMemoryUsageTracker.setSnapshotSupplier(() -> new HashMap<>(SNAPSHOT));
            NativeMemoryUsageTracker.setNativeMemoryBudgetSupplier(BUDGET_BYTES::get);
            return Collections.emptyList();
        }

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Collections.singletonList(new ActionHandler<>(TestTransportAction.ACTION, TestTransportAction.class));
        }

        @Override
        public List<ActionType<? extends ActionResponse>> getClientActions() {
            return Collections.singletonList(TestTransportAction.ACTION);
        }
    }

    /**
     * A request whose handler-side hook lets each test publish a per-task snapshot entry
     * before busy-waiting for cancellation. The hook receives the task id assigned by
     * {@code TaskManager.register}, which is the same id SBP looks up in the snapshot.
     */
    public static class TestRequest extends ActionRequest {
        private final transient SnapshotPublisher publisher;

        public TestRequest(SnapshotPublisher publisher) {
            this.publisher = publisher;
        }

        public TestRequest(StreamInput in) throws IOException {
            super(in);
            this.publisher = id -> {};
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            // Use SearchShardTask so SBP picks it up via getTaskByType(SearchShardTask.class).
            SearchShardTask task = new SearchShardTask(id, type, action, "native-memory-it", parentTaskId, headers);
            publisher.publish(id);
            return task;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    /** Side-channel for tests to inject the per-task snapshot reading at task-creation time. */
    @FunctionalInterface
    public interface SnapshotPublisher {
        void publish(long taskId);
    }

    public static class TestResponse extends ActionResponse {
        public TestResponse() {}

        public TestResponse(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    public static class TestTransportAction extends HandledTransportAction<TestRequest, TestResponse> {
        public static final ActionType<TestResponse> ACTION = new ActionType<>("internal::native_memory_bp_test_action", TestResponse::new);
        private final ThreadPool threadPool;

        @Inject
        public TestTransportAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, TestRequest::new);
            this.threadPool = threadPool;
        }

        @Override
        @SuppressForbidden(reason = "Simulating a busy task by sleeping")
        protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                try {
                    CancellableTask cancellableTask = (CancellableTask) task;
                    ((WorkloadGroupTask) task).setWorkloadGroupId(threadPool.getThreadContext());
                    long startTime = System.nanoTime();
                    // Busy-wait for cancellation or timeout. SBP runs every interval_millis
                    // (default 1s); the loop must outlive at least two ticks for the duress
                    // streak to trip and the cancel decision to fire.
                    do {
                        Thread.sleep(50);
                    } while (cancellableTask.isCancelled() == false && (System.nanoTime() - startTime) < TIMEOUT.getNanos());

                    if (cancellableTask.isCancelled()) {
                        throw new TaskCancelledException(cancellableTask.getReasonCancelled());
                    } else {
                        listener.onResponse(new TestResponse());
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    /** Reused in-suite instead of importing from {@link SearchBackpressureIT}. */
    public static class ExceptionCatchingListener implements ActionListener<TestResponse> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile Exception exception = null;

        @Override
        public void onResponse(TestResponse r) {
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            this.exception = e;
            latch.countDown();
        }

        public Exception getException() {
            return exception;
        }
    }
}
