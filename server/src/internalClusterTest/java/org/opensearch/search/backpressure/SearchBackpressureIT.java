/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.backpressure.settings.NodeDuressSettings;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.settings.SearchTaskSettings;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancelledException;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class SearchBackpressureIT extends OpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(10, TimeUnit.SECONDS);
    private static final int MOVING_AVERAGE_WINDOW_SIZE = 10;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    @Before
    public final void setupNodeSettings() {
        Settings request = Settings.builder()
            .put(NodeDuressSettings.SETTING_CPU_THRESHOLD.getKey(), 0.0)
            .put(NodeDuressSettings.SETTING_HEAP_THRESHOLD.getKey(), 0.0)
            .put(NodeDuressSettings.SETTING_NUM_SUCCESSIVE_BREACHES.getKey(), 1)
            .put(SearchTaskSettings.SETTING_TOTAL_HEAP_PERCENT_THRESHOLD.getKey(), 0.0)
            .put(SearchShardTaskSettings.SETTING_TOTAL_HEAP_PERCENT_THRESHOLD.getKey(), 0.0)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());
    }

    @After
    public final void cleanupNodeSettings() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testCancellationSettingsChanged() {
        Settings request = Settings.builder().put(SearchTaskSettings.SETTING_CANCELLATION_RATE.getKey(), "0.05").build();
        ClusterUpdateSettingsResponse response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get();
        assertEquals(response.getPersistentSettings().get(SearchTaskSettings.SETTING_CANCELLATION_RATE.getKey()), "0.05");

        request = Settings.builder().put(SearchShardTaskSettings.SETTING_CANCELLATION_RATIO.getKey(), "0.7").build();
        response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get();
        assertEquals(response.getPersistentSettings().get(SearchShardTaskSettings.SETTING_CANCELLATION_RATIO.getKey()), "0.7");
    }

    public void testSearchTaskCancellationWithHighElapsedTime() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(SearchTaskSettings.SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.getKey(), 1000)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(
            TestTransportAction.ACTION,
            new TestRequest<>(
                RequestType.HIGH_ELAPSED_TIME,
                (TaskFactory<Task>) (id, type, action, description, parentTaskId, headers) -> new SearchTask(
                    id,
                    type,
                    action,
                    descriptionSupplier(description),
                    parentTaskId,
                    headers
                )
            ),
            listener
        );
        assertTrue(listener.latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));

        Exception caughtException = listener.getException();
        assertNotNull("SearchTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        MatcherAssert.assertThat(caughtException.getMessage(), containsString("elapsed time exceeded"));
    }

    public void testSearchShardTaskCancellationWithHighElapsedTime() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(SearchShardTaskSettings.SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.getKey(), 1000)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestTransportAction.ACTION, new TestRequest<>(RequestType.HIGH_ELAPSED_TIME, SearchShardTask::new), listener);
        assertTrue(listener.latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));

        Exception caughtException = listener.getException();
        assertNotNull("SearchShardTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        MatcherAssert.assertThat(caughtException.getMessage(), containsString("elapsed time exceeded"));
    }

    public void testSearchTaskCancellationWithHighCpu() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(SearchTaskSettings.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 1000)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(
            TestTransportAction.ACTION,
            new TestRequest<>(
                RequestType.HIGH_CPU,
                (TaskFactory<Task>) (id, type, action, description, parentTaskId, headers) -> new SearchTask(
                    id,
                    type,
                    action,
                    descriptionSupplier(description),
                    parentTaskId,
                    headers
                )
            ),
            listener
        );
        assertTrue(listener.latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));

        Exception caughtException = listener.getException();
        assertNotNull("SearchTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        MatcherAssert.assertThat(caughtException.getMessage(), containsString("cpu usage exceeded"));
    }

    public void testSearchShardTaskCancellationWithHighCpu() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(SearchShardTaskSettings.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 1000)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestTransportAction.ACTION, new TestRequest<>(RequestType.HIGH_CPU, SearchShardTask::new), listener);
        assertTrue(listener.latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));

        Exception caughtException = listener.getException();
        assertNotNull("SearchShardTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        MatcherAssert.assertThat(caughtException.getMessage(), containsString("cpu usage exceeded"));
    }

    public void testSearchTaskCancellationWithHighHeapUsage() throws InterruptedException {
        // Before SearchBackpressureService cancels a task based on its heap usage, we need to build up the heap moving average
        // To build up the heap moving average, we need to hit the same node with multiple requests and then hit the same node with a
        // request having higher heap usage
        String node = randomFrom(internalCluster().getNodeNames());
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(SearchTaskSettings.SETTING_HEAP_PERCENT_THRESHOLD.getKey(), 0.0)
            .put(SearchTaskSettings.SETTING_HEAP_VARIANCE_THRESHOLD.getKey(), 1.0)
            .put(SearchTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.getKey(), MOVING_AVERAGE_WINDOW_SIZE)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        for (int i = 0; i < MOVING_AVERAGE_WINDOW_SIZE; i++) {
            client(node).execute(
                TestTransportAction.ACTION,
                new TestRequest<>(
                    RequestType.HIGH_HEAP,
                    (TaskFactory<Task>) (id, type, action, description, parentTaskId, headers) -> new SearchTask(
                        id,
                        type,
                        action,
                        descriptionSupplier(description),
                        parentTaskId,
                        headers
                    )
                ),
                listener
            );
        }

        listener = new ExceptionCatchingListener();
        client(node).execute(
            TestTransportAction.ACTION,
            new TestRequest<>(
                RequestType.HIGHER_HEAP,
                (TaskFactory<Task>) (id, type, action, description, parentTaskId, headers) -> new SearchTask(
                    id,
                    type,
                    action,
                    descriptionSupplier(description),
                    parentTaskId,
                    headers
                )
            ),
            listener
        );
        assertTrue(listener.latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));

        Exception caughtException = listener.getException();
        assertNotNull("SearchTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        MatcherAssert.assertThat(caughtException.getMessage(), containsString("heap usage exceeded"));
    }

    public void testSearchShardTaskCancellationWithHighHeapUsage() throws InterruptedException {
        // Before SearchBackpressureService cancels a task based on its heap usage, we need to build up the heap moving average
        // To build up the heap moving average, we need to hit the same node with multiple requests and then hit the same node with a
        // request having higher heap usage
        String node = randomFrom(internalCluster().getNodeNames());
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(SearchShardTaskSettings.SETTING_HEAP_PERCENT_THRESHOLD.getKey(), 0.0)
            .put(SearchShardTaskSettings.SETTING_HEAP_VARIANCE_THRESHOLD.getKey(), 1.0)
            .put(SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.getKey(), MOVING_AVERAGE_WINDOW_SIZE)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        for (int i = 0; i < MOVING_AVERAGE_WINDOW_SIZE; i++) {
            client(node).execute(TestTransportAction.ACTION, new TestRequest<>(RequestType.HIGH_HEAP, SearchShardTask::new), listener);
        }

        listener = new ExceptionCatchingListener();
        client(node).execute(TestTransportAction.ACTION, new TestRequest<>(RequestType.HIGHER_HEAP, SearchShardTask::new), listener);
        assertTrue(listener.latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));

        Exception caughtException = listener.getException();
        assertNotNull("SearchShardTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        MatcherAssert.assertThat(caughtException.getMessage(), containsString("heap usage exceeded"));
    }

    public void testSearchCancellationWithBackpressureDisabled() throws InterruptedException {
        Settings request = Settings.builder().put(SearchBackpressureSettings.SETTING_MODE.getKey(), "monitor_only").build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestTransportAction.ACTION, new TestRequest<>(RequestType.HIGH_ELAPSED_TIME, SearchShardTask::new), listener);
        // waiting for the TIMEOUT * 3 time for the request to complete and the latch to countdown.
        assertTrue(
            "SearchShardTask should have been completed by now and countdown the latch",
            listener.latch.await(TIMEOUT.getSeconds() * 3, TimeUnit.SECONDS)
        );

        Exception caughtException = listener.getException();
        assertNull("SearchShardTask shouldn't have cancelled for monitor_only mode", caughtException);
    }

    private static class ExceptionCatchingListener implements ActionListener<TestResponse> {
        private final CountDownLatch latch;
        private Exception exception = null;

        public ExceptionCatchingListener() {
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onResponse(TestResponse r) {
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            this.exception = e;
            latch.countDown();
        }

        private Exception getException() {
            return exception;
        }
    }

    enum RequestType {
        HIGH_CPU,
        HIGH_HEAP,
        HIGHER_HEAP,
        HIGH_ELAPSED_TIME;
    }

    private Supplier<String> descriptionSupplier(String description) {
        return () -> description;
    }

    interface TaskFactory<T extends Task> {
        T createTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers);
    }

    public static class TestRequest<T extends Task> extends ActionRequest {
        private final RequestType type;
        private TaskFactory<T> taskFactory;

        public TestRequest(RequestType type, TaskFactory<T> taskFactory) {
            this.type = type;
            this.taskFactory = taskFactory;
        }

        public TestRequest(StreamInput in) throws IOException {
            super(in);
            this.type = in.readEnum(RequestType.class);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return taskFactory.createTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeEnum(type);
        }

        public RequestType getType() {
            return this.type;
        }
    }

    public static class TestResponse extends ActionResponse {
        public TestResponse() {}

        public TestResponse(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    public static class TestTransportAction extends HandledTransportAction<TestRequest, TestResponse> {
        public static final ActionType<TestResponse> ACTION = new ActionType<>("internal::test_action", TestResponse::new);
        private final ThreadPool threadPool;

        @Inject
        public TestTransportAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, TestRequest::new);
            this.threadPool = threadPool;
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                try {
                    CancellableTask cancellableTask = (CancellableTask) task;
                    long startTime = System.nanoTime();

                    // Doing a busy-wait until task cancellation or timeout.
                    // We are running HIGH_HEAP requests to build up heap moving average and not expect it to get cancelled.
                    do {
                        doWork(request);
                    } while (request.type != RequestType.HIGH_HEAP
                        && cancellableTask.isCancelled() == false
                        && (System.nanoTime() - startTime) < TIMEOUT.getNanos());

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

        private void doWork(TestRequest<Task> request) throws InterruptedException {
            switch (request.getType()) {
                case HIGH_CPU:
                    long i = 0, j = 1, k = 1, iterations = 1000;
                    do {
                        j += i;
                        k *= j;
                        i++;
                    } while (i < iterations);
                    break;
                case HIGH_HEAP:
                    Byte[] bytes = new Byte[100000];
                    int[] ints = new int[1000];
                    break;
                case HIGHER_HEAP:
                    Byte[] more_bytes = new Byte[1000000];
                    int[] more_ints = new int[10000];
                    break;
                case HIGH_ELAPSED_TIME:
                    Thread.sleep(100);
                    break;
            }
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Collections.singletonList(new ActionHandler<>(TestTransportAction.ACTION, TestTransportAction.class));
        }

        @Override
        public List<ActionType<? extends ActionResponse>> getClientActions() {
            return Collections.singletonList(TestTransportAction.ACTION);
        }
    }
}
