/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchShardTask;
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
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
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

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class SearchBackpressureIT extends OpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

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

    public void testSearchShardTaskCancellationWithHighElapsedTime() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(ElapsedTimeTracker.SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.getKey(), 5000)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        CountDownLatch latch = new CountDownLatch(1);

        client().execute(TestTransportAction.ACTION, new TestRequest(RequestType.HIGH_ELAPSED_TIME), new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                fail("SearchShardTask should have been cancelled");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(TaskCancelledException.class, e.getClass());
                assertTrue(e.getMessage().contains("elapsed time exceeded"));
                latch.countDown();
            }
        });

        latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }

    public void testSearchShardTaskCancellationWithHighCpu() throws InterruptedException {
        Settings request = Settings.builder()
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(CpuUsageTracker.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 15000)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        CountDownLatch latch = new CountDownLatch(1);

        client().execute(TestTransportAction.ACTION, new TestRequest(RequestType.HIGH_CPU), new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                fail("SearchShardTask should have been cancelled");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(TaskCancelledException.class, e.getClass());
                assertTrue(e.getMessage().contains("cpu usage exceeded"));
                latch.countDown();
            }
        });

        latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }

    public void testSearchCancellationWithBackpressureDisabled() throws InterruptedException {
        Settings request = Settings.builder().put(SearchBackpressureSettings.SETTING_MODE.getKey(), "monitor_only").build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());

        CountDownLatch latch = new CountDownLatch(1);

        client().execute(TestTransportAction.ACTION, new TestRequest(RequestType.HIGH_ELAPSED_TIME), new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("SearchShardTask shouldn't have cancelled for monitor_only mode");
                latch.countDown();
            }
        });

        latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }

    enum RequestType {
        HIGH_CPU,
        HIGH_ELAPSED_TIME;
    }

    public static class TestRequest extends ActionRequest {
        private final RequestType type;

        public TestRequest(RequestType type) {
            this.type = type;
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
            return new SearchShardTask(id, type, action, "", parentTaskId, headers);
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
                    SearchShardTask searchShardTask = (SearchShardTask) task;
                    long startTime = System.nanoTime();

                    // Doing a busy-wait until task cancellation or timeout.
                    do {
                        doWork(request);
                    } while (searchShardTask.isCancelled() == false && (System.nanoTime() - startTime) < TIMEOUT.getNanos());

                    if (searchShardTask.isCancelled()) {
                        throw new TaskCancelledException(searchShardTask.getReasonCancelled());
                    } else {
                        listener.onResponse(new TestResponse());
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private void doWork(TestRequest request) throws InterruptedException {
            switch (request.getType()) {
                case HIGH_CPU:
                    long i = 0, j = 1, k = 1, iterations = 1000;
                    do {
                        j += i;
                        k *= j;
                        i++;
                    } while (i < iterations);
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
