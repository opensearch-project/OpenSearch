/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
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
import org.opensearch.tasks.ResourceUsageMetric;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancelledException;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.ResourceStats;
import org.opensearch.tasks.ResourceStatsType;
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
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(SearchBackpressureSettings.SETTING_CANCELLATION_RATIO.getKey(), 1)
            .put(SearchBackpressureSettings.SETTING_CANCELLATION_RATE.getKey(), 10)
            .put(SearchBackpressureSettings.SETTING_CANCELLATION_BURST.getKey(), 10)
            .put(NodeDuressSettings.SETTING_CPU_THRESHOLD.getKey(), 0.3)
            .put(NodeDuressSettings.SETTING_HEAP_THRESHOLD.getKey(), 0.0)
            .put(NodeDuressSettings.SETTING_NUM_SUCCESSIVE_BREACHES.getKey(), 1)
            .put(SearchShardTaskSettings.SETTING_TOTAL_HEAP_PERCENT_THRESHOLD.getKey(), 0.0)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    public void testSearchShardTaskCancellationWithHighElapsedTime() throws InterruptedException {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(ElapsedTimeTracker.SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.getKey(), 5000)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        CountDownLatch latch = new CountDownLatch(1);

        client().execute(TestTransportAction.ACTION, generateRequestWithHighElapsedTime(8000), new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                fail("SearchShardTask should have been cancelled");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.info("testSearchShardTaskCancellationWithHighElapsedTime onFailure e = " + e.getMessage());
                assertEquals(TaskCancelledException.class, e.getClass());
                assertTrue(e.getMessage().contains("elapsed time exceeded"));
                latch.countDown();
            }
        });

        latch.await();
        resetSettingValue(ElapsedTimeTracker.SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.getKey());
    }

    public void testSearchShardTaskCancellationWithHighCpu() throws InterruptedException {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(CpuUsageTracker.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey(), 1000));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        CountDownLatch latch = new CountDownLatch(1);

        client().execute(TestTransportAction.ACTION, generateRequestWithHigCpuUsage(0.8, 10000), new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                fail("SearchShardTask should have been cancelled");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.info("testSearchShardTaskCancellationWithHighCpu onFailure e = " + e.getMessage());
                assertEquals(TaskCancelledException.class, e.getClass());
                assertTrue(e.getMessage().contains("cpu usage exceeded"));
                latch.countDown();
            }
        });

        latch.await();
        resetSettingValue(CpuUsageTracker.SETTING_CPU_TIME_MILLIS_THRESHOLD.getKey());
    }

    public void testSearchCancellationWithBackpressureDisabled() throws InterruptedException {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(SearchBackpressureSettings.SETTING_MODE.getKey(), "monitor_only"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        CountDownLatch latch = new CountDownLatch(1);

        client().execute(TestTransportAction.ACTION, generateRequestWithHigCpuUsage(0.8, 10000), new ActionListener<>() {
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

        latch.await();
        resetSettingValue(SearchBackpressureSettings.SETTING_MODE.getKey());
    }

    /**
     * Returns a {@link TestRequest} which will continue running for given milliseconds
     * @param duration time in milliseconds
     */
    private TestRequest generateRequestWithHighElapsedTime(long duration) {
        return new TestRequest(() -> {
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Returns a {@link TestRequest} which is CPU heavy
     * @param load percentage load to the CPU
     * @param duration time in milliseconds
     */
    private TestRequest generateRequestWithHigCpuUsage(double load, long duration) {
        return new TestRequest(() -> {
            long startTime = System.currentTimeMillis();
            try {
                while (System.currentTimeMillis() - startTime < duration) {
                    // sleeping for the percentage of unladen time every 100ms
                    if (System.currentTimeMillis() % 100 == 0) {
                        Thread.sleep((long) Math.floor((1 - load) * 100));
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void resetSettingValue(String key) {
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull(key)).get();
    }

    public static class TestRequest extends ActionRequest {
        Runnable runnable;

        public TestRequest(StreamInput in) {}

        public TestRequest(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new SearchShardTask(id, type, action, "", parentTaskId, headers);
        }

        public void execute() {
            runnable.run();
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

        @Inject
        public TestTransportAction(TransportService transportService, NodeClient client, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, TestRequest::new, ThreadPool.Names.GENERIC);
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            try {
                SearchShardTask searchShardTask = (SearchShardTask) task;
                // starting resource tracking for the current thread
                ResourceUsageMetric[] initialTaskResourceMetrics = new ResourceUsageMetric[] {
                    new ResourceUsageMetric(ResourceStats.MEMORY, 0),
                    new ResourceUsageMetric(ResourceStats.CPU, 0) };
                task.startThreadResourceTracking(
                    Thread.currentThread().getId(),
                    ResourceStatsType.WORKER_STATS,
                    initialTaskResourceMetrics
                );
                long startTime = System.nanoTime();

                // Doing a busy-wait until task cancellation or timeout.
                do {
                    request.execute();
                    Thread.sleep(500);
                } while (searchShardTask.isCancelled() == false && (System.nanoTime() - startTime) < TIMEOUT.getNanos());

                if (searchShardTask.isCancelled()) {
                    throw new TaskCancelledException(searchShardTask.getReasonCancelled());
                } else {
                    listener.onResponse(new TestResponse());
                }
            } catch (Exception e) {
                listener.onFailure(e);
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
