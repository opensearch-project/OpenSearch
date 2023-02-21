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
import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.backpressure.settings.SearchBackpressureMode;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.settings.SearchTaskSettings;
import org.opensearch.search.backpressure.stats.SearchShardTaskStats;
import org.opensearch.search.backpressure.stats.SearchTaskStats;
import org.opensearch.search.backpressure.trackers.NodeDuressTracker;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.TaskCancellationService;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class SearchBackpressureServiceTests extends OpenSearchTestCase {
    MockTransportService transportService;
    TaskManager taskManager;
    ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        taskManager = transportService.getTaskManager();
        taskManager.setTaskCancellationService(new TaskCancellationService(transportService));
    }

    @After
    public void cleanup() {
        transportService.close();
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testIsNodeInDuress() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);

        AtomicReference<Double> cpuUsage = new AtomicReference<>();
        AtomicReference<Double> heapUsage = new AtomicReference<>();
        NodeDuressTracker cpuUsageTracker = new NodeDuressTracker(() -> cpuUsage.get() >= 0.5);
        NodeDuressTracker heapUsageTracker = new NodeDuressTracker(() -> heapUsage.get() >= 0.5);

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureService service = new SearchBackpressureService(
            settings,
            mockTaskResourceTrackingService,
            threadPool,
            System::nanoTime,
            List.of(cpuUsageTracker, heapUsageTracker),
            Collections.emptyList(),
            Collections.emptyList(),
            taskManager
        );

        // Node not in duress.
        cpuUsage.set(0.0);
        heapUsage.set(0.0);
        assertFalse(service.isNodeInDuress());

        // Node in duress; but not for many consecutive data points.
        cpuUsage.set(1.0);
        heapUsage.set(1.0);
        assertFalse(service.isNodeInDuress());

        // Node in duress for consecutive data points.
        assertFalse(service.isNodeInDuress());
        assertTrue(service.isNodeInDuress());

        // Node not in duress anymore.
        cpuUsage.set(0.0);
        heapUsage.set(0.0);
        assertFalse(service.isNodeInDuress());
    }

    public void testTrackerStateUpdateOnSearchTaskCompletion() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        LongSupplier mockTimeNanosSupplier = () -> TimeUnit.SECONDS.toNanos(1234);
        TaskResourceUsageTracker mockTaskResourceUsageTracker = mock(TaskResourceUsageTracker.class);

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureService service = new SearchBackpressureService(
            settings,
            mockTaskResourceTrackingService,
            threadPool,
            mockTimeNanosSupplier,
            Collections.emptyList(),
            List.of(mockTaskResourceUsageTracker),
            Collections.emptyList(),
            taskManager
        );

        for (int i = 0; i < 100; i++) {
            // service.onTaskCompleted(new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>()));
            service.onTaskCompleted(createMockTaskWithResourceStats(SearchTask.class, 100, 200));
        }
        assertEquals(100, service.getSearchBackpressureState(SearchTask.class).getCompletionCount());
        verify(mockTaskResourceUsageTracker, times(100)).update(any());
    }

    public void testTrackerStateUpdateOnSearchShardTaskCompletion() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        LongSupplier mockTimeNanosSupplier = () -> TimeUnit.SECONDS.toNanos(1234);
        TaskResourceUsageTracker mockTaskResourceUsageTracker = mock(TaskResourceUsageTracker.class);

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureService service = new SearchBackpressureService(
            settings,
            mockTaskResourceTrackingService,
            threadPool,
            mockTimeNanosSupplier,
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(mockTaskResourceUsageTracker),
            taskManager
        );

        // Record task completions to update the tracker state. Tasks other than SearchTask & SearchShardTask are ignored.
        service.onTaskCompleted(createMockTaskWithResourceStats(CancellableTask.class, 100, 200));
        for (int i = 0; i < 100; i++) {
            service.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, 200));
        }
        assertEquals(100, service.getSearchBackpressureState(SearchShardTask.class).getCompletionCount());
        verify(mockTaskResourceUsageTracker, times(100)).update(any());
    }

    public void testSearchTaskInFlightCancellation() {
        TaskManager mockTaskManager = spy(taskManager);
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        AtomicLong mockTime = new AtomicLong(0);
        LongSupplier mockTimeNanosSupplier = mockTime::get;
        NodeDuressTracker mockNodeDuressTracker = new NodeDuressTracker(() -> true);

        TaskResourceUsageTracker mockTaskResourceUsageTracker = getMockedTaskResourceUsageTracker();

        // Mocking 'settings' with predictable rate limiting thresholds.
        SearchBackpressureSettings settings = getBackpressureSettings("enforced", 0.1, 0.003, 5.0);

        SearchBackpressureService service = new SearchBackpressureService(
            settings,
            mockTaskResourceTrackingService,
            threadPool,
            mockTimeNanosSupplier,
            List.of(mockNodeDuressTracker),
            List.of(mockTaskResourceUsageTracker),
            Collections.emptyList(),
            mockTaskManager
        );

        // Run two iterations so that node is marked 'in duress' from the third iteration onwards.
        service.doRun();
        service.doRun();

        // Mocking 'settings' with predictable totalHeapBytesThreshold so that cancellation logic doesn't get skipped.
        long taskHeapUsageBytes = 500;
        SearchTaskSettings searchTaskSettings = mock(SearchTaskSettings.class);
        // setting the total heap percent threshold to minimum so that circuit does not break in SearchBackpressureService
        when(searchTaskSettings.getTotalHeapPercentThreshold()).thenReturn(0.0);
        when(settings.getSearchTaskSettings()).thenReturn(searchTaskSettings);

        // Create a mix of low and high resource usage SearchTasks (50 low + 25 high resource usage tasks).
        Map<Long, Task> activeSearchTasks = new HashMap<>();
        for (long i = 0; i < 75; i++) {
            if (i % 3 == 0) {
                activeSearchTasks.put(i, createMockTaskWithResourceStats(SearchTask.class, 500, taskHeapUsageBytes));
            } else {
                activeSearchTasks.put(i, createMockTaskWithResourceStats(SearchTask.class, 100, taskHeapUsageBytes));
            }
        }
        doReturn(activeSearchTasks).when(mockTaskResourceTrackingService).getResourceAwareTasks();

        // There are 25 SearchTasks eligible for cancellation but only 5 will be cancelled (burst limit).
        service.doRun();
        verify(mockTaskManager, times(5)).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
        assertEquals(1, service.getSearchBackpressureState(SearchTask.class).getLimitReachedCount());

        // If the clock or completed task count haven't made sufficient progress, we'll continue to be rate-limited.
        service.doRun();
        verify(mockTaskManager, times(5)).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
        assertEquals(2, service.getSearchBackpressureState(SearchTask.class).getLimitReachedCount());

        // Fast-forward the clock by ten second to replenish some tokens.
        // This will add 50 tokens (time delta * rate) to 'rateLimitPerTime' but it will cancel only 5 tasks (burst limit).
        mockTime.addAndGet(TimeUnit.SECONDS.toNanos(10));
        service.doRun();
        verify(mockTaskManager, times(10)).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
        assertEquals(3, service.getSearchBackpressureState(SearchTask.class).getLimitReachedCount());

        // Verify search backpressure stats.
        SearchBackpressureStats expectedStats = new SearchBackpressureStats(
            new SearchTaskStats(10, 3, Map.of(TaskResourceUsageTrackerType.CPU_USAGE_TRACKER, new MockStats(10))),
            new SearchShardTaskStats(0, 0, Collections.emptyMap()),
            SearchBackpressureMode.ENFORCED
        );
        SearchBackpressureStats actualStats = service.nodeStats();
        assertEquals(expectedStats, actualStats);
    }

    public void testSearchShardTaskInFlightCancellation() {
        TaskManager mockTaskManager = spy(taskManager);
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        AtomicLong mockTime = new AtomicLong(0);
        LongSupplier mockTimeNanosSupplier = mockTime::get;
        NodeDuressTracker mockNodeDuressTracker = new NodeDuressTracker(() -> true);

        TaskResourceUsageTracker mockTaskResourceUsageTracker = getMockedTaskResourceUsageTracker();

        // Mocking 'settings' with predictable rate limiting thresholds.
        SearchBackpressureSettings settings = getBackpressureSettings("enforced", 0.1, 0.003, 10.0);

        SearchBackpressureService service = new SearchBackpressureService(
            settings,
            mockTaskResourceTrackingService,
            threadPool,
            mockTimeNanosSupplier,
            List.of(mockNodeDuressTracker),
            Collections.emptyList(),
            List.of(mockTaskResourceUsageTracker),
            mockTaskManager
        );

        // Run two iterations so that node is marked 'in duress' from the third iteration onwards.
        service.doRun();
        service.doRun();

        // Mocking 'settings' with predictable totalHeapBytesThreshold so that cancellation logic doesn't get skipped.
        long taskHeapUsageBytes = 500;
        SearchShardTaskSettings searchShardTaskSettings = mock(SearchShardTaskSettings.class);
        // setting the total heap percent threshold to minimum so that circuit does not break in SearchBackpressureService
        when(searchShardTaskSettings.getTotalHeapPercentThreshold()).thenReturn(0.0);
        when(settings.getSearchShardTaskSettings()).thenReturn(searchShardTaskSettings);

        // Create a mix of low and high resource usage tasks (60 low + 15 high resource usage tasks).
        Map<Long, Task> activeSearchShardTasks = new HashMap<>();
        for (long i = 0; i < 75; i++) {
            if (i % 5 == 0) {
                activeSearchShardTasks.put(i, createMockTaskWithResourceStats(SearchShardTask.class, 500, taskHeapUsageBytes));
            } else {
                activeSearchShardTasks.put(i, createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
            }
        }
        doReturn(activeSearchShardTasks).when(mockTaskResourceTrackingService).getResourceAwareTasks();

        // There are 15 SearchShardTasks eligible for cancellation but only 10 will be cancelled (burst limit).
        service.doRun();
        verify(mockTaskManager, times(10)).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
        assertEquals(1, service.getSearchBackpressureState(SearchShardTask.class).getLimitReachedCount());

        // If the clock or completed task count haven't made sufficient progress, we'll continue to be rate-limited.
        service.doRun();
        verify(mockTaskManager, times(10)).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
        assertEquals(2, service.getSearchBackpressureState(SearchShardTask.class).getLimitReachedCount());

        // Simulate task completion to replenish some tokens.
        // This will add 2 tokens (task count delta * cancellationRatio) to 'rateLimitPerTaskCompletion'.
        for (int i = 0; i < 20; i++) {
            service.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
        }
        service.doRun();
        verify(mockTaskManager, times(12)).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
        assertEquals(3, service.getSearchBackpressureState(SearchShardTask.class).getLimitReachedCount());

        // Verify search backpressure stats.
        SearchBackpressureStats expectedStats = new SearchBackpressureStats(
            new SearchTaskStats(0, 0, Collections.emptyMap()),
            new SearchShardTaskStats(12, 3, Map.of(TaskResourceUsageTrackerType.CPU_USAGE_TRACKER, new MockStats(12))),
            SearchBackpressureMode.ENFORCED
        );
        SearchBackpressureStats actualStats = service.nodeStats();
        assertEquals(expectedStats, actualStats);
    }

    private SearchBackpressureSettings getBackpressureSettings(String mode, double ratio, double rate, double burst) {
        return spy(
            new SearchBackpressureSettings(
                Settings.builder().put(SearchBackpressureSettings.SETTING_MODE.getKey(), mode).build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        );
    }

    private TaskResourceUsageTracker getMockedTaskResourceUsageTracker() {
        return new TaskResourceUsageTracker() {
            @Override
            public String name() {
                return TaskResourceUsageTrackerType.CPU_USAGE_TRACKER.getName();
            }

            @Override
            public void update(Task task) {}

            @Override
            public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
                if (task.getTotalResourceStats().getCpuTimeInNanos() < 300) {
                    return Optional.empty();
                }

                return Optional.of(new TaskCancellation.Reason("limits exceeded", 5));
            }

            @Override
            public Stats stats(List<? extends Task> tasks) {
                return new MockStats(getCancellations());
            }
        };
    }

    private static class MockStats implements TaskResourceUsageTracker.Stats {
        private final long cancellationCount;

        public MockStats(long cancellationCount) {
            this.cancellationCount = cancellationCount;
        }

        public MockStats(StreamInput in) throws IOException {
            this(in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("cancellation_count", cancellationCount).endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(cancellationCount);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MockStats mockStats = (MockStats) o;
            return cancellationCount == mockStats.cancellationCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cancellationCount);
        }
    }
}
