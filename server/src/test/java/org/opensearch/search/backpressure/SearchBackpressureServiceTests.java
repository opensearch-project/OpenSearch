/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.trackers.NodeDuressTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class SearchBackpressureServiceTests extends OpenSearchTestCase {

    public void testIsNodeInDuress() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);

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
            mockThreadPool,
            System::nanoTime,
            List.of(cpuUsageTracker, heapUsageTracker),
            Collections.emptyList()
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

    public void testTrackerStateUpdateOnTaskCompletion() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        LongSupplier mockTimeNanosSupplier = () -> TimeUnit.SECONDS.toNanos(1234);
        TaskResourceUsageTracker mockTaskResourceUsageTracker = mock(TaskResourceUsageTracker.class);

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureService service = new SearchBackpressureService(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            Collections.emptyList(),
            List.of(mockTaskResourceUsageTracker)
        );

        // Record task completions to update the tracker state. Tasks other than SearchShardTask are ignored.
        service.onTaskCompleted(createMockTaskWithResourceStats(CancellableTask.class, 100, 200));
        for (int i = 0; i < 100; i++) {
            service.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, 200));
        }
        assertEquals(100, service.getState().getCompletionCount());
        verify(mockTaskResourceUsageTracker, times(100)).update(any());
    }

    public void testInFlightCancellation() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        AtomicLong mockTime = new AtomicLong(0);
        LongSupplier mockTimeNanosSupplier = mockTime::get;
        NodeDuressTracker mockNodeDuressTracker = new NodeDuressTracker(() -> true);

        TaskResourceUsageTracker mockTaskResourceUsageTracker = new TaskResourceUsageTracker() {
            @Override
            public String name() {
                return "mock_tracker";
            }

            @Override
            public void update(Task task) {}

            @Override
            public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
                if (task.getTotalResourceStats().getCpuTimeInNanos() < 300) {
                    return Optional.empty();
                }

                return Optional.of(new TaskCancellation.Reason("limits exceeded", 5));
            }
        };

        // Mocking 'settings' with predictable rate limiting thresholds.
        SearchBackpressureSettings settings = spy(
            new SearchBackpressureSettings(
                Settings.builder()
                    .put(SearchBackpressureSettings.SETTING_ENFORCED.getKey(), true)
                    .put(SearchBackpressureSettings.SETTING_CANCELLATION_RATIO.getKey(), 0.1)
                    .put(SearchBackpressureSettings.SETTING_CANCELLATION_RATE.getKey(), 0.003)
                    .put(SearchBackpressureSettings.SETTING_CANCELLATION_BURST.getKey(), 10.0)
                    .build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        );

        SearchBackpressureService service = new SearchBackpressureService(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            List.of(mockNodeDuressTracker),
            List.of(mockTaskResourceUsageTracker)
        );

        // Run two iterations so that node is marked 'in duress' from the third iteration onwards.
        service.doRun();
        service.doRun();

        // Mocking 'settings' with predictable totalHeapBytesThreshold so that cancellation logic doesn't get skipped.
        long taskHeapUsageBytes = 500;
        SearchShardTaskSettings shardTaskSettings = mock(SearchShardTaskSettings.class);
        when(shardTaskSettings.getTotalHeapBytesThreshold()).thenReturn(taskHeapUsageBytes);
        when(settings.getSearchShardTaskSettings()).thenReturn(shardTaskSettings);

        // Create a mix of low and high resource usage tasks (60 low + 15 high resource usage tasks).
        Map<Long, Task> activeTasks = new HashMap<>();
        for (long i = 0; i < 75; i++) {
            if (i % 5 == 0) {
                activeTasks.put(i, createMockTaskWithResourceStats(SearchShardTask.class, 500, taskHeapUsageBytes));
            } else {
                activeTasks.put(i, createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
            }
        }
        doReturn(activeTasks).when(mockTaskResourceTrackingService).getResourceAwareTasks();

        // There are 15 tasks eligible for cancellation but only 10 will be cancelled (burst limit).
        service.doRun();
        assertEquals(10, service.getState().getCancellationCount());
        assertEquals(1, service.getState().getLimitReachedCount());

        // If the clock or completed task count haven't made sufficient progress, we'll continue to be rate-limited.
        service.doRun();
        assertEquals(10, service.getState().getCancellationCount());
        assertEquals(2, service.getState().getLimitReachedCount());

        // Simulate task completion to replenish some tokens.
        // This will add 2 tokens (task count delta * cancellationRatio) to 'rateLimitPerTaskCompletion'.
        for (int i = 0; i < 20; i++) {
            service.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
        }
        service.doRun();
        assertEquals(12, service.getState().getCancellationCount());
        assertEquals(3, service.getState().getLimitReachedCount());

        // Fast-forward the clock by one second to replenish some tokens.
        // This will add 3 tokens (time delta * rate) to 'rateLimitPerTime'.
        mockTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
        service.doRun();
        assertEquals(15, service.getState().getCancellationCount());
        assertEquals(3, service.getState().getLimitReachedCount());  // no more tasks to cancel; limit not reached
    }
}
