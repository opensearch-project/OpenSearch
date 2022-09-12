/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.stats.CancellationStats;
import org.opensearch.search.backpressure.stats.CancelledTaskStats;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.ResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;

public class SearchBackpressureManagerTests extends OpenSearchTestCase {

    public void testIsNodeInDuress() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);

        AtomicReference<Double> cpuUsage = new AtomicReference<>();
        AtomicReference<Double> heapUsage = new AtomicReference<>();
        DoubleSupplier cpuUsageSupplier = cpuUsage::get;
        DoubleSupplier heapUsageSupplier = heapUsage::get;

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureManager manager = new SearchBackpressureManager(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            System::nanoTime,
            cpuUsageSupplier,
            heapUsageSupplier,
            Collections.emptyList()
        );

        // Node not in duress.
        cpuUsage.set(0.0);
        heapUsage.set(0.0);
        assertFalse(manager.isNodeInDuress());

        // Node in duress; but not for many consecutive data points.
        cpuUsage.set(1.0);
        heapUsage.set(1.0);
        assertFalse(manager.isNodeInDuress());

        // Node in duress for consecutive data points.
        assertFalse(manager.isNodeInDuress());
        assertTrue(manager.isNodeInDuress());

        // Node not in duress anymore.
        cpuUsage.set(0.0);
        heapUsage.set(0.0);
        assertFalse(manager.isNodeInDuress());
    }

    public void testGetTaskCancellations() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        LongSupplier mockTimeNanosSupplier = () -> TimeUnit.SECONDS.toNanos(1234);
        long cpuTimeThreshold = 100;
        long elapsedTimeThreshold = 500;

        doReturn(
            Map.of(
                1L,
                createMockTaskWithResourceStats(SearchShardTask.class, cpuTimeThreshold + 1, 0, mockTimeNanosSupplier.getAsLong()),
                2L,
                createMockTaskWithResourceStats(
                    SearchShardTask.class,
                    cpuTimeThreshold + 1,
                    0,
                    mockTimeNanosSupplier.getAsLong() - elapsedTimeThreshold
                ),
                3L,
                createMockTaskWithResourceStats(SearchShardTask.class, 0, 0, mockTimeNanosSupplier.getAsLong()),
                4L,
                createMockTaskWithResourceStats(CancellableTask.class, 100, 200)  // generic task; not eligible for search backpressure
            )
        ).when(mockTaskResourceTrackingService).getResourceAwareTasks();

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureManager manager = new SearchBackpressureManager(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            () -> 0,
            () -> 0,
            List.of(new CpuUsageTracker(() -> cpuTimeThreshold), new ElapsedTimeTracker(mockTimeNanosSupplier, () -> elapsedTimeThreshold))
        );

        // There are three search shard tasks.
        List<CancellableTask> searchShardTasks = manager.getSearchShardTasks();
        assertEquals(3, searchShardTasks.size());

        // But only two of them are breaching thresholds.
        List<TaskCancellation> taskCancellations = manager.getTaskCancellations(searchShardTasks);
        assertEquals(2, taskCancellations.size());

        // Task cancellations are sorted in reverse order of the score.
        assertEquals(2, taskCancellations.get(0).getReasons().size());
        assertEquals(1, taskCancellations.get(1).getReasons().size());
        assertEquals(2, taskCancellations.get(0).totalCancellationScore());
        assertEquals(1, taskCancellations.get(1).totalCancellationScore());
    }

    public void testTrackerStateUpdateOnTaskCompletion() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        LongSupplier mockTimeNanosSupplier = () -> TimeUnit.SECONDS.toNanos(1234);
        ResourceUsageTracker mockTracker = mock(ResourceUsageTracker.class);

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureManager manager = new SearchBackpressureManager(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            () -> 0.5,
            () -> 0.5,
            List.of(mockTracker)
        );

        // Record task completions to update the tracker state. Tasks other than SearchShardTask are ignored.
        manager.onTaskCompleted(createMockTaskWithResourceStats(CancellableTask.class, 100, 200));
        for (int i = 0; i < 100; i++) {
            manager.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, 200));
        }
        assertEquals(100, manager.getCompletionCount());
        verify(mockTracker, times(100)).update(any());
    }

    public void testInFlightCancellation() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        AtomicLong mockTime = new AtomicLong(0);
        LongSupplier mockTimeNanosSupplier = mockTime::get;

        class MockStats implements ResourceUsageTracker.Stats {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().endObject();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {}

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                return o != null && getClass() == o.getClass();
            }

            @Override
            public int hashCode() {
                return 0;
            }
        }

        ResourceUsageTracker mockTracker = new ResourceUsageTracker() {
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

                return Optional.of(new TaskCancellation.Reason(this, "limits exceeded", 5));
            }

            @Override
            public Stats currentStats(List<Task> activeTasks) {
                return new MockStats();
            }
        };

        SearchBackpressureSettings settings = spy(
            new SearchBackpressureSettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );

        // Mocking 'settings' with predictable rate limiting thresholds.
        when(settings.getCancellationRatio()).thenReturn(0.1);
        when(settings.getCancellationRate()).thenReturn(0.003);
        when(settings.getCancellationBurst()).thenReturn(10.0);

        SearchBackpressureManager manager = new SearchBackpressureManager(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            () -> 1.0,  // node in duress
            () -> 0.0,
            List.of(mockTracker)
        );

        manager.run();
        manager.run();  // allowing node to be marked 'in duress' from the next iteration
        assertNull(manager.getLastCancelledTaskUsage());

        // Mocking 'settings' with predictable searchHeapThresholdBytes so that cancellation logic doesn't get skipped.
        long taskHeapUsageBytes = 500;
        when(settings.getSearchHeapThresholdBytes()).thenReturn(taskHeapUsageBytes);

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
        manager.run();
        assertEquals(10, manager.getCancellationCount());
        assertEquals(1, manager.getLimitReachedCount());
        assertNotNull(manager.getLastCancelledTaskUsage());

        // If the clock or completed task count haven't made sufficient progress, we'll continue to be rate-limited.
        manager.run();
        assertEquals(10, manager.getCancellationCount());
        assertEquals(2, manager.getLimitReachedCount());

        // Simulate task completion to replenish some tokens.
        // This will add 2 tokens (task count delta * cancellationRatio) to 'rateLimitPerTaskCompletion'.
        for (int i = 0; i < 20; i++) {
            manager.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
        }
        manager.run();
        assertEquals(12, manager.getCancellationCount());
        assertEquals(3, manager.getLimitReachedCount());

        // Fast-forward the clock by one second to replenish some tokens.
        // This will add 3 tokens (time delta * rate) to 'rateLimitPerTime'.
        mockTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
        manager.run();
        assertEquals(15, manager.getCancellationCount());
        assertEquals(3, manager.getLimitReachedCount());  // no more tasks to cancel; limit not reached

        // Verify search backpressure stats.
        SearchBackpressureStats expectedStats = new SearchBackpressureStats(
            Map.of("mock_tracker", new MockStats()),
            new CancellationStats(15, Map.of("mock_tracker", 15L), 3, new CancelledTaskStats(500, taskHeapUsageBytes, 1000000000)),
            true,
            true
        );
        assertEquals(expectedStats, manager.nodeStats());
    }
}
