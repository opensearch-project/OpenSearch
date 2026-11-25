/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeoutTaskCancellationUtilityTests extends OpenSearchTestCase {

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueMinutes(5);

    private final ActionListener<Object> listener = ActionListener.wrap(response -> {}, e -> {});
    private ThreadPool threadPool;
    private NodeClient client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(mock(Scheduler.ScheduledCancellable.class));
        client = new NodeClient(Settings.EMPTY, threadPool);
    }

    public void testExplicitIntervalReplacesTheTasksOwnTimeout() {
        TimeValue interval = TimeValue.timeValueSeconds(1);
        CancellableTask task = task(randomFrom(SearchService.NO_TIMEOUT, TimeValue.timeValueHours(1), null));
        assertNotSame(listener, wrap(task, interval));
        verify(threadPool).schedule(any(), eq(interval), eq(ThreadPool.Names.GENERIC));
    }

    public void testNullIntervalFallsBackToTheDefault() {
        CancellableTask task = task(randomFrom(SearchService.NO_TIMEOUT, TimeValue.timeValueHours(1)));
        assertNotSame(listener, wrap(task, null));
        verify(threadPool).schedule(any(), eq(DEFAULT_TIMEOUT), eq(ThreadPool.Names.GENERIC));
    }

    public void testNoTimeoutIntervalArmsNoTimer() {
        CancellableTask task = task(randomFrom(TimeValue.timeValueSeconds(1), null));
        assertSame(listener, wrap(task, SearchService.NO_TIMEOUT));
        verify(threadPool, never()).schedule(any(), any(), any());
    }

    public void testZeroOrNegativeIntervalIsScheduledRatherThanTurningCancellationOff() {
        // Guards existing behavior, pending a decision on rejecting these values in a workload group's settings: only -1
        // (NO_TIMEOUT, which is -1ms) turns cancellation off, so 0 and negative durations such as -1s cancel the task at once.
        for (TimeValue interval : List.of(TimeValue.ZERO, TimeValue.timeValueSeconds(-1), TimeValue.timeValueMinutes(-1))) {
            assertNotEquals(SearchService.NO_TIMEOUT, interval);
            assertNotSame(listener, wrap(task(null), interval));
            verify(threadPool).schedule(any(), eq(interval), eq(ThreadPool.Names.GENERIC));
        }
    }

    public void testWithoutAnIntervalTheTasksOwnTimeoutApplies() {
        TimeValue taskTimeout = TimeValue.timeValueSeconds(1);
        assertNotSame(
            listener,
            TimeoutTaskCancellationUtility.wrapWithCancellationListener(client, task(taskTimeout), DEFAULT_TIMEOUT, listener, e -> {})
        );
        verify(threadPool).schedule(any(), eq(taskTimeout), eq(ThreadPool.Names.GENERIC));
    }

    public void testWithoutAnIntervalATaskWithoutATimeoutGetsTheDefault() {
        assertNotSame(
            listener,
            TimeoutTaskCancellationUtility.wrapWithCancellationListener(client, task(null), DEFAULT_TIMEOUT, listener, e -> {})
        );
        verify(threadPool).schedule(any(), eq(DEFAULT_TIMEOUT), eq(ThreadPool.Names.GENERIC));
    }

    private ActionListener<Object> wrap(CancellableTask task, TimeValue cancelAfterTimeInterval) {
        return TimeoutTaskCancellationUtility.wrapWithCancellationListener(
            client,
            task,
            cancelAfterTimeInterval,
            DEFAULT_TIMEOUT,
            listener,
            e -> {}
        );
    }

    private static CancellableTask task(TimeValue cancelAfterTimeInterval) {
        return new CancellableTask(
            1,
            "transport",
            "action",
            "test",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            cancelAfterTimeInterval
        ) {
            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return false;
            }
        };
    }
}
