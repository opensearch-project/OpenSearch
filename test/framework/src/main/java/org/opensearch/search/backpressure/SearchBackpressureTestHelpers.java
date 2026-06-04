/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchBackpressureTestHelpers extends OpenSearchTestCase {

    public static <T extends CancellableTask> T createMockTaskWithResourceStats(Class<T> type, long cpuUsage, long heapUsage, long taskId) {
        return createMockTaskWithResourceStats(type, cpuUsage, heapUsage, 0, taskId);
    }

    public static <T extends CancellableTask> T createMockTaskWithResourceStats(
        Class<T> type,
        long cpuUsage,
        long heapUsage,
        long startTimeNanos,
        long taskId
    ) {
        T task = mock(type);
        when(task.getTotalResourceStats()).thenReturn(new TaskResourceUsage(cpuUsage, heapUsage));
        when(task.getStartTimeNanos()).thenReturn(startTimeNanos);
        when(task.getId()).thenReturn(randomNonNegativeLong());

        AtomicBoolean isCancelled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            isCancelled.set(true);
            return null;
        }).when(task).cancel(anyString());
        doAnswer(invocation -> isCancelled.get()).when(task).isCancelled();

        return task;
    }
}
