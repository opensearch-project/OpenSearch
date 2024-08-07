/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LongestRunningTaskFirstStrategySelectionStrategyTests extends OpenSearchTestCase {
    public void testSortingCondition() {
        Task task1 = mock(Task.class);
        Task task2 = mock(Task.class);
        Task task3 = mock(Task.class);
        when(task1.getStartTime()).thenReturn(100L);
        when(task2.getStartTime()).thenReturn(200L);
        when(task3.getStartTime()).thenReturn(300L);

        List<Task> tasks = Arrays.asList(task2, task1, task3);
        tasks.sort(new LongestRunningTaskFirstSelectionStrategy().sortingCondition());

        assertEquals(Arrays.asList(task1, task2, task3), tasks);
    }
}