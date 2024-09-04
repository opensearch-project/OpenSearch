/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerServiceTests.TestClock;
import org.opensearch.wlm.tracker.TaskResourceUsageCalculator.TaskCpuUsageCalculator;
import org.opensearch.wlm.tracker.TaskResourceUsageCalculator.TaskMemoryUsageCalculator;

import static org.opensearch.wlm.cancellation.DefaultTaskCancellation.MIN_VALUE;
import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTests.createMockTaskWithResourceStats;
import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.HEAP_SIZE_BYTES;

public class TaskResourceUsageCalculatorTests extends OpenSearchTestCase {
    TaskResourceUsageCalculator sut;

    public void testFactoryMethod() {
        assertTrue(TaskResourceUsageCalculator.from(ResourceType.CPU) instanceof TaskCpuUsageCalculator);
        assertTrue(TaskResourceUsageCalculator.from(ResourceType.MEMORY) instanceof TaskMemoryUsageCalculator);
        assertThrows(IllegalArgumentException.class, () -> TaskMemoryUsageCalculator.from(null));
    }

    public void testTaskCpuUsageCalculator() {
        sut = new TaskCpuUsageCalculator();
        TestClock clock = new TestClock();
        QueryGroupTask task = createMockTaskWithResourceStats(QueryGroupTask.class, 100, 200, 0, 1);
        clock.fastForwardBy(200);

        double expectedUsage = 0.5;
        double actualUsage = sut.calculateFor(task, clock::getTime);
        assertEquals(expectedUsage, actualUsage, MIN_VALUE);
    }

    public void testTaskMemoryUsageCalculator() {
        sut = new TaskMemoryUsageCalculator();
        TestClock clock = new TestClock();
        QueryGroupTask task = createMockTaskWithResourceStats(QueryGroupTask.class, 100, 200, 0, 1);
        clock.fastForwardBy(200);

        double expectedUsage = 200.0 / HEAP_SIZE_BYTES;
        double actualUsage = sut.calculateFor(task, clock::getTime);
        assertEquals(expectedUsage, actualUsage, MIN_VALUE);
    }
}
