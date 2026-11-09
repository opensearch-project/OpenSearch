/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link NativeMemoryUsageCalculator}, the placeholder calculator wired to
 * {@link org.opensearch.wlm.ResourceType#NATIVE_MEMORY}. WLM does not account off-heap
 * memory per workload group, so this calculator MUST return zero unconditionally.
 */
public class NativeMemoryUsageCalculatorTests extends OpenSearchTestCase {

    public void testCalculatorIsSingleton() {
        assertSame(NativeMemoryUsageCalculator.INSTANCE, NativeMemoryUsageCalculator.INSTANCE);
    }

    public void testCalculateResourceUsageReturnsZero() {
        // Multi-task input with mocks that have no behavior — calculator must not consult them.
        List<WorkloadGroupTask> tasks = List.of(mock(WorkloadGroupTask.class), mock(WorkloadGroupTask.class));
        assertEquals(0.0d, NativeMemoryUsageCalculator.INSTANCE.calculateResourceUsage(tasks), 0.0d);
    }

    public void testCalculateResourceUsageEmptyList() {
        assertEquals(0.0d, NativeMemoryUsageCalculator.INSTANCE.calculateResourceUsage(List.of()), 0.0d);
    }

    public void testCalculateTaskResourceUsageReturnsZero() {
        WorkloadGroupTask task = mock(WorkloadGroupTask.class);
        assertEquals(0.0d, NativeMemoryUsageCalculator.INSTANCE.calculateTaskResourceUsage(task), 0.0d);
    }
}
