/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceTypeTests extends OpenSearchTestCase {

    public void testFromName() {
        assertSame(ResourceType.CPU, ResourceType.fromName("cpu"));
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("CPU"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Cpu"); });

        assertSame(ResourceType.MEMORY, ResourceType.fromName("memory"));
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Memory"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("MEMORY"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("JVM"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Heap"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Disk"); });
    }

    public void testGetName() {
        assertEquals("cpu", ResourceType.CPU.getName());
        assertEquals("memory", ResourceType.MEMORY.getName());
    }

    public void testGetResourceUsage() {
        SearchShardTask mockTask = createMockTask(SearchShardTask.class, 100, 200);
        assertEquals(100, ResourceType.CPU.getResourceUsage(mockTask));
        assertEquals(200, ResourceType.MEMORY.getResourceUsage(mockTask));
    }

    private <T extends CancellableTask> T createMockTask(Class<T> type, long cpuUsage, long heapUsage) {
        T task = mock(type);
        when(task.getTotalResourceUtilization(ResourceStats.CPU)).thenReturn(cpuUsage);
        when(task.getTotalResourceUtilization(ResourceStats.MEMORY)).thenReturn(heapUsage);
        return task;
    }
}
