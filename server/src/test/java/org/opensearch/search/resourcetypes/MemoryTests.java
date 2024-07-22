/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resourcetypes;

import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.tasks.Task;

import org.mockito.Mockito;

public class MemoryTests extends ResourceTypeTests {

    public void testGetResourceUsage() {
        Task mockTask = Mockito.mock(Task.class);
        TaskResourceUsage mockStats = Mockito.mock(TaskResourceUsage.class);
        Mockito.when(mockTask.getTotalResourceStats()).thenReturn(mockStats);
        Mockito.when(mockStats.getMemoryInBytes()).thenReturn(1024L);

        Memory memory = new Memory();
        assertEquals(1024L, memory.getResourceUsage(mockTask));
    }

    public void testGetName() {
        Memory memory = new Memory();
        assertEquals("memory", memory.getName());
    }

    public void testEquals() {
        Memory memory1 = new Memory();
        Memory memory2 = new Memory();
        Object notMemory = new Object();

        assertTrue(memory1.equals(memory2));
        assertFalse(memory1.equals(notMemory));
    }

    public void testHashCode() {
        Memory memory = new Memory();
        assertEquals("Memory".hashCode(), memory.hashCode());
    }
}
