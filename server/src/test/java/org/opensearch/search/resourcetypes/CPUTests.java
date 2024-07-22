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
import org.opensearch.test.OpenSearchTestCase;

import org.mockito.Mockito;

public class CPUTests extends OpenSearchTestCase {

    public void testGetResourceUsage() {
        Task mockTask = Mockito.mock(Task.class);
        TaskResourceUsage mockStats = Mockito.mock(TaskResourceUsage.class);
        Mockito.when(mockTask.getTotalResourceStats()).thenReturn(mockStats);
        Mockito.when(mockStats.getCpuTimeInNanos()).thenReturn(1024L);

        CPU cpu = new CPU();
        assertEquals(1024L, cpu.getResourceUsage(mockTask));
    }

    public void testGetName() {
        CPU cpu = new CPU();
        assertEquals("cpu", cpu.getName());
    }

    public void testEquals() {
        CPU cpu1 = new CPU();
        CPU cpu2 = new CPU();
        Object notCPU = new Object();

        assertTrue(cpu1.equals(cpu2));
        assertFalse(cpu1.equals(notCPU));
    }

    public void testHashCode() {
        CPU cpu = new CPU();
        assertEquals("CPU".hashCode(), cpu.hashCode());
    }
}
