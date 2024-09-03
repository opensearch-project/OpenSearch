/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;

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
}
