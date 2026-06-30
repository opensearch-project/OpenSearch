/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;

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

        assertSame(ResourceType.NATIVE_MEMORY, ResourceType.fromName("native_memory"));
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("NATIVE_MEMORY"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Native_Memory"); });
    }

    public void testGetName() {
        assertEquals("cpu", ResourceType.CPU.getName());
        assertEquals("memory", ResourceType.MEMORY.getName());
        assertEquals("native_memory", ResourceType.NATIVE_MEMORY.getName());
    }

    public void testNativeMemoryStatsDisabled() {
        // statsEnabled=false ensures WorkloadGroupState skips allocating slots for NATIVE_MEMORY.
        assertFalse(ResourceType.NATIVE_MEMORY.hasStatsEnabled());
        // CPU and MEMORY remain stats-enabled; this guards against accidental refactors.
        assertTrue(ResourceType.CPU.hasStatsEnabled());
        assertTrue(ResourceType.MEMORY.hasStatsEnabled());
    }

    public void testNativeMemoryUsesPlaceholderCalculator() {
        // Calculator is a no-op (returns 0.0) so accidental iteration over NATIVE_MEMORY by
        // a WLM stats path does not produce spurious numbers.
        assertSame(
            org.opensearch.wlm.tracker.NativeMemoryUsageCalculator.INSTANCE,
            ResourceType.NATIVE_MEMORY.getResourceUsageCalculator()
        );
    }
}
