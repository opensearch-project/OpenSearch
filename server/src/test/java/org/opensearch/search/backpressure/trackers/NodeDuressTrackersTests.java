/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.search.backpressure.trackers.NodeDuressTrackers.NodeDuressTracker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.ResourceType;

import java.util.EnumMap;
import java.util.function.BooleanSupplier;

public class NodeDuressTrackersTests extends OpenSearchTestCase {

    final BooleanSupplier resourceCacheExpiryChecker = () -> true;

    public void testNodeNotInDuress() {
        EnumMap<ResourceType, NodeDuressTracker> map = new EnumMap<>(ResourceType.class) {
            {
                put(ResourceType.MEMORY, new NodeDuressTracker(() -> false, () -> 2));
                put(ResourceType.CPU, new NodeDuressTracker(() -> false, () -> 2));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map, resourceCacheExpiryChecker);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenHeapInDuress() {
        EnumMap<ResourceType, NodeDuressTracker> map = new EnumMap<>(ResourceType.class) {
            {
                put(ResourceType.MEMORY, new NodeDuressTracker(() -> true, () -> 6));
                put(ResourceType.CPU, new NodeDuressTracker(() -> false, () -> 1));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map, resourceCacheExpiryChecker);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenCPUInDuress() {
        EnumMap<ResourceType, NodeDuressTracker> map = new EnumMap<>(ResourceType.class) {
            {
                put(ResourceType.MEMORY, new NodeDuressTracker(() -> false, () -> 1));
                put(ResourceType.CPU, new NodeDuressTracker(() -> true, () -> 5));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map, resourceCacheExpiryChecker);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenCPUAndHeapInDuress() {
        EnumMap<ResourceType, NodeDuressTracker> map = new EnumMap<>(ResourceType.class) {
            {
                put(ResourceType.MEMORY, new NodeDuressTracker(() -> true, () -> 6));
                put(ResourceType.CPU, new NodeDuressTracker(() -> true, () -> 5));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map, resourceCacheExpiryChecker);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenNativeMemoryInDuress() {
        // Only NATIVE_MEMORY tracker registered. With breach threshold 9, isNodeInDuress
        // crosses the threshold on the third call (each isNodeInDuress invocation triggers
        // updateCache once per ResourceType value, so each call increments the streak by
        // ResourceType.values().length).
        EnumMap<ResourceType, NodeDuressTracker> map = new EnumMap<>(ResourceType.class) {
            {
                put(ResourceType.NATIVE_MEMORY, new NodeDuressTracker(() -> true, () -> 9));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map, resourceCacheExpiryChecker);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertTrue(nodeDuressTrackers.isNodeInDuress());
        assertTrue(nodeDuressTrackers.isNativeMemoryInDuress());
    }

    public void testIsNativeMemoryInDuressFalseWhenUnregistered() {
        // CPU + MEMORY only — NATIVE_MEMORY missing entirely. Per
        // NodeDuressTrackers#updateCache, missing trackers must be treated as "not in duress"
        // rather than throwing on the absent enum value.
        EnumMap<ResourceType, NodeDuressTracker> map = new EnumMap<>(ResourceType.class) {
            {
                put(ResourceType.CPU, new NodeDuressTracker(() -> true, () -> 1));
                put(ResourceType.MEMORY, new NodeDuressTracker(() -> false, () -> 1));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map, resourceCacheExpiryChecker);
        // Drive isResourceInDuress to populate the cache for all enum values.
        assertTrue(nodeDuressTrackers.isResourceInDuress(ResourceType.CPU));
        assertFalse(nodeDuressTrackers.isResourceInDuress(ResourceType.MEMORY));
        // NATIVE_MEMORY was not registered — must be reported as not in duress.
        assertFalse(nodeDuressTrackers.isResourceInDuress(ResourceType.NATIVE_MEMORY));
        assertFalse(nodeDuressTrackers.isNativeMemoryInDuress());
    }

    public void testIsNativeMemoryInDuressFalseWhenStreakNotMet() {
        // Tracker is registered but breach threshold not reached. With threshold 100 the
        // streak never crosses regardless of how many calls we make in this test loop.
        EnumMap<ResourceType, NodeDuressTracker> map = new EnumMap<>(ResourceType.class) {
            {
                put(ResourceType.NATIVE_MEMORY, new NodeDuressTracker(() -> true, () -> 100));
            }
        };
        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map, resourceCacheExpiryChecker);
        for (int i = 0; i < 10; i++) {
            assertFalse(nodeDuressTrackers.isNativeMemoryInDuress());
        }
    }
}
