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
}
