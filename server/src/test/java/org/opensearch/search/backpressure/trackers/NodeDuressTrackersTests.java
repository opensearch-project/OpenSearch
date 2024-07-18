/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.search.backpressure.trackers.NodeDuressTrackers.NodeDuressTracker;
import org.opensearch.search.resourcetypes.ResourceType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;

public class NodeDuressTrackersTests extends OpenSearchTestCase {

    public void testNodeNotInDuress() {
        HashMap<ResourceType, NodeDuressTracker> map = new HashMap<>() {
            {
                put(ResourceType.fromName("jvm"), new NodeDuressTracker(() -> false, () -> 2));
                put(ResourceType.fromName("cpu"), new NodeDuressTracker(() -> false, () -> 2));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenHeapInDuress() {
        HashMap<ResourceType, NodeDuressTracker> map = new HashMap<>() {
            {
                put(ResourceType.fromName("jvm"), new NodeDuressTracker(() -> true, () -> 3));
                put(ResourceType.fromName("cpu"), new NodeDuressTracker(() -> false, () -> 1));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenCPUInDuress() {
        HashMap<ResourceType, NodeDuressTracker> map = new HashMap<>() {
            {
                put(ResourceType.fromName("jvm"), new NodeDuressTracker(() -> false, () -> 1));
                put(ResourceType.fromName("cpu"), new NodeDuressTracker(() -> true, () -> 3));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenCPUAndHeapInDuress() {
        HashMap<ResourceType, NodeDuressTracker> map = new HashMap<>() {
            {
                put(ResourceType.fromName("jvm"), new NodeDuressTracker(() -> true, () -> 3));
                put(ResourceType.fromName("cpu"), new NodeDuressTracker(() -> false, () -> 3));
            }
        };

        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(map);

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }
}
