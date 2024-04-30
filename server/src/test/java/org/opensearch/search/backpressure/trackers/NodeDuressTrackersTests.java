/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.test.OpenSearchTestCase;

public class NodeDuressTrackersTests extends OpenSearchTestCase {

    public void testNodeNotInDuress() {
        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(
            new NodeDuressTrackers.NodeDuressTracker(() -> false, () -> 2),
            new NodeDuressTrackers.NodeDuressTracker(() -> false, () -> 2)
        );

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenHeapInDuress() {
        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(
            new NodeDuressTrackers.NodeDuressTracker(() -> true, () -> 3),
            new NodeDuressTrackers.NodeDuressTracker(() -> false, () -> 1)
        );

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenCPUInDuress() {
        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(
            new NodeDuressTrackers.NodeDuressTracker(() -> false, () -> 1),
            new NodeDuressTrackers.NodeDuressTracker(() -> true, () -> 3)
        );

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }

    public void testNodeInDuressWhenCPUAndHeapInDuress() {
        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(
            new NodeDuressTrackers.NodeDuressTracker(() -> true, () -> 3),
            new NodeDuressTrackers.NodeDuressTracker(() -> true, () -> 3)
        );

        assertFalse(nodeDuressTrackers.isNodeInDuress());
        assertFalse(nodeDuressTrackers.isNodeInDuress());

        // for the third time it should be in duress
        assertTrue(nodeDuressTrackers.isNodeInDuress());
    }
}
