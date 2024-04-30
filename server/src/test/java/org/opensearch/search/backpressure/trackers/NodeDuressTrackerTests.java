/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicReference;

public class NodeDuressTrackerTests extends OpenSearchTestCase {

    public void testNodeDuressTracker() {
        AtomicReference<Double> cpuUsage = new AtomicReference<>(0.0);
        NodeDuressTrackers.NodeDuressTracker tracker = new NodeDuressTrackers.NodeDuressTracker(() -> cpuUsage.get() >= 0.5, () -> 3);

        // Node not in duress.
        assertFalse(tracker.test());

        // Node in duress; the streak must keep increasing.
        cpuUsage.set(0.7);
        assertFalse(tracker.test());
        assertFalse(tracker.test());
        assertTrue(tracker.test());

        // Node not in duress anymore.
        cpuUsage.set(0.3);
        assertFalse(tracker.test());
        assertFalse(tracker.test());
    }
}
