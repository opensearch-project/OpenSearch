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

public class NodeResourceUsageTrackerTests extends OpenSearchTestCase {

    public void testNodeResourceUsageTracker() {
        AtomicReference<Double> cpuUsage = new AtomicReference<>(0.0);
        NodeResourceUsageTracker tracker = new NodeResourceUsageTracker(() -> cpuUsage.get() >= 0.5);

        // Node not in duress.
        assertEquals(0, tracker.check());

        // Node in duress; the streak must keep increasing.
        cpuUsage.set(0.7);
        assertEquals(1, tracker.check());
        assertEquals(2, tracker.check());
        assertEquals(3, tracker.check());

        // Node not in duress anymore.
        cpuUsage.set(0.3);
        assertEquals(0, tracker.check());
        assertEquals(0, tracker.check());
    }
}
