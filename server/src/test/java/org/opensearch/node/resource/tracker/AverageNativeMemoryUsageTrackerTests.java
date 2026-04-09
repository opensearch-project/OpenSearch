/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for AverageNativeMemoryUsageTracker to verify it correctly polls OS-level memory stats
 */
public class AverageNativeMemoryUsageTrackerTests extends OpenSearchTestCase {
    ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testGetUsageReturnsValidPercentage() {
        AverageNativeMemoryUsageTracker tracker = new AverageNativeMemoryUsageTracker(
            threadPool,
            TimeValue.timeValueMillis(500),
            TimeValue.timeValueSeconds(30)
        );
        long usage = tracker.getUsage();
        assertThat(usage, greaterThanOrEqualTo(0L));
        assertThat(usage, lessThanOrEqualTo(100L));
    }

    public void testTrackerBecomesReady() throws Exception {
        AverageNativeMemoryUsageTracker tracker = new AverageNativeMemoryUsageTracker(
            threadPool,
            TimeValue.timeValueMillis(100),
            TimeValue.timeValueMillis(500)
        );
        assertFalse(tracker.isReady());
        tracker.doStart();
        assertBusy(() -> assertTrue(tracker.isReady()), 5, TimeUnit.SECONDS);
        assertBusy(() -> assertThat(tracker.getAverage(), greaterThanOrEqualTo(0.0)), 5, TimeUnit.SECONDS);
        tracker.doStop();
        tracker.doClose();
    }
}
