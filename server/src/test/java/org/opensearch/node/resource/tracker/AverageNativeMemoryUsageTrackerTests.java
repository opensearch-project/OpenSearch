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

/**
 * Tests for {@link AverageNativeMemoryUsageTracker}. Uses the package-private test constructor
 * so RSS, heap-committed, non-heap-committed, and native-memory-cap inputs are deterministic.
 */
public class AverageNativeMemoryUsageTrackerTests extends OpenSearchTestCase {

    private static final long ONE_GB = 1024L * 1024L * 1024L;
    private static final long RSS_ANON_BYTES = 2L * ONE_GB;
    private static final long HEAP_COMMITTED_BYTES = 1L * ONE_GB;
    private static final long NON_HEAP_COMMITTED_BYTES = 256L * 1024L * 1024L; // 256 MB
    private static final long NATIVE_MEMORY_CAP_BYTES = 4L * ONE_GB;

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
            TimeValue.timeValueSeconds(30),
            () -> RSS_ANON_BYTES,
            () -> HEAP_COMMITTED_BYTES,
            () -> NON_HEAP_COMMITTED_BYTES,
            () -> NATIVE_MEMORY_CAP_BYTES
        );
        try {
            // nativeUsed = max(0, 2GB - 1GB - 256MB) = 768MB; percent = (768MB * 100) / 4GB = 18
            assertEquals(18L, tracker.getUsage());
        } finally {
            tracker.doStop();
            tracker.close();
        }
    }

    public void testGetUsageSubtractsNonHeap() {
        AverageNativeMemoryUsageTracker tracker = new AverageNativeMemoryUsageTracker(
            threadPool,
            TimeValue.timeValueMillis(500),
            TimeValue.timeValueSeconds(30),
            () -> RSS_ANON_BYTES,
            () -> HEAP_COMMITTED_BYTES,
            () -> 0L, // no non-heap subtraction
            () -> NATIVE_MEMORY_CAP_BYTES
        );
        try {
            // Without non-heap: nativeUsed = 2GB - 1GB = 1GB; percent = 25
            assertEquals(25L, tracker.getUsage());
        } finally {
            tracker.doStop();
            tracker.close();
        }
    }

    public void testGetUsageReturnsZeroWhenEffectiveMemoryUnavailable() {
        AverageNativeMemoryUsageTracker tracker = new AverageNativeMemoryUsageTracker(
            threadPool,
            TimeValue.timeValueMillis(500),
            TimeValue.timeValueSeconds(30),
            () -> RSS_ANON_BYTES,
            () -> HEAP_COMMITTED_BYTES,
            () -> NON_HEAP_COMMITTED_BYTES,
            () -> 0L // no limit configured, auto-detection failed
        );
        try {
            assertEquals(0L, tracker.getUsage());
        } finally {
            tracker.doStop();
            tracker.close();
        }
    }

    public void testTrackerBecomesReady() throws Exception {
        AverageNativeMemoryUsageTracker tracker = new AverageNativeMemoryUsageTracker(
            threadPool,
            TimeValue.timeValueMillis(100),
            TimeValue.timeValueMillis(500),
            () -> RSS_ANON_BYTES,
            () -> HEAP_COMMITTED_BYTES,
            () -> NON_HEAP_COMMITTED_BYTES,
            () -> NATIVE_MEMORY_CAP_BYTES
        );
        try {
            assertFalse(tracker.isReady());
            tracker.doStart();
            assertBusy(() -> assertTrue(tracker.isReady()), 5, TimeUnit.SECONDS);
            assertBusy(() -> assertThat(tracker.getAverage(), greaterThanOrEqualTo(0.0)), 5, TimeUnit.SECONDS);
        } finally {
            tracker.doStop();
            tracker.close();
        }
    }
}
