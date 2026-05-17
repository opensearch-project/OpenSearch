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
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for {@link AverageNativeMemoryUsageTracker}. Uses the package-private six-argument
 * test constructor so RSS, heap-committed, and native-memory-cap inputs are deterministic
 * and the percentage computation does not depend on the host's {@code /proc/self/status},
 * JVM heap state, or plugin settings.
 */
public class AverageNativeMemoryUsageTrackerTests extends OpenSearchTestCase {

    private static final long ONE_GB = 1024L * 1024L * 1024L;
    private static final long RSS_ANON_BYTES = 2L * ONE_GB;
    private static final long HEAP_COMMITTED_BYTES = 1L * ONE_GB;
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
        LongSupplier rssAnonSupplier = () -> RSS_ANON_BYTES;
        LongSupplier heapCommittedSupplier = () -> HEAP_COMMITTED_BYTES;
        LongSupplier nativeMemoryCapSupplier = () -> NATIVE_MEMORY_CAP_BYTES;

        AverageNativeMemoryUsageTracker tracker = new AverageNativeMemoryUsageTracker(
            threadPool,
            TimeValue.timeValueMillis(500),
            TimeValue.timeValueSeconds(30),
            rssAnonSupplier,
            heapCommittedSupplier,
            nativeMemoryCapSupplier
        );
        try {
            // usage = max(0, 2GB - 1GB) = 1GB; percent = (1GB * 100) / 4GB = 25.
            assertEquals(25L, tracker.getUsage());
        } finally {
            tracker.doStop();
            tracker.close();
        }
    }

    public void testTrackerBecomesReady() throws Exception {
        LongSupplier rssAnonSupplier = () -> RSS_ANON_BYTES;
        LongSupplier heapCommittedSupplier = () -> HEAP_COMMITTED_BYTES;
        LongSupplier nativeMemoryCapSupplier = () -> NATIVE_MEMORY_CAP_BYTES;

        AverageNativeMemoryUsageTracker tracker = new AverageNativeMemoryUsageTracker(
            threadPool,
            TimeValue.timeValueMillis(100),
            TimeValue.timeValueMillis(500),
            rssAnonSupplier,
            heapCommittedSupplier,
            nativeMemoryCapSupplier
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
