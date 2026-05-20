/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.test.OpenSearchTestCase;

import java.util.function.LongSupplier;

public class TimeBasedCacheExpiryTrackerTests extends OpenSearchTestCase {
    TimeBasedExpiryTracker sut;
    final long ONE_SEC = 1_000_000_000;

    public void testExpiryEvent() {
        TestTimeSupplier testTimeSupplier = new TestTimeSupplier();
        sut = new TimeBasedExpiryTracker(testTimeSupplier);

        testTimeSupplier.advanceClockBy(2 * ONE_SEC);
        assertTrue(sut.getAsBoolean());
    }

    public void testNonExpiryEvent() {
        TestTimeSupplier testTimeSupplier = new TestTimeSupplier();
        sut = new TimeBasedExpiryTracker(testTimeSupplier);

        testTimeSupplier.advanceClockBy(ONE_SEC / 2);
        assertFalse(sut.getAsBoolean());
    }

    public static class TestTimeSupplier implements LongSupplier {
        long currentTime = System.nanoTime();

        @Override
        public long getAsLong() {
            return currentTime;
        }

        public void advanceClockBy(long nanos) {
            currentTime += nanos;
        }
    }
}
