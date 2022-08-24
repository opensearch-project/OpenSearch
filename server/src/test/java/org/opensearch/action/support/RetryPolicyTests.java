/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Iterator;

public class RetryPolicyTests extends OpenSearchTestCase {

    public void testEqualJitterExponentialBackOffPolicy() {
        int baseDelay = 10;
        int maxDelay = 10000;
        RetryPolicy policy = RetryPolicy.exponentialEqualJitterBackoff(baseDelay, maxDelay);
        Iterator<TimeValue> iterator = policy.iterator();

        // Assert equal jitter
        int retriesTillMaxDelay = 10;
        for (int i = 0; i < retriesTillMaxDelay; i++) {
            TimeValue delay = iterator.next();
            assertTrue(delay.getMillis() >= baseDelay * (1L << i) / 2);
            assertTrue(delay.getMillis() <= baseDelay * (1L << i));
        }

        // Now policy should return max delay for next retries.
        int retriesAfterMaxDelay = randomInt(10);
        for (int i = 0; i < retriesAfterMaxDelay; i++) {
            TimeValue delay = iterator.next();
            assertTrue(delay.getMillis() >= maxDelay / 2);
            assertTrue(delay.getMillis() <= maxDelay);
        }
    }

    public void testExponentialBackOffPolicy() {
        long baseDelay = 10;
        int maxDelay = 10000;
        long currentDelay = baseDelay;
        RetryPolicy policy = RetryPolicy.exponentialBackoff(baseDelay);
        Iterator<TimeValue> iterator = policy.iterator();

        // Assert equal jitter
        int numberOfRetries = randomInt(20);

        for (int i = 0; i < numberOfRetries; i++) {
            TimeValue delay = iterator.next();
            assertTrue(delay.getMillis() >= 0);
            assertTrue(delay.getMillis() <= currentDelay);
            currentDelay = currentDelay * 2;
        }
    }
}
