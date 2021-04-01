/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.AdjustableSemaphore;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Contains tests of {@link Throttler}
 */
public class ThrottlerTests extends OpenSearchTestCase {

    public void testDisabledThrottling() {
        Throttler throttler = new Throttler(true);
        throttler.updateThrottlingLimit("testKey", 1);
        throttler.setThrottlingEnabled(false);
        boolean firstCall = throttler.acquire("testKey", 5);
        assertTrue(firstCall);
    }

    public void testThrottling() {
        Throttler throttler = new Throttler(true);
        throttler.updateThrottlingLimit("testKey", 5);
        boolean firstCall = throttler.acquire("testKey", 1);
        assertTrue(firstCall);

        boolean secondCall = throttler.acquire("testKey", 2);
        assertTrue(secondCall);

        boolean thirdCall = throttler.acquire("testKey", 4);
        assertFalse(thirdCall);

        // will remove used value
        throttler.release("testKey", 2);
        boolean fourthCall = throttler.acquire("testKey", 4);
        assertTrue(fourthCall);


        boolean fifthCall = throttler.acquire("testKey", 1);
        assertFalse(fifthCall);

        // update limit and check
        throttler.updateThrottlingLimit("testKey", 6);
        boolean sixthCall = throttler.acquire("testKey", 1);
        assertTrue(sixthCall);
    }

    public void testAcquireWithFlippingOfThrottlingFlag() throws Exception {
        String test_task = "test";
        Throttler<String> throttler = new Throttler(true);
        throttler.updateThrottlingLimit(test_task, 1);
        assertTrue(throttler.acquire(test_task, 1));
        assertFalse(throttler.acquire(test_task, 1));

        throttler.setThrottlingEnabled(false);
        throttler.setThrottlingEnabled(true);
        assertEquals(1, throttler.getThrottlingLimit(test_task).intValue());
        assertTrue(throttler.acquire(test_task, 1));
        assertFalse(throttler.acquire(test_task, 1));

    }

    public void testThrottlingWithoutLimit() {
        Throttler throttler = new Throttler(true);
        boolean firstCall = throttler.acquire("testKey", 1);
        assertTrue(firstCall);

        boolean secondCall = throttler.acquire("testKey", 2000);
        assertTrue(secondCall);
    }

    public void testRemoveThrottlingLimit() {
        Throttler throttler = new Throttler(true);
        throttler.updateThrottlingLimit("testKey", 5);

        boolean firstCall = throttler.acquire("testKey", 1);
        assertTrue(firstCall);

        boolean secondCall = throttler.acquire("testKey", 5);
        assertFalse(secondCall);

        throttler.removeThrottlingLimit("testKey");
        boolean thirdCall = throttler.acquire("testKey", 500);
        assertTrue(thirdCall);
    }

    public void testUpdateLimitForThrottlingSemaphore() {
        int initialLimit = randomInt(10);
        AdjustableSemaphore semaphore = new AdjustableSemaphore(initialLimit);
        assertEquals(initialLimit, semaphore.availablePermits());

        int newIncreasedLimit = randomIntBetween(initialLimit, 20);
        semaphore.setMaxPermits(newIncreasedLimit);
        assertEquals(newIncreasedLimit, semaphore.availablePermits());

        int newDecreasedLimit = randomInt(newIncreasedLimit-1);
        semaphore.setMaxPermits(newDecreasedLimit);
        assertEquals(newDecreasedLimit, semaphore.availablePermits());
    }
}
