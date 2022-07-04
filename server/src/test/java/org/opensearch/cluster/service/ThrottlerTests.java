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

    public void testThrottling() {
        Throttler throttler = new Throttler();
        throttler.updateThrottlingLimit("testKey", 5);
        // total acquired permits = 0, available permit = 5

        boolean firstCall = throttler.acquire("testKey", 1);
        // total acquired permit = 1, available permit = 4
        assertTrue(firstCall);

        boolean secondCall = throttler.acquire("testKey", 2);
        // total acquired permit = 3, available permit = 2
        assertTrue(secondCall);

        // since available permits are 2 and trying to acquire 4 permit, it should return false.
        boolean thirdCall = throttler.acquire("testKey", 4);
        assertFalse(thirdCall);

        // releasing 2 permits
        throttler.release("testKey", 2);
        // total acquired permits = 1, available permits = 4

        boolean fourthCall = throttler.acquire("testKey", 4);
        // total acquired permits = 5, available permits = 0
        assertTrue(fourthCall);

        // since available permits are 0, below acquire should return false.
        boolean fifthCall = throttler.acquire("testKey", 1);
        assertFalse(fifthCall);

        // updating limit of throttler to 6 permits.
        throttler.updateThrottlingLimit("testKey", 6);
        // total acquired permits = 5, available permits = 1

        // acquire should pass now as there is 1 available permit.
        boolean sixthCall = throttler.acquire("testKey", 1);
        assertTrue(sixthCall);
    }

    // Test for Throttler for keys where we haven't configured limits.
    // Default behavior for those case is to allow it.
    public void testThrottlingWithoutLimit() {
        Throttler throttler = new Throttler();
        boolean firstCall = throttler.acquire("testKey", 1);
        assertTrue(firstCall);

        boolean secondCall = throttler.acquire("testKey", 2000);
        assertTrue(secondCall);
    }

    public void testRemoveThrottlingLimit() {
        Throttler throttler = new Throttler();
        throttler.updateThrottlingLimit("testKey", 5);
        // Total acquired permit = 0, available permit = 5

        boolean firstCall = throttler.acquire("testKey", 1);
        // Total acquired permit = 1, available permit = 4
        assertTrue(firstCall);

        // Since available permits are 4, below acquire should return false.
        boolean secondCall = throttler.acquire("testKey", 5);
        assertFalse(secondCall);

        // removing throttling limit for key, so it should return true for any acquire permit.
        throttler.removeThrottlingLimit("testKey");
        boolean thirdCall = throttler.acquire("testKey", 500);
        assertTrue(thirdCall);
    }

    public void testUpdateLimitForThrottlingSemaphore() {
        int initialLimit = randomInt(10);
        AdjustableSemaphore semaphore = new AdjustableSemaphore(initialLimit, true);
        assertEquals(initialLimit, semaphore.availablePermits());

        int newIncreasedLimit = randomIntBetween(initialLimit, 20);
        semaphore.setMaxPermits(newIncreasedLimit);
        assertEquals(newIncreasedLimit, semaphore.availablePermits());

        int newDecreasedLimit = randomInt(newIncreasedLimit - 1);
        semaphore.setMaxPermits(newDecreasedLimit);
        assertEquals(newDecreasedLimit, semaphore.availablePermits());
    }
}
