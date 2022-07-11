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

import java.util.Optional;

/**
 * Contains tests of {@link Throttler}
 */
public class ThrottlerTests extends OpenSearchTestCase {

    public void testThrottling() {
        Throttler throttler = new Throttler();
        throttler.updateThrottlingLimit("testKey", 5);
        // total acquired permits = 0, available permit = 5

        Optional<Boolean> firstCall = throttler.acquire("testKey", 1);
        // total acquired permit = 1, available permit = 4
        assertTrue(firstCall.get());

        Optional<Boolean> secondCall = throttler.acquire("testKey", 2);
        // total acquired permit = 3, available permit = 2
        assertTrue(secondCall.get());

        // since available permits are 2 and trying to acquire 4 permit, it should return false.
        Optional<Boolean> thirdCall = throttler.acquire("testKey", 4);
        assertFalse(thirdCall.get());

        // releasing 2 permits
        throttler.release("testKey", 2);
        // total acquired permits = 1, available permits = 4

        Optional<Boolean> fourthCall = throttler.acquire("testKey", 4);
        // total acquired permits = 5, available permits = 0
        assertTrue(fourthCall.get());

        // since available permits are 0, below acquire should return false.
        Optional<Boolean> fifthCall = throttler.acquire("testKey", 1);
        assertFalse(fifthCall.get());

        // updating limit of throttler to 6 permits.
        throttler.updateThrottlingLimit("testKey", 6);
        // total acquired permits = 5, available permits = 1

        // acquire should pass now as there is 1 available permit.
        Optional<Boolean> sixthCall = throttler.acquire("testKey", 1);
        // total acquired permits = 6, available permits = 0
        assertTrue(sixthCall.get());

        // acquire should fail now as again there is not any available permit
        Optional<Boolean> seventhCall = throttler.acquire("testKey", 1);
        assertFalse(seventhCall.get());
    }

    // Test for Throttler for keys where we haven't configured limits.
    // Default behavior for those case is to allow it.
    public void testThrottlingWithoutLimit() {
        Throttler throttler = new Throttler();
        Optional<Boolean> firstCall = throttler.acquire("testKey", 1);
        assertTrue(firstCall.isEmpty());

        Optional<Boolean> secondCall = throttler.acquire("testKey", 2000);
        assertTrue(secondCall.isEmpty());
    }

    public void testRemoveThrottlingLimit() {
        Throttler throttler = new Throttler();
        throttler.updateThrottlingLimit("testKey", 5);
        // Total acquired permit = 0, available permit = 5

        Optional<Boolean> firstCall = throttler.acquire("testKey", 1);
        // Total acquired permit = 1, available permit = 4
        assertTrue(firstCall.get());

        // Since available permits are 4, below acquire should return false.
        Optional<Boolean> secondCall = throttler.acquire("testKey", 5);
        assertFalse(secondCall.get());

        // removing throttling limit for key, so it should return true for any acquire permit.
        throttler.removeThrottlingLimit("testKey");
        Optional<Boolean> thirdCall = throttler.acquire("testKey", 500);
        assertTrue(thirdCall.isEmpty());
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
