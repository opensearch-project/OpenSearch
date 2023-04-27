/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.test.OpenSearchTestCase;

public class MovingAverageTests extends OpenSearchTestCase {

    public void testMovingAverage() {
        MovingAverage ma = new MovingAverage(5);

        // No observations
        assertEquals(0.0, ma.getAverage(), 0.0);
        assertEquals(0, ma.getCount());

        // Not enough observations
        ma.record(1);
        ma.record(2);
        ma.record(3);
        assertEquals(2.0, ma.getAverage(), 0.0);
        assertEquals(3, ma.getCount());
        assertFalse(ma.isReady());

        // Enough observations
        ma.record(4);
        ma.record(5);
        ma.record(6);
        assertEquals(4, ma.getAverage(), 0.0);
        assertEquals(6, ma.getCount());
        assertTrue(ma.isReady());
    }

    public void testMovingAverageWithZeroSize() {
        try {
            new MovingAverage(0);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("window size must be greater than zero"));
            return;
        }

        fail("exception should have been thrown");
    }

    public void testUpdateMovingAverageWindowSize() {
        MovingAverage ma = new MovingAverage(5);
        ma.record(1);
        ma.record(2);
        ma.record(3);
        double avg = ma.getAverage();

        // Test case 1 - Not ready and increasing size
        MovingAverage newMa = ma.copyWithSize(10);
        assertEquals(avg, newMa.getAverage(), 0.0d);

        // Test case 2 - Not ready and decreasing size
        newMa = ma.copyWithSize(2);
        assertEquals(2.5, newMa.getAverage(), 0.0d);

        // Test case 3 - Ready and increasing size
        ma.record(4);
        ma.record(5);
        ma.record(6);
        assertEquals(4.0, ma.getAverage(), 0.0d);

        newMa = ma.copyWithSize(10);
        assertEquals(ma.getAverage(), newMa.getAverage(), 0.0d);

        // Test case 4 - Ready and decreasing size
        newMa = ma.copyWithSize(3);
        assertEquals(5.0, newMa.getAverage(), 0.0d);

        // Test case 5 - Ready, array overwritten and increasing size
        for (int i = 7; i < 20; i++) {
            ma.record(i);
        }
        newMa = ma.copyWithSize(6);
        assertEquals(ma.getAverage(), newMa.getAverage(), 0.0d);

        // Test case 6 - Ready, array overwritten and decreasing size
        newMa = ma.copyWithSize(3);
        assertEquals(18.0, newMa.getAverage(), 0.0d);
    }
}
