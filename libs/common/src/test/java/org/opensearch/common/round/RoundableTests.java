/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import org.opensearch.test.OpenSearchTestCase;

public class RoundableTests extends OpenSearchTestCase {

    public void testFloor() {
        int size = randomIntBetween(1, 256);
        long[] values = new long[size];
        for (int i = 1; i < values.length; i++) {
            values[i] = values[i - 1] + (randomNonNegativeLong() % 200) + 1;
        }

        Roundable[] impls = { new BinarySearcher(values, size), new BidirectionalLinearSearcher(values, size) };

        for (int i = 0; i < 100000; i++) {
            // Index of the expected round-down point.
            int idx = randomIntBetween(0, size - 1);

            // Value of the expected round-down point.
            long expected = values[idx];

            // Delta between the expected and the next round-down point.
            long delta = (idx < size - 1) ? (values[idx + 1] - values[idx]) : 200;

            // Adding a random delta between 0 (inclusive) and delta (exclusive) to the expected
            // round-down point, which will still floor to the same value.
            long key = expected + (randomNonNegativeLong() % delta);

            for (Roundable roundable : impls) {
                assertEquals(expected, roundable.floor(key));
            }
        }
    }

    public void testFailureCases() {
        Throwable throwable;

        throwable = assertThrows(IllegalArgumentException.class, () -> new BinarySearcher(new long[0], 0));
        assertEquals("at least one value must be present", throwable.getMessage());
        throwable = assertThrows(IllegalArgumentException.class, () -> new BidirectionalLinearSearcher(new long[0], 0));
        assertEquals("at least one value must be present", throwable.getMessage());

        throwable = assertThrows(AssertionError.class, () -> new BinarySearcher(new long[] { 100 }, 1).floor(50));
        assertEquals("key must be greater than or equal to 100", throwable.getMessage());
        throwable = assertThrows(AssertionError.class, () -> new BidirectionalLinearSearcher(new long[] { 100 }, 1).floor(50));
        assertEquals("key must be greater than or equal to 100", throwable.getMessage());
    }
}
