/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

public class RoundableTests extends OpenSearchTestCase {

    @BeforeClass
    @SuppressForbidden(reason = "Sets the feature flag system property to a random value")
    public static void setupClass() {
        System.setProperty("opensearch.experimental.feature.simd.rounding.enabled", String.valueOf(randomBoolean()));
    }

    public void testRoundingEmptyArray() {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> RoundableFactory.create(new long[0], 0));
        assertEquals("at least one value must be present", throwable.getMessage());
    }

    public void testRoundingSmallArray() {
        int size = randomIntBetween(1, 64);
        long[] values = randomArrayOfSortedValues(size);
        Roundable roundable = RoundableFactory.create(values, size);

        assertEquals("BidirectionalLinearSearcher", roundable.getClass().getSimpleName());
        assertRounding(roundable, values, size);
    }

    @SuppressForbidden(reason = "Reads a package-private static field using reflection as it's only available on Java 20 and above")
    public void testRoundingLargeArray() {
        int size = randomIntBetween(65, 256);
        long[] values = randomArrayOfSortedValues(size);
        Roundable roundable = RoundableFactory.create(values, size);

        boolean isBtreeSearchSupported;
        try {
            // Not supported below Java 20.
            isBtreeSearchSupported = RoundableFactory.class.getDeclaredField("IS_BTREE_SEARCH_SUPPORTED").getBoolean(null);
        } catch (NoSuchFieldException e) {
            isBtreeSearchSupported = false;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        assertEquals(isBtreeSearchSupported ? "BtreeSearcher" : "BinarySearcher", roundable.getClass().getSimpleName());
        assertRounding(roundable, values, size);
    }

    private void assertRounding(Roundable roundable, long[] values, int size) {
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

            assertEquals(expected, roundable.floor(key));
        }

        Throwable throwable = assertThrows(AssertionError.class, () -> roundable.floor(values[0] - 1));
        assertEquals("key must be greater than or equal to " + values[0], throwable.getMessage());
    }

    private static long[] randomArrayOfSortedValues(int size) {
        int capacity = size + randomInt(20); // May be slightly more than the size.
        long[] values = new long[capacity];

        for (int i = 1; i < size; i++) {
            values[i] = values[i - 1] + (randomNonNegativeLong() % 200) + 1;
        }

        return values;
    }
}
