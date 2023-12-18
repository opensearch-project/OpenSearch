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

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;

public class RoundableTests extends OpenSearchTestCase {

    public void testBidirectionalLinearSearcher() {
        assertRounding(BidirectionalLinearSearcher::new);
    }

    public void testBinarySearcher() {
        assertRounding(BinarySearcher::new);
    }

    @SuppressForbidden(reason = "Reflective construction of BtreeSearcher since it's not supported below Java 20")
    public void testBtreeSearcher() {
        RoundableSupplier supplier;

        try {
            Class<?> clz = MethodHandles.lookup().findClass("org.opensearch.common.round.BtreeSearcher");
            supplier = (values, size) -> {
                try {
                    return (Roundable) clz.getDeclaredConstructor(long[].class, int.class).newInstance(values, size);
                } catch (InvocationTargetException e) {
                    // Failed to instantiate the class. Unwrap if the nested exception is already a runtime exception,
                    // say due to an IllegalArgumentException due to bad constructor arguments.
                    if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else {
                        throw new RuntimeException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
        } catch (ClassNotFoundException e) {
            assumeTrue("BtreeSearcher is not supported below Java 20", false);
            return;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        assertRounding(supplier);
    }

    private void assertRounding(RoundableSupplier supplier) {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> supplier.get(new long[0], 0));
        assertEquals("at least one value must be present", throwable.getMessage());

        int size = randomIntBetween(1, 256);
        long[] values = randomArrayOfSortedValues(size);
        Roundable roundable = supplier.get(values, size);

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

        throwable = assertThrows(AssertionError.class, () -> roundable.floor(values[0] - 1));
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

    @FunctionalInterface
    private interface RoundableSupplier {
        Roundable get(long[] values, int size);
    }
}
