/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class ReorganizingLongHashTests extends OpenSearchTestCase {

    public void testFuzzy() {
        Map<Long, Long> reference = new HashMap<>();

        try (
            ReorganizingLongHash h = new ReorganizingLongHash(
                randomIntBetween(1, 100),      // random capacity
                0.6f + randomFloat() * 0.39f,  // random load factor to verify collision resolution
                BigArrays.NON_RECYCLING_INSTANCE
            )
        ) {
            // Verify the behaviour of "add" and "find".
            for (int i = 0; i < (1 << 20); i++) {
                long key = randomLong() % (1 << 12);  // roughly ~4% unique keys
                if (reference.containsKey(key)) {
                    long expectedOrdinal = reference.get(key);
                    assertEquals(-1 - expectedOrdinal, h.add(key));
                    assertEquals(expectedOrdinal, h.find(key));
                } else {
                    assertEquals(-1, h.find(key));
                    reference.put(key, (long) reference.size());
                    assertEquals((long) reference.get(key), h.add(key));
                }
            }

            // Verify the behaviour of "get".
            for (Map.Entry<Long, Long> entry : reference.entrySet()) {
                assertEquals((long) entry.getKey(), h.get(entry.getValue()));
            }

            // Verify the behaviour of "size".
            assertEquals(reference.size(), h.size());

            // Verify the calculation of PSLs.
            final long capacity = h.getTable().size();
            final long mask = capacity - 1;
            for (long idx = 0; idx < h.getTable().size(); idx++) {
                final long value = h.getTable().get(idx);
                if (value != -1) {
                    final long homeIdx = h.hash(h.get((int) value)) & mask;
                    assertEquals((capacity + idx - homeIdx) & mask, value >>> 48);
                }
            }
        }
    }

    public void testRearrangement() {
        try (ReorganizingLongHash h = new ReorganizingLongHash(4, 0.6f, BigArrays.NON_RECYCLING_INSTANCE) {
            /**
             * Overriding with an "identity" hash function to make it easier to reason about the placement
             * of values in the hash table. The backing array of the hash table will have a size (8),
             * i.e. nextPowerOfTwo(initialCapacity/loadFactor), so the bitmask will be (7).
             * The ideal home slot of a key can then be defined as: (hash(key) & mask) = (key & 7).
             */
            @Override
            long hash(long key) {
                return key;
            }
        }) {
            /*
             * Add key=0, hash=0, home_slot=0
             *
             * Before: empty slot.
             *   ▼
             * [ _ _ _ _ _ _ _ _ ]
             *
             * After: inserted [ordinal=0, psl=0] at the empty slot.
             * [ 0 _ _ _ _ _ _ _ ]
             */
            h.add(0);
            assertEquals(encodeValue(0, 0, 0), h.getTable().get(0));

            /*
             * Add key=8, hash=8, home_slot=0
             *
             * Before: occupied slot.
             *   ▼
             * [ 0 _ _ _ _ _ _ _ ]
             *
             * After: inserted [ordinal=1, psl=0] at the existing slot, displaced [ordinal=0, psl=0],
             *        and re-inserted it at the next empty slot as [ordinal=0, psl=1].
             * [ 1 0 _ _ _ _ _ _ ]
             */
            h.add(8);
            assertEquals(encodeValue(0, 0, 1), h.getTable().get(0));
            assertEquals(encodeValue(1, 0, 0), h.getTable().get(1));

            /*
             * Add key=1, hash=1, home_slot=1
             *
             * Before: occupied slot.
             *     ▼
             * [ 1 0 _ _ _ _ _ _ ]
             *
             * After: inserted [ordinal=2, psl=0] at the existing slot, displaced [ordinal=0, psl=1],
             *        and re-inserted it at the next empty slot as [ordinal=0, psl=2].
             * [ 1 2 0 _ _ _ _ _ ]
             */
            h.add(1);
            assertEquals(encodeValue(0, 0, 1), h.getTable().get(0));
            assertEquals(encodeValue(0, 0, 2), h.getTable().get(1));
            assertEquals(encodeValue(2, 0, 0), h.getTable().get(2));

            /*
             * Add key=16, hash=16, home_slot=0
             *
             * Before: occupied slot.
             *   ▼
             * [ 1 2 0 _ _ _ _ _ ]
             *
             * After: inserted [ordinal=3, psl=0] at the existing slot, displaced [ordinal=1, psl=0]
             *        and re-inserted it at the next best slot. Repeated this for other displaced values
             *        until everything found an empty slot.
             * [ 3 1 0 2 _ _ _ _ ]
             */
            h.add(16);
            assertEquals(encodeValue(0, 0, 3), h.getTable().get(0));
            assertEquals(encodeValue(1, 0, 1), h.getTable().get(1));
            assertEquals(encodeValue(2, 0, 0), h.getTable().get(2));
            assertEquals(encodeValue(2, 0, 2), h.getTable().get(3));
        }
    }

    private static long encodeValue(long psl, long fingerprint, long ordinal) {
        assert psl < (1L << 15);
        assert fingerprint < (1L << 16);
        assert ordinal < (1L << 32);
        return (psl << 48) | (fingerprint << 32) | ordinal;
    }
}
