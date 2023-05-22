/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.TreeMap;

public class LongRHHashTests extends OpenSearchTestCase {

    public void testFuzzy() {
        Map<Long, Long> reference = new TreeMap<>();
        try (
            LongRHHash h = new LongRHHash(
                randomIntBetween(1, 100),               // random capacity
                0.6f + randomFloat() * 0.39f,           // random load factor to verify collision resolution
                randomIntBetween(1, 256),               // random hints' size
                BigArrays.NON_RECYCLING_INSTANCE
            )
        ) {
            // Verify the behaviour of "add" and "find".
            for (int i = 0; i < (1 << 20); i++) {
                long key = randomLong() % (1 << 12);    // roughly ~4% unique keys
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
        }
    }
}
