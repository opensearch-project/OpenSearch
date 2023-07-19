/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import net.openhft.hashing.LongHashFunction;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ReorganizingBytesRefHashTests extends OpenSearchTestCase {

    public void testFuzzy() {
        LongHashFunction hasher = LongHashFunction.xx3(randomLong());
        Map<BytesRef, Long> reference = new HashMap<>();
        BytesRef[] keys = Stream.generate(() -> new BytesRef(randomAlphaOfLength(20))).limit(1000).toArray(BytesRef[]::new);

        try (
            ReorganizingBytesRefHash h = new ReorganizingBytesRefHash(
                randomIntBetween(1, 100),      // random capacity
                0.6f + randomFloat() * 0.39f,  // random load factor to verify collision resolution
                key -> hasher.hashBytes(key.bytes, key.offset, key.length),
                BigArrays.NON_RECYCLING_INSTANCE
            )
        ) {
            // Verify the behaviour of "add" and "find".
            for (int i = 0; i < keys.length * 10; i++) {
                BytesRef key = keys[i % keys.length];
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
            BytesRef scratch = new BytesRef();
            for (Map.Entry<BytesRef, Long> entry : reference.entrySet()) {
                assertEquals(entry.getKey(), h.get(entry.getValue(), scratch));
            }

            // Verify the behaviour of "size".
            assertEquals(reference.size(), h.size());

            // Verify the calculation of PSLs.
            long capacity = h.getTable().size();
            long mask = capacity - 1;
            for (long idx = 0; idx < h.getTable().size(); idx++) {
                long value = h.getTable().get(idx);
                if (value != -1) {
                    BytesRef key = h.get((int) value, scratch);
                    long homeIdx = hasher.hashBytes(key.bytes, key.offset, key.length) & mask;
                    assertEquals((capacity + idx - homeIdx) & mask, value >>> 48);
                }
            }
        }
    }
}
