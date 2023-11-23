/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.util;

import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.common.hash.T1ha1;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

public class BytesRefHashTests extends OpenSearchTestCase {

    BytesRefHash hash;

    private BigArrays randomBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private void newHash() {
        if (hash != null) {
            hash.close();
        }
        long seed = randomLong();
        hash = new BytesRefHash(
            randomIntBetween(1, 100),      // random capacity
            0.6f + randomFloat() * 0.39f,  // random load factor to verify collision resolution
            key -> T1ha1.hash(key.bytes, key.offset, key.length, seed),
            randomBigArrays()
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        newHash();
    }

    public void testFuzzy() {
        Map<BytesRef, Long> reference = new HashMap<>();
        BytesRef[] keys = Stream.generate(() -> new BytesRef(randomAlphaOfLength(20)))
            .limit(randomIntBetween(1000, 2000))
            .toArray(BytesRef[]::new);

        // Verify the behaviour of "add" and "find".
        for (int i = 0; i < keys.length * 10; i++) {
            BytesRef key = keys[i % keys.length];
            if (reference.containsKey(key)) {
                long expectedOrdinal = reference.get(key);
                assertEquals(-1 - expectedOrdinal, hash.add(key));
                assertEquals(expectedOrdinal, hash.find(key));
            } else {
                assertEquals(-1, hash.find(key));
                reference.put(key, (long) reference.size());
                assertEquals((long) reference.get(key), hash.add(key));
            }
        }

        // Verify the behaviour of "get".
        BytesRef scratch = new BytesRef();
        for (Map.Entry<BytesRef, Long> entry : reference.entrySet()) {
            assertEquals(entry.getKey(), hash.get(entry.getValue(), scratch));
        }

        // Verify the behaviour of "size".
        assertEquals(reference.size(), hash.size());
        hash.close();
    }

    // START - tests borrowed from LUCENE

    /**
     * Test method for {@link org.apache.lucene.util.BytesRefHash#size()}.
     */
    public void testSize() {
        BytesRefBuilder ref = new BytesRefBuilder();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            final int mod = 1 + randomInt(40);
            for (int i = 0; i < 797; i++) {
                String str;
                do {
                    str = TestUtil.randomRealisticUnicodeString(random(), 1000);
                } while (str.length() == 0);
                ref.copyChars(str);
                long count = hash.size();
                long key = hash.add(ref.get());
                if (key < 0) assertEquals(hash.size(), count);
                else assertEquals(hash.size(), count + 1);
                if (i % mod == 0) {
                    newHash();
                }
            }
        }
        hash.close();
    }

    /**
     * Test method for
     * {@link org.apache.lucene.util.BytesRefHash#get(int, BytesRef)}
     * .
     */
    public void testGet() {
        BytesRefBuilder ref = new BytesRefBuilder();
        BytesRef scratch = new BytesRef();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Map<String, Long> strings = new HashMap<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                String str;
                do {
                    str = TestUtil.randomRealisticUnicodeString(random(), 1000);
                } while (str.length() == 0);
                ref.copyChars(str);
                long count = hash.size();
                long key = hash.add(ref.get());
                if (key >= 0) {
                    assertNull(strings.put(str, key));
                    assertEquals(uniqueCount, key);
                    uniqueCount++;
                    assertEquals(hash.size(), count + 1);
                } else {
                    assertTrue((-key) - 1 < count);
                    assertEquals(hash.size(), count);
                }
            }
            for (Entry<String, Long> entry : strings.entrySet()) {
                ref.copyChars(entry.getKey());
                assertEquals(ref.get(), hash.get(entry.getValue(), scratch));
            }
            newHash();
        }
        hash.close();
    }

    /**
     * Test method for
     * {@link org.apache.lucene.util.BytesRefHash#add(org.apache.lucene.util.BytesRef)}
     * .
     */
    public void testAdd() {
        BytesRefBuilder ref = new BytesRefBuilder();
        BytesRef scratch = new BytesRef();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Set<String> strings = new HashSet<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                String str;
                do {
                    str = TestUtil.randomRealisticUnicodeString(random(), 1000);
                } while (str.length() == 0);
                ref.copyChars(str);
                long count = hash.size();
                long key = hash.add(ref.get());

                if (key >= 0) {
                    assertTrue(strings.add(str));
                    assertEquals(uniqueCount, key);
                    assertEquals(hash.size(), count + 1);
                    uniqueCount++;
                } else {
                    assertFalse(strings.add(str));
                    assertTrue((-key) - 1 < count);
                    assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
                    assertEquals(count, hash.size());
                }
            }

            assertAllIn(strings, hash);
            newHash();
        }
        hash.close();
    }

    public void testFind() {
        BytesRefBuilder ref = new BytesRefBuilder();
        BytesRef scratch = new BytesRef();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Set<String> strings = new HashSet<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                String str;
                do {
                    str = TestUtil.randomRealisticUnicodeString(random(), 1000);
                } while (str.length() == 0);
                ref.copyChars(str);
                long count = hash.size();
                long key = hash.find(ref.get()); // hash.add(ref);
                if (key >= 0) { // string found in hash
                    assertFalse(strings.add(str));
                    assertTrue(key < count);
                    assertEquals(str, hash.get(key, scratch).utf8ToString());
                    assertEquals(count, hash.size());
                } else {
                    key = hash.add(ref.get());
                    assertTrue(strings.add(str));
                    assertEquals(uniqueCount, key);
                    assertEquals(hash.size(), count + 1);
                    uniqueCount++;
                }
            }

            assertAllIn(strings, hash);
            newHash();
        }
        hash.close();
    }

    private void assertAllIn(Set<String> strings, BytesRefHash hash) {
        BytesRefBuilder ref = new BytesRefBuilder();
        BytesRef scratch = new BytesRef();
        long count = hash.size();
        for (String string : strings) {
            ref.copyChars(string);
            long key = hash.add(ref.get()); // add again to check duplicates
            assertEquals(string, hash.get((-key) - 1, scratch).utf8ToString());
            assertEquals(count, hash.size());
            assertTrue("key: " + key + " count: " + count + " string: " + string, key < count);
        }
    }

    // END - tests borrowed from LUCENE

}
