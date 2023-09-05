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

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.opensearch.common.util.CollectionUtils.eagerPartition;
import static org.hamcrest.Matchers.containsString;

public class CollectionUtilsTests extends OpenSearchTestCase {
    public void testRotateEmpty() {
        assertTrue(CollectionUtils.rotate(Collections.emptyList(), randomInt()).isEmpty());
    }

    public void testRotate() {
        final int iters = scaledRandomIntBetween(10, 100);
        for (int k = 0; k < iters; ++k) {
            final int size = randomIntBetween(1, 100);
            final int distance = randomInt();
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                list.add(new Object());
            }
            final List<Object> rotated = CollectionUtils.rotate(list, distance);
            // check content is the same
            assertEquals(rotated.size(), list.size());
            assertEquals(rotated.size(), list.size());
            assertEquals(new HashSet<>(rotated), new HashSet<>(list));
            // check stability
            for (int j = randomInt(4); j >= 0; --j) {
                assertEquals(rotated, CollectionUtils.rotate(list, distance));
            }
            // reverse
            if (distance != Integer.MIN_VALUE) {
                assertEquals(list, CollectionUtils.rotate(CollectionUtils.rotate(list, distance), -distance));
            }
        }
    }

    private <T> void assertDeduped(List<T> array, Comparator<T> cmp, int expectedLength) {
        // test the dedup w/ ArrayLists and LinkedLists
        List<List<T>> types = List.of(new ArrayList<T>(array), new LinkedList<>(array));
        for (List<T> clone : types) {
            // dedup the list
            CollectionUtils.sortAndDedup(clone, cmp);
            // verify unique elements
            for (int i = 0; i < clone.size() - 1; ++i) {
                assertNotEquals(cmp.compare(clone.get(i), clone.get(i + 1)), 0);
            }
            assertEquals(expectedLength, clone.size());
        }
    }

    public void testSortAndDedup() {
        // test no elements in a string array
        assertDeduped(List.<String>of(), Comparator.naturalOrder(), 0);
        // test no elements in an integer array
        assertDeduped(List.<Integer>of(), Comparator.naturalOrder(), 0);
        // test unsorted array
        assertDeduped(List.of(-1, 0, 2, 1, -1, 19, -1), Comparator.naturalOrder(), 5);
        // test sorted array
        assertDeduped(List.of(-1, 0, 1, 2, 19, 19), Comparator.naturalOrder(), 5);
        // test sorted
    }

    public void testEmptyPartition() {
        assertEquals(Collections.emptyList(), eagerPartition(Collections.emptyList(), 1));
    }

    public void testSimplePartition() {
        assertEquals(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4), Arrays.asList(5)),
            eagerPartition(Arrays.asList(1, 2, 3, 4, 5), 2)
        );
    }

    public void testSingletonPartition() {
        assertEquals(
            Arrays.asList(Arrays.asList(1), Arrays.asList(2), Arrays.asList(3), Arrays.asList(4), Arrays.asList(5)),
            eagerPartition(Arrays.asList(1, 2, 3, 4, 5), 1)
        );
    }

    public void testOversizedPartition() {
        assertEquals(Arrays.asList(Arrays.asList(1, 2, 3, 4, 5)), eagerPartition(Arrays.asList(1, 2, 3, 4, 5), 15));
    }

    public void testPerfectPartition() {
        assertEquals(
            Arrays.asList(Arrays.asList(1, 2, 3, 4, 5, 6), Arrays.asList(7, 8, 9, 10, 11, 12)),
            eagerPartition(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 6)
        );
    }

    public void testEnsureNoSelfReferences() {
        CollectionUtils.ensureNoSelfReferences(emptyMap(), "test with empty map");
        CollectionUtils.ensureNoSelfReferences(null, "test with null");

        {
            Map<String, Object> map = new HashMap<>();
            map.put("field", map);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> CollectionUtils.ensureNoSelfReferences(map, "test with self ref value")
            );
            assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref value)"));
        }
        {
            Map<Object, Object> map = new HashMap<>();
            map.put(map, 1);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> CollectionUtils.ensureNoSelfReferences(map, "test with self ref key")
            );
            assertThat(e.getMessage(), containsString("Iterable object is self-referencing itself (test with self ref key)"));
        }

    }

    public void testIsEmpty() {
        assertTrue(CollectionUtils.isEmpty(new ArrayList<>()));
        final List<Integer> list = null;
        assertTrue(CollectionUtils.isEmpty(list));
        assertFalse(CollectionUtils.isEmpty(Collections.singletonList(5)));
    }
}
