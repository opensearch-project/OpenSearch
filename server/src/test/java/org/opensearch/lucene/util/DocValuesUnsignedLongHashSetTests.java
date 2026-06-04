package org.opensearch.lucene.util;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.Numbers;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class DocValuesUnsignedLongHashSetTests extends LuceneTestCase {

    private void assertEquals(Set<Long> set1, UnsignedLongHashSet unsignedLongHashSet) {
        assertEquals(set1.size(), unsignedLongHashSet.size());

        Set<Long> set2 = unsignedLongHashSet.stream().boxed().collect(Collectors.toSet());
        LuceneTestCase.assertEquals(set1, set2);

        if (set1.isEmpty() == false) {
            Set<Long> set3 = new HashSet<>(set1);
            long removed = set3.iterator().next();
            while (true) {
                long next = random().nextLong();
                if (next != removed && set3.add(next)) {
                    assertFalse(unsignedLongHashSet.contains(next));
                    break;
                }
            }
            assertNotEquals(set3, unsignedLongHashSet);
        }

        assertTrue(set1.stream().allMatch(unsignedLongHashSet::contains));
    }

    private void assertNotEquals(Set<Long> set1, UnsignedLongHashSet unsignedLongHashSet) {
        Set<Long> set2 = unsignedLongHashSet.stream().boxed().collect(Collectors.toSet());

        LuceneTestCase.assertNotEquals(set1, set2);

        UnsignedLongHashSet set3 = new UnsignedLongHashSet(
            set1.stream().sorted(Long::compareUnsigned).mapToLong(Long::longValue).toArray()
        );

        LuceneTestCase.assertNotEquals(set2, set3.stream().boxed().collect(Collectors.toSet()));

        assertFalse(set1.stream().allMatch(unsignedLongHashSet::contains));
    }

    public void testEmpty() {
        Set<Long> set1 = new HashSet<>();
        UnsignedLongHashSet set2 = new UnsignedLongHashSet(new long[] {});
        assertEquals(0, set2.size());
        assertEquals(Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG, set2.minValue);
        assertEquals(Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG, set2.maxValue);
        assertEquals(set1, set2);
    }

    public void testOneValue() {
        Set<Long> set1 = new HashSet<>(Arrays.asList(42L));
        UnsignedLongHashSet set2 = new UnsignedLongHashSet(new long[] { 42L });
        assertEquals(1, set2.size());
        assertEquals(42L, set2.minValue);
        assertEquals(42L, set2.maxValue);
        assertEquals(set1, set2);

        set1 = new HashSet<>(Arrays.asList(Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG));
        set2 = new UnsignedLongHashSet(new long[] { Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG });
        assertEquals(1, set2.size());
        assertEquals(Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG, set2.minValue);
        assertEquals(Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG, set2.maxValue);
        assertEquals(set1, set2);
    }

    public void testTwoValues() {
        Set<Long> set1 = new HashSet<>(Arrays.asList(42L, Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG));
        UnsignedLongHashSet set2 = new UnsignedLongHashSet(new long[] { 42L, Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG });
        assertEquals(2, set2.size());
        assertEquals(42, set2.minValue);
        assertEquals(Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG, set2.maxValue);
        assertEquals(set1, set2);

        set1 = new HashSet<>(Arrays.asList(Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG, 42L));
        set2 = new UnsignedLongHashSet(new long[] { Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG, 42L });
        assertEquals(2, set2.size());
        assertEquals(Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG, set2.minValue);
        assertEquals(42, set2.maxValue);
        assertEquals(set1, set2);
    }

    public void testSameValue() {
        UnsignedLongHashSet set2 = new UnsignedLongHashSet(new long[] { 42L, 42L });
        assertEquals(1, set2.size());
        assertEquals(42L, set2.minValue);
        assertEquals(42L, set2.maxValue);
    }

    public void testSameMissingPlaceholder() {
        UnsignedLongHashSet set2 = new UnsignedLongHashSet(new long[] { Long.MIN_VALUE, Long.MIN_VALUE });
        assertEquals(1, set2.size());
        assertEquals(Long.MIN_VALUE, set2.minValue);
        assertEquals(Long.MIN_VALUE, set2.maxValue);
    }

    public void testRandom() {
        final int iters = atLeast(10);
        for (int iter = 0; iter < iters; ++iter) {
            long[] values = new long[random().nextInt(1 << random().nextInt(16))];
            for (int i = 0; i < values.length; ++i) {
                if (i == 0 || random().nextInt(10) < 9) {
                    values[i] = random().nextLong();
                } else {
                    values[i] = values[random().nextInt(i)];
                }
            }
            if (values.length > 0 && random().nextBoolean()) {
                values[values.length / 2] = Long.MIN_VALUE;
            }
            Set<Long> set1 = LongStream.of(values).boxed().collect(Collectors.toSet());
            Long[] longObjects = Arrays.stream(values).boxed().toArray(Long[]::new);
            // Sort using compareUnsigned
            Arrays.sort(longObjects, Long::compareUnsigned);

            long[] arr = new long[values.length];
            // Convert back to long[]
            for (int i = 0; i < arr.length; i++) {
                arr[i] = longObjects[i];
            }
            UnsignedLongHashSet set2 = new UnsignedLongHashSet(arr);
            assertEquals(set1, set2);
        }
    }
}
