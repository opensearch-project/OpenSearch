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

package org.opensearch.common.collect;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IteratorsTests extends OpenSearchTestCase {
    public void testConcatentation() {
        List<Integer> threeTwoOne = Arrays.asList(3, 2, 1);
        List<Integer> fourFiveSix = Arrays.asList(4, 5, 6);
        Iterator<Integer> concat = Iterators.concat(threeTwoOne.iterator(), fourFiveSix.iterator());
        assertContainsInOrder(concat, 3, 2, 1, 4, 5, 6);
    }

    public void testNoConcatenation() {
        Iterator<Integer> iterator = Iterators.<Integer>concat();
        assertEmptyIterator(iterator);
    }

    public void testEmptyConcatenation() {
        Iterator<Integer> iterator = Iterators.<Integer>concat(empty());
        assertEmptyIterator(iterator);
    }

    public void testMultipleEmptyConcatenation() {
        Iterator<Integer> iterator = Iterators.concat(empty(), empty());
        assertEmptyIterator(iterator);
    }

    public void testSingleton() {
        int value = randomInt();
        assertSingleton(value, singletonIterator(value));
    }

    public void testEmptyBeforeSingleton() {
        int value = randomInt();
        assertSingleton(value, empty(), singletonIterator(value));
    }

    public void testEmptyAfterSingleton() {
        int value = randomInt();
        assertSingleton(value, singletonIterator(value), empty());
    }

    public void testRandomSingleton() {
        int numberOfIterators = randomIntBetween(1, 1000);
        int singletonIndex = randomIntBetween(0, numberOfIterators - 1);
        int value = randomInt();
        @SuppressWarnings("rawtypes")
        Iterator<Integer>[] iterators = new Iterator[numberOfIterators];
        for (int i = 0; i < numberOfIterators; i++) {
            iterators[i] = i != singletonIndex ? empty() : singletonIterator(value);
        }
        assertSingleton(value, iterators);
    }

    public void testRandomIterators() {
        int numberOfIterators = randomIntBetween(1, 1000);
        @SuppressWarnings("rawtypes")
        Iterator<Integer>[] iterators = new Iterator[numberOfIterators];
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < numberOfIterators; i++) {
            int numberOfValues = randomIntBetween(0, 256);
            List<Integer> theseValues = new ArrayList<>();
            for (int j = 0; j < numberOfValues; j++) {
                int value = randomInt();
                values.add(value);
                theseValues.add(value);
            }
            iterators[i] = theseValues.iterator();
        }
        assertContainsInOrder(Iterators.concat(iterators), values.toArray(new Integer[0]));
    }

    public void testTwoEntries() {
        int first = randomInt();
        int second = randomInt();
        Iterator<Integer> concat = Iterators.concat(singletonIterator(first), empty(), empty(), singletonIterator(second));
        assertContainsInOrder(concat, first, second);
    }

    public void testNull() {
        try {
            Iterators.concat((Iterator<?>) null);
            fail("expected " + NullPointerException.class.getSimpleName());
        } catch (NullPointerException e) {

        }
    }

    public void testNullIterator() {
        try {
            Iterators.concat(singletonIterator(1), empty(), null, empty(), singletonIterator(2));
            fail("expected " + NullPointerException.class.getSimpleName());
        } catch (NullPointerException e) {

        }
    }

    private <T> Iterator<T> singletonIterator(T value) {
        return Collections.singleton(value).iterator();
    }

    private <T> void assertSingleton(T value, Iterator<T>... iterators) {
        Iterator<T> concat = Iterators.concat(iterators);
        assertContainsInOrder(concat, value);
    }

    private <T> Iterator<T> empty() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                throw new NoSuchElementException();
            }
        };
    }

    private <T> void assertContainsInOrder(Iterator<T> iterator, T... values) {
        for (T value : values) {
            assertTrue(iterator.hasNext());
            assertEquals(value, iterator.next());
        }
        assertNoSuchElementException(iterator);
    }

    private <T> void assertEmptyIterator(Iterator<T> iterator) {
        assertFalse(iterator.hasNext());
        assertNoSuchElementException(iterator);
    }

    private <T> void assertNoSuchElementException(Iterator<T> iterator) {
        try {
            iterator.next();
            fail("expected " + NoSuchElementException.class.getSimpleName());
        } catch (NoSuchElementException e) {

        }
    }
}
