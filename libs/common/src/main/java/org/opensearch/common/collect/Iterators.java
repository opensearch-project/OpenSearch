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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterators utility class.
 *
 * @opensearch.internal
 */
public class Iterators {

    /**
     * Concat iterators
     *
     * @param iterators the iterators to concat
     * @param <T> the type of iterator
     * @return a new {@link ConcatenatedIterator}
     * @throws NullPointerException if iterators is null
     */
    public static <T> Iterator<T> concat(Iterator<? extends T>... iterators) {
        if (iterators == null) {
            throw new NullPointerException("iterators");
        }

        // explicit generic type argument needed for type inference
        return new ConcatenatedIterator<T>(iterators);
    }

    /**
     * Concat iterators
     *
     * @opensearch.internal
     */
    static class ConcatenatedIterator<T> implements Iterator<T> {
        private final Iterator<? extends T>[] iterators;
        private int index = 0;

        ConcatenatedIterator(Iterator<? extends T>... iterators) {
            if (iterators == null) {
                throw new NullPointerException("iterators");
            }
            for (int i = 0; i < iterators.length; i++) {
                if (iterators[i] == null) {
                    throw new NullPointerException("iterators[" + i + "]");
                }
            }
            this.iterators = iterators;
        }

        /**
         * Returns {@code true} if the iteration has more elements. (In other words, returns {@code true} if {@link #next} would return an
         * element rather than throwing an exception.)
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            boolean hasNext = false;
            while (index < iterators.length && !(hasNext = iterators[index].hasNext())) {
                index++;
            }

            return hasNext;
        }

        /**
         * Returns the next element in the iteration.
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iterators[index].next();
        }
    }
}
