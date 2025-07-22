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

/**
 * Performs binary search on an arbitrary data structure.
 * <p>
 * To do a search, create a subclass and implement custom {@link #compare(int)} and {@link #distance(int)} methods.
 * <p>
 * {@link BinarySearcher} knows nothing about the value being searched for or the underlying data structure.
 * These things should be determined by the subclass in its overridden methods.
 * <p>
 * Refer to {@link BigArrays.DoubleBinarySearcher} for an example.
 * <p>
 * NOTE: this class is not thread safe
 *
 * @opensearch.internal
 */
public abstract class BinarySearcher {

    /**
     * @return a negative integer, zero, or a positive integer if the array's value at <code>index</code> is less than,
     * equal to, or greater than the value being searched for.
     */
    protected abstract int compare(int index);

    /**
     * @return the magnitude of the distance between the element at <code>index</code> and the value being searched for.
     * It will usually be <code>Math.abs(array[index] - searchValue)</code>.
     */
    protected abstract double distance(int index);

    /**
     * @return the index who's underlying value is closest to the value being searched for.
     */
    private int getClosestIndex(int index1, int index2) {
        if (distance(index1) < distance(index2)) {
            return index1;
        } else {
            return index2;
        }
    }

    /**
     * Uses a binary search to determine the index of the element within the index range {from, ... , to} that is
     * closest to the search value.
     * <p>
     * Unlike most binary search implementations, the value being searched for is not an argument to search method.
     * Rather, this value should be stored by the subclass along with the underlying array.
     *
     * @return the index of the closest element.
     *
     * Requires: The underlying array should be sorted.
     **/
    public int search(int from, int to) {
        while (from < to) {
            int mid = (from + to) >>> 1;
            int compareResult = compare(mid);

            if (compareResult == 0) {
                // arr[mid] == value
                return mid;
            } else if (compareResult < 0) {
                // arr[mid] < val

                if (mid < to) {
                    // Check if val is between (mid, mid + 1) before setting left = mid + 1
                    // (mid < to) ensures that mid + 1 is not out of bounds
                    int compareValAfterMid = compare(mid + 1);
                    if (compareValAfterMid > 0) {
                        return getClosestIndex(mid, mid + 1);
                    }
                } else if (mid == to) {
                    // val > arr[mid] and there are no more elements above mid, so mid is the closest
                    return mid;
                }

                from = mid + 1;
            } else {
                // arr[mid] > val

                if (mid > from) {
                    // Check if val is between (mid - 1, mid)
                    // (mid > from) ensures that mid - 1 is not out of bounds
                    int compareValBeforeMid = compare(mid - 1);
                    if (compareValBeforeMid < 0) {
                        // val is between indices (mid - 1), mid
                        return getClosestIndex(mid, mid - 1);
                    }
                } else if (mid == 0) {
                    // val < arr[mid] and there are no more candidates below mid, so mid is the closest
                    return mid;
                }

                to = mid - 1;
            }
        }

        return from;
    }

}
