/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import org.opensearch.common.annotation.InternalApi;

/**
 * It uses linear search on a sorted array of pre-computed round-down points.
 * For small inputs (&le; 64 elements), this can be much faster than binary search as it avoids the penalty of
 * branch mispredictions and pipeline stalls, and accesses memory sequentially.
 *
 * <p>
 * It uses "meet in the middle" linear search to avoid the worst case scenario when the desired element is present
 * at either side of the array. This is helpful for time-series data where velocity increases over time, so more
 * documents are likely to find a greater timestamp which is likely to be present on the right end of the array.
 *
 * @opensearch.internal
 */
@InternalApi
class BidirectionalLinearSearcher implements Roundable {
    private final long[] ascending;
    private final long[] descending;

    BidirectionalLinearSearcher(long[] values, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("at least one value must be present");
        }

        int len = (size + 1) >>> 1; // rounded-up to handle odd number of values
        ascending = new long[len];
        descending = new long[len];

        for (int i = 0; i < len; i++) {
            ascending[i] = values[i];
            descending[i] = values[size - i - 1];
        }
    }

    @Override
    public long floor(long key) {
        int i = 0;
        for (; i < ascending.length; i++) {
            if (descending[i] <= key) {
                return descending[i];
            }
            if (ascending[i] > key) {
                assert i > 0 : "key must be greater than or equal to " + ascending[0];
                return ascending[i - 1];
            }
        }
        return ascending[i - 1];
    }
}
