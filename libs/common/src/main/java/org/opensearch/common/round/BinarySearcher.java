/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import org.opensearch.common.annotation.InternalApi;

import java.util.Arrays;

/**
 * It uses binary search on a sorted array of pre-computed round-down points.
 *
 * @opensearch.internal
 */
@InternalApi
class BinarySearcher implements Roundable {
    private final long[] values;
    private final int size;

    BinarySearcher(long[] values, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("at least one value must be present");
        }

        this.values = values;
        this.size = size;
    }

    @Override
    public long floor(long key) {
        int idx = Arrays.binarySearch(values, 0, size, key);
        assert idx != -1 : "key must be greater than or equal to " + values[0];
        if (idx < 0) {
            idx = -2 - idx;
        }
        return values[idx];
    }
}
