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
 * Factory class to create and return the fastest implementation of {@link Roundable}.
 *
 * @opensearch.internal
 */
@InternalApi
public final class RoundableFactory {
    /**
     * The maximum limit up to which linear search is used, otherwise binary search is used.
     * This is because linear search is much faster on small arrays.
     * Benchmark results: <a href="https://github.com/opensearch-project/OpenSearch/pull/9727">PR #9727</a>
     */
    private static final int LINEAR_SEARCH_MAX_SIZE = 64;

    private RoundableFactory() {}

    /**
     * Creates and returns the fastest implementation of {@link Roundable}.
     */
    public static Roundable create(long[] values, int size) {
        if (size <= LINEAR_SEARCH_MAX_SIZE) {
            return new BidirectionalLinearSearcher(values, size);
        } else {
            return new BinarySearcher(values, size);
        }
    }
}
