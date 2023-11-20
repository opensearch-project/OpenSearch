/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import org.opensearch.common.annotation.InternalApi;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

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

    /**
     * The preferred LongVector species with the maximal bit-size supported on this platform.
     */
    private static final VectorSpecies<Long> LONG_VECTOR_SPECIES = LongVector.SPECIES_PREFERRED;

    /**
     * Indicates whether the vectorized (SIMD) B-tree search implementation is supported.
     * This is true when the platform has a minimum of 4 long vector lanes and the feature flag is enabled.
     */
    private static final boolean IS_BTREE_SEARCH_SUPPORTED = LONG_VECTOR_SPECIES.length() >= 4
        && "true".equalsIgnoreCase(System.getProperty("opensearch.experimental.feature.simd.rounding.enabled"));

    private RoundableFactory() {}

    /**
     * Creates and returns the fastest implementation of {@link Roundable}.
     */
    public static Roundable create(long[] values, int size) {
        if (size <= LINEAR_SEARCH_MAX_SIZE) {
            return new BidirectionalLinearSearcher(values, size);
        } else if (IS_BTREE_SEARCH_SUPPORTED) {
            return new BtreeSearcher(values, size, LONG_VECTOR_SPECIES);
        } else {
            return new BinarySearcher(values, size);
        }
    }
}
