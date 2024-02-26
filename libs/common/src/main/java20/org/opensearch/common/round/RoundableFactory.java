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
     * The maximum limit up to which linear search is used, otherwise binary or B-tree search is used.
     * This is because linear search is much faster on small arrays.
     * Benchmark results: <a href="https://github.com/opensearch-project/OpenSearch/pull/9727">PR #9727</a>
     */
    private static final int LINEAR_SEARCH_MAX_SIZE = 64;

    /**
     * Indicates whether the vectorized (SIMD) B-tree search implementation is to be used.
     * It is true when either:
     * 1. The feature flag is set to "forced", or
     * 2. The platform has a minimum of 4 long vector lanes and the feature flag is set to "true".
     */
    private static final boolean USE_BTREE_SEARCHER;

    /**
     * This class is initialized only when:
     *  - JDK-20+
     *  - jdk.incubator.vector.LongVector is available (--add-modules=jdk.incubator.vector is passed)
     */
    private static final class VectorCheck {
        final static int SPECIES_PREFERRED = jdk.incubator.vector.LongVector.SPECIES_PREFERRED.length();
    }

    static {
        String simdRoundingFeatureFlag = System.getProperty("opensearch.experimental.feature.simd.rounding.enabled");
        boolean useBtreeSearcher = false;

        try {
            final Class<?> incubator = Class.forName("jdk.incubator.vector.LongVector");

            useBtreeSearcher = "forced".equalsIgnoreCase(simdRoundingFeatureFlag)
                || (VectorCheck.SPECIES_PREFERRED >= 4 && "true".equalsIgnoreCase(simdRoundingFeatureFlag));

        } catch (final ClassNotFoundException ex) {
            /* do not use BtreeSearcher */
        }

        USE_BTREE_SEARCHER = useBtreeSearcher;
    }

    private RoundableFactory() {}

    /**
     * Creates and returns the fastest implementation of {@link Roundable}.
     */
    public static Roundable create(long[] values, int size) {
        if (size <= LINEAR_SEARCH_MAX_SIZE) {
            return new BidirectionalLinearSearcher(values, size);
        } else if (USE_BTREE_SEARCHER) {
            return new BtreeSearcher(values, size);
        } else {
            return new BinarySearcher(values, size);
        }
    }
}
