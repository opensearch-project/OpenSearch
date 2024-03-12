/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cache.common.policy;

import org.opensearch.common.unit.TimeValue;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A cache tier policy which accepts queries whose took time is greater than some threshold.
 * The threshold should be set to approximately
 * the time it takes to get a result from the cache tier.
 * The policy accepts values of type V and decodes them into CachePolicyInfoWrapper, which has the data needed
 * to decide whether to admit the value.
 * @param <V> The type of data consumed by test().
 */
public class TookTimePolicy<V> implements Predicate<V> {
    /**
     * The minimum took time to allow a query. Set to TimeValue.ZERO to let all data through.
     */
    private final TimeValue threshold;

    /**
     * Function which extracts took time in nanoseconds from a serialized CachedQueryResult
     */
    private final Function<V, Long> cachedResultParser; //

    /**
     * Constructs a took time policy.
     * @param threshold the threshold
     * @param cachedResultParser the function providing took time
     */
    public TookTimePolicy(TimeValue threshold, Function<V, Long> cachedResultParser) {
        this.threshold = threshold;
        this.cachedResultParser = cachedResultParser;
    }

    /**
     * Check whether to admit data.
     * @param data the input argument
     * @return whether to admit the data
     */
    public boolean test(V data) {
        Long tookTimeNanos;
        try {
            tookTimeNanos = cachedResultParser.apply(data);
        } catch (Exception e) {
            // If we can't read a CachePolicyInfoWrapper from the BytesReference, reject the data
            return false;
        }

        if (tookTimeNanos == null) {
            // If the wrapper contains null took time, reject the data
            // This can happen if no CachePolicyInfoWrapper was written to the BytesReference, as the wrapper's constructor
            // reads an optional long, which will end up as null in this case. This is why we should reject it.
            return false;
        }
        TimeValue tookTime = TimeValue.timeValueNanos(tookTimeNanos);
        if (tookTime.compareTo(threshold) < 0) { // negative -> tookTime is shorter than threshold
            return false;
        }
        return true;
    }
}
