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

package org.opensearch.common.cache.policy;

import org.opensearch.common.unit.TimeValue;

import java.util.function.Function;

/**
 * A cache tier policy which accepts queries whose took time is greater than some threshold.
 * The threshold should be set to approximately
 * the time it takes to get a result from the cache tier.
 * The policy accepts values of type V and decodes them into CachePolicyInfoWrapper, which has the data needed
 * to decide whether to admit the value.
 */
public class TookTimePolicy<V> implements CacheTierPolicy<V> {
    private TimeValue threshold; // Set this to TimeValue.ZERO to let all data through
    private final Function<V, CachePolicyInfoWrapper> getPolicyInfoFn;

    public TookTimePolicy(TimeValue threshold, Function<V, CachePolicyInfoWrapper> getPolicyInfoFn) {
        this.threshold = threshold;
        this.getPolicyInfoFn = getPolicyInfoFn;
    }

    protected void setThreshold(TimeValue threshold) { // protected so that we can manually set value in unit test
        this.threshold = threshold;
    }

    @Override
    public boolean checkData(V data) {
        Long tookTimeNanos;
        try {
            tookTimeNanos = getPolicyInfoFn.apply(data).getTookTimeNanos();
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

        if (threshold.equals(TimeValue.ZERO)) {
            // If the policy is set to zero, admit any well-formed data
            return true;
        }
        TimeValue tookTime = TimeValue.timeValueNanos(tookTimeNanos);
        if (tookTime.compareTo(threshold) < 0) { // negative -> tookTime is shorter than threshold
            return false;
        }
        return true;
    }
}
