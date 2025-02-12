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

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.policy.CachedQueryResult;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;

import java.util.function.Function;
import java.util.function.Predicate;

import static org.opensearch.cache.common.tier.TieredSpilloverCacheSettings.TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP;

/**
 * A cache tier policy which accepts queries whose took time is greater than some threshold.
 * The threshold should be set to approximately the time it takes to get a result from the cache tier.
 * The policy accepts values of type V and decodes them into CachedQueryResult.PolicyValues, which has the data needed
 * to decide whether to admit the value.
 * @param <V> The type of data consumed by test().
 */
public class TookTimePolicy<V> implements Predicate<V> {
    /**
     * The minimum took time to allow a query. Set to TimeValue.ZERO to let all data through.
     */
    private TimeValue threshold;

    /**
     * Function which extracts the relevant PolicyValues from a serialized CachedQueryResult
     */
    private final Function<V, CachedQueryResult.PolicyValues> cachedResultParser;

    /**
     * Constructs a took time policy.
     * @param threshold the threshold
     * @param cachedResultParser the function providing policy values
     * @param clusterSettings cluster settings
     * @param cacheType cache type
     */
    public TookTimePolicy(
        TimeValue threshold,
        Function<V, CachedQueryResult.PolicyValues> cachedResultParser,
        ClusterSettings clusterSettings,
        CacheType cacheType
    ) {
        if (threshold.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("Threshold for TookTimePolicy must be >= 0ms but was " + threshold.getStringRep());
        }
        this.threshold = threshold;
        this.cachedResultParser = cachedResultParser;
        clusterSettings.addSettingsUpdateConsumer(TOOK_TIME_POLICY_CONCRETE_SETTINGS_MAP.get(cacheType), this::setThreshold);
    }

    private void setThreshold(TimeValue threshold) {
        this.threshold = threshold;
    }

    /**
     * Check whether to admit data.
     * @param data the input argument
     * @return whether to admit the data
     */
    public boolean test(V data) {
        long tookTimeNanos;
        try {
            tookTimeNanos = cachedResultParser.apply(data).getTookTimeNanos();
        } catch (Exception e) {
            // If we can't read a CachedQueryResult.PolicyValues from the BytesReference, reject the data
            return false;
        }

        TimeValue tookTime = TimeValue.timeValueNanos(tookTimeNanos);
        return tookTime.compareTo(threshold) >= 0;
    }
}
