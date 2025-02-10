/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.List;

/**
 * A dummy version of CacheStatsHolder, which cache implementations use when FeatureFlags.PLUGGABLE_CACHES is false.
 * Returns all-zero stats when calling getImmutableCacheStatsHolder(). Always returns 0 for count().
 * A singleton instance is used for memory purposes.
 */
public class NoopCacheStatsHolder implements CacheStatsHolder {
    private static final String dummyStoreName = "noop_store";
    private static final NoopCacheStatsHolder singletonInstance = new NoopCacheStatsHolder();
    private static final ImmutableCacheStatsHolder immutableCacheStatsHolder;
    static {
        DefaultCacheStatsHolder.Node dummyNode = new DefaultCacheStatsHolder.Node("", false);
        immutableCacheStatsHolder = new ImmutableCacheStatsHolder(dummyNode, new String[0], List.of(), dummyStoreName);
    }

    private NoopCacheStatsHolder() {}

    public static NoopCacheStatsHolder getInstance() {
        return singletonInstance;
    }

    @Override
    public void incrementHits(List<String> dimensionValues) {}

    @Override
    public void incrementMisses(List<String> dimensionValues) {}

    @Override
    public void incrementEvictions(List<String> dimensionValues) {}

    @Override
    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {}

    @Override
    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {}

    @Override
    public void incrementItems(List<String> dimensionValues) {}

    @Override
    public void decrementItems(List<String> dimensionValues) {}

    @Override
    public void reset() {}

    @Override
    public long count() {
        return 0;
    }

    @Override
    public void removeDimensions(List<String> dimensionValues) {}

    @Override
    public ImmutableCacheStatsHolder getImmutableCacheStatsHolder(String[] levels) {
        return immutableCacheStatsHolder;
    }
}
