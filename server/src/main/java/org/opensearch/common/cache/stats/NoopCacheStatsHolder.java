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
 */
public class NoopCacheStatsHolder implements CacheStatsHolder {

    public NoopCacheStatsHolder() {}

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
    public void incrementEntries(List<String> dimensionValues) {}

    @Override
    public void decrementEntries(List<String> dimensionValues) {}

    @Override
    public void reset() {}

    @Override
    public long count() {
        return 0;
    }

    @Override
    public void removeDimensions(List<String> dimensionValues) {}

    @Override
    public ImmutableCacheStatsHolder getImmutableCacheStatsHolder() {
        ImmutableCacheStatsHolder.Node dummyNode = new ImmutableCacheStatsHolder.Node("", null, new ImmutableCacheStats(0, 0, 0, 0, 0));
        return new ImmutableCacheStatsHolder(dummyNode, List.of());
    }
}
