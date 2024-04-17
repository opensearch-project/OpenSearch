/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.metrics.CounterMetric;

import java.util.List;

/**
 * A dummy version of CacheStatsHolder, which cache implementations use when FeatureFlags.PLUGGABLE_CACHES is false.
 * Will be removed once pluggable caches is no longer an experimental feature.
 * Returns all-zero stats when calling getImmutableCacheStatsHolder(). Does keep track of entries for use in ICache.count().
 */
public class DummyCacheStatsHolder implements CacheStatsHolderInterface {
    private CounterMetric entries;
    private CounterMetric sizeInBytes;
    private final List<String> dimensionNames;

    public DummyCacheStatsHolder(List<String> dimensionNames) {
        this.dimensionNames = dimensionNames;
        this.entries = new CounterMetric();
        this.sizeInBytes = new CounterMetric();
    }

    @Override
    public void incrementHits(List<String> dimensionValues) {}

    @Override
    public void incrementMisses(List<String> dimensionValues) {}

    @Override
    public void incrementEvictions(List<String> dimensionValues) {}

    @Override
    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        sizeInBytes.inc(amountBytes);
    }

    @Override
    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        sizeInBytes.dec(amountBytes);
    }

    @Override
    public void incrementEntries(List<String> dimensionValues) {
        entries.inc();
    }

    @Override
    public void decrementEntries(List<String> dimensionValues) {
        entries.dec();
    }

    @Override
    public void reset() {
        this.entries = new CounterMetric();
        this.sizeInBytes = new CounterMetric();
    }

    @Override
    public long count() {
        return entries.count();
    }

    @Override
    public void removeDimensions(List<String> dimensionValues) {}

    @Override
    public ImmutableCacheStatsHolder getImmutableCacheStatsHolder() {
        ImmutableCacheStatsHolder.Node dummyNode = new ImmutableCacheStatsHolder.Node("", null, new ImmutableCacheStats(0, 0, 0, 0, 0));
        return new ImmutableCacheStatsHolder(dummyNode, dimensionNames);
    }
}
