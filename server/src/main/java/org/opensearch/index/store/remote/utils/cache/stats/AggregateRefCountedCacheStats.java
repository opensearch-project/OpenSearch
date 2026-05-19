/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache.stats;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.store.remote.utils.cache.RefCountedCache;

import java.util.Objects;

/**
 * Statistics about the Cumulative performance of a {@link RefCountedCache}.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public final class AggregateRefCountedCacheStats implements IRefCountedCacheStats {

    private final RefCountedCacheStats overallCacheStats;
    private final RefCountedCacheStats fullFileCacheStats;
    private final RefCountedCacheStats blockFileCacheStats;
    private final RefCountedCacheStats pinnedFileCacheStats;

    /**
     * Constructs a new {@code AggregateRefCountedCacheStats} instance.
     * <p>
     * Many parameters of the same type in a row is a bad thing, but this class is not constructed
     * by end users and is too fine-grained for a builder.
     *
    */
    public AggregateRefCountedCacheStats(
        RefCountedCacheStats overallCacheStats,
        RefCountedCacheStats fullFileCacheStats,
        RefCountedCacheStats blockFileCacheStats,
        RefCountedCacheStats pinnedFileCacheStats
    ) {
        this.overallCacheStats = overallCacheStats;
        this.fullFileCacheStats = fullFileCacheStats;
        this.blockFileCacheStats = blockFileCacheStats;
        this.pinnedFileCacheStats = pinnedFileCacheStats;
    }

    /**
     * Getter for OverallCacheStats.
     * @return {@link RefCountedCacheStats} overallCacheStats.
     */
    public RefCountedCacheStats getOverallCacheStats() {
        return overallCacheStats;
    }

    /**
     * Getter for blockFileCacheStats.
     * @return {@link RefCountedCacheStats} blockFileCacheStats.
     */
    public RefCountedCacheStats getBlockFileCacheStats() {
        return blockFileCacheStats;
    }

    /**
     * Getter for pinnedFileCacheStats.
     * @return {@link RefCountedCacheStats} pinnedFileCacheStats.
     */
    public RefCountedCacheStats getPinnedFileCacheStats() {
        return pinnedFileCacheStats;
    }

    /**
     * Getter for fullFileCacheStats.
     * @return {@link RefCountedCacheStats} fullFileCacheStats.
     */
    public RefCountedCacheStats getFullFileCacheStats() {
        return fullFileCacheStats;
    }

    /**
     * Returns the number of times {@link RefCountedCache} lookup methods have returned either a cached or
     * uncached value. This is defined as {@code hitCount + missCount}.
     *
     * @return the {@code hitCount + missCount}
     */
    public long requestCount() {
        return this.overallCacheStats.requestCount();
    }

    /**
     * Returns the number of times {@link RefCountedCache} lookup methods have returned a cached value.
     *
     * @return the number of times {@link RefCountedCache} lookup methods have returned a cached value
     */
    public long hitCount() {
        return this.overallCacheStats.hitCount();
    }

    /**
     * Returns the ratio of cache requests which were hits. This is defined as
     * {@code hitCount / requestCount}, or {@code 1.0} when {@code requestCount == 0}. Note that
     * {@code hitRate + missRate =~ 1.0}.
     *
     * @return the ratio of cache requests which were hits
     */
    public double hitRate() {
        long requestCount = requestCount();
        return (requestCount == 0) ? 1.0 : (double) hitCount() / requestCount;
    }

    /**
     * Returns the number of times {@link RefCountedCache} lookup methods have returned an uncached (newly
     * loaded) value, or null. Multiple concurrent calls to {@link RefCountedCache} lookup methods on an absent
     * value can result in multiple misses, all returning the results of a single cache load
     * operation.
     *
     * @return the number of times {@link RefCountedCache} lookup methods have returned an uncached (newly
     * loaded) value, or null
     */
    public long missCount() {
        return this.overallCacheStats.missCount();
    }

    /**
     * Returns the ratio of cache requests which were misses. This is defined as
     * {@code missCount / requestCount}, or {@code 0.0} when {@code requestCount == 0}.
     * Note that {@code hitRate + missRate =~ 1.0}. Cache misses include all requests which
     * weren't cache hits, including requests which resulted in either successful or failed loading
     * attempts, and requests which waited for other threads to finish loading. It is thus the case
     * that {@code missCount &gt;= loadSuccessCount + loadFailureCount}. Multiple
     * concurrent misses for the same key will result in a single load operation.
     *
     * @return the ratio of cache requests which were misses
     */
    public double missRate() {
        long requestCount = requestCount();
        return (requestCount == 0) ? 0.0 : (double) missCount() / requestCount;
    }

    /**
     * Returns the number of times an entry has been removed explicitly.
     *
     * @return the number of times an entry has been removed
     */
    public long removeCount() {
        return this.overallCacheStats.removeCount();
    }

    /**
     * Returns the sum of weights of explicitly removed entries.
     *
     * @return the sum of weights of explicitly removed entries
     */
    public long removeWeight() {
        return this.overallCacheStats.removeWeight();
    }

    /**
     * Returns the number of times an entry has been replaced.
     *
     * @return the number of times an entry has been replaced
     */
    public long replaceCount() {
        return this.overallCacheStats.replaceCount();
    }

    /**
     * Returns the number of times an entry has been evicted. This count does not include manual
     * {@linkplain RefCountedCache#remove removals}.
     *
     * @return the number of times an entry has been evicted
     */
    public long evictionCount() {
        return this.overallCacheStats.evictionCount();
    }

    /**
     * Returns the sum of weights of evicted entries. This total does not include manual
     * {@linkplain RefCountedCache#remove removals}.
     *
     * @return the sum of weights of evicted entities
     */
    public long evictionWeight() {
        return this.overallCacheStats.evictionWeight();
    }

    /**
     * Returns the total weight of the cache.
     *
     * @return the total weight of the cache
     */
    public long usage() {
        return this.overallCacheStats.usage();
    }

    /**
     * Returns the total active weight of the cache.
     *
     * @return the total active weight of the cache
     */
    public long activeUsage() {
        return this.overallCacheStats.activeUsage();
    }

    /**
     * Returns the total pinned weight of the cache.
     *
     * @return the total pinned weight of the cache
     */
    @Override
    public long pinnedUsage() {
        return this.pinnedFileCacheStats.pinnedUsage();
    }

    /**
     * Accumulates the values of another {@link IRefCountedCacheStats} into this one.
     *
     * @param other another {@link IRefCountedCacheStats}
     * @return result of accumulation of the other {@link IRefCountedCacheStats} into this one.
     */
    @Override
    public IRefCountedCacheStats accumulate(IRefCountedCacheStats other) {

        if (other instanceof AggregateRefCountedCacheStats == false) {
            throw new IllegalArgumentException("Invalid Argument passed for Accumulating AggregateRefCountedCacheStats");
        }

        final AggregateRefCountedCacheStats otherStats = (AggregateRefCountedCacheStats) other;

        this.overallCacheStats.accumulate(otherStats.overallCacheStats);
        this.fullFileCacheStats.accumulate(otherStats.fullFileCacheStats);
        this.blockFileCacheStats.accumulate(otherStats.blockFileCacheStats);
        this.pinnedFileCacheStats.accumulate(otherStats.pinnedFileCacheStats);

        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(overallCacheStats, fullFileCacheStats, blockFileCacheStats);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof AggregateRefCountedCacheStats)) {
            return false;
        }
        AggregateRefCountedCacheStats other = (AggregateRefCountedCacheStats) o;
        return overallCacheStats.equals(other.overallCacheStats)
            && fullFileCacheStats.equals(other.fullFileCacheStats)
            && blockFileCacheStats.equals(other.blockFileCacheStats)
            && pinnedFileCacheStats.equals(other.pinnedFileCacheStats);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + '{'
            + "overallRefCountedCacheStats="
            + overallCacheStats.toString()
            + ", "
            + "fullRefCountedCacheStats="
            + fullFileCacheStats.toString()
            + ", "
            + "blockRefCountedCacheStats="
            + blockFileCacheStats.toString()
            + ", "
            + "pinnedRefCountedCacheStats="
            + pinnedFileCacheStats.toString()
            + '}';
    }
}
