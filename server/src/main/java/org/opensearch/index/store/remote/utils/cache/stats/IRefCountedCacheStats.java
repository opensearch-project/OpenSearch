/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.remote.utils.cache.RefCountedCache;

/**
 *  Statistics about the performance of a {@link RefCountedCache}.
 */
@ExperimentalApi
public interface IRefCountedCacheStats {

    /**
     * Returns the number of times {@link IRefCountedCacheStats} lookup methods have returned either a cached or
     * uncached value. This is defined as {@code hitCount + missCount}.
     *
     * @return the {@code hitCount + missCount}
     */
    public long requestCount();

    /**
     * Returns the number of times {@link IRefCountedCacheStats} lookup methods have returned a cached value.
     *
     * @return the number of times {@link IRefCountedCacheStats} lookup methods have returned a cached value
     */
    public long hitCount();

    /**
     * Returns the ratio of cache requests which were hits. This is defined as
     * {@code hitCount / requestCount}, or {@code 1.0} when {@code requestCount == 0}. Note that
     * {@code hitRate + missRate =~ 1.0}.
     *
     * @return the ratio of cache requests which were hits
     */
    public double hitRate();

    /**
     * Returns the number of times {@link IRefCountedCacheStats} lookup methods have returned an uncached (newly
     * loaded) value, or null. Multiple concurrent calls to {@link IRefCountedCacheStats} lookup methods on an absent
     * value can result in multiple misses, all returning the results of a single cache load
     * operation.
     *
     * @return the number of times {@link IRefCountedCacheStats} lookup methods have returned an uncached (newly
     * loaded) value, or null
     */
    public long missCount();

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
    public double missRate();

    /**
     * Returns the number of times an entry has been removed explicitly.
     *
     * @return the number of times an entry has been removed
     */
    public long removeCount();

    /**
     * Returns the sum of weights of explicitly removed entries.
     *
     * @return the sum of weights of explicitly removed entries
     */
    public long removeWeight();

    /**
     * Returns the number of times an entry has been replaced.
     *
     * @return the number of times an entry has been replaced
     */
    public long replaceCount();

    /**
     * Returns the number of times an entry has been evicted.
     *
     * @return the number of times an entry has been evicted
     */
    public long evictionCount();

    /**
     * Returns the sum of weights of evicted entries.
     *
     * @return the sum of weights of evicted entities
     */
    public long evictionWeight();

    /**
     * Returns the total weight of the cache.
     *
     * @return the total weight of the cache
     */
    public long usage();

    /**
     * Returns the total active weight of the cache.
     *
     * @return the total active weight of the cache
     */
    public long activeUsage();

    /**
     * Returns the total pinned weight of the cache.
     *
     * @return the total pinned weight of the cache
     */
    public long pinnedUsage();

    /**
     * Accumulates the values of another {@link IRefCountedCacheStats} into this one.
     * @param other another {@link IRefCountedCacheStats}
     * @return result of accumulation of the other {@link IRefCountedCacheStats} into this one.
     */
    public IRefCountedCacheStats accumulate(IRefCountedCacheStats other) throws IllegalArgumentException;

}
