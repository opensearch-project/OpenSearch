/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.stats;

/**
 * Read-only snapshot of block cache performance statistics.
 *
 * <p>Implemented by {@link BlockCacheStats}.
 *
 * @opensearch.experimental
 */
public interface IBlockCacheStats {

    /** Number of cache hits. */
    long hitCount();

    /**
     * Bytes served from cache across all hits.
     *
     * <p>For variable-size entries, byte-level hit rate
     * ({@code hitBytes / (hitBytes + missBytes)}) is more meaningful than
     * count-based hit rate.
     */
    long hitBytes();

    /** Number of cache misses. */
    long missCount();

    /**
     * Bytes that required a remote fetch due to cache misses.
     *
     * <p>Together with {@link #hitBytes()}, gives the byte-level hit rate:
     * {@code hitBytes / (hitBytes + missBytes)}.
     */
    long missBytes();

    /** Total number of requests ({@code hitCount + missCount}). */
    default long requestCount() {
        return hitCount() + missCount();
    }

    /**
     * Hit rate: {@code hitCount / requestCount}, or {@code 1.0} when
     * {@code requestCount == 0}.
     */
    default double hitRate() {
        long total = requestCount();
        return total == 0 ? 1.0 : (double) hitCount() / total;
    }

    /**
     * Miss rate: {@code missCount / requestCount}, or {@code 0.0} when
     * {@code requestCount == 0}.
     */
    default double missRate() {
        long total = requestCount();
        return total == 0 ? 0.0 : (double) missCount() / total;
    }

    /** Number of entries evicted by LRU pressure. */
    long evictionCount();

    /** Bytes evicted by LRU pressure. */
    long evictionBytes();

    /** Bytes currently resident in the cache. */
    long usedBytes();

    /** Configured capacity in bytes. */
    long capacityBytes();

    /** Used / capacity ratio, or {@code 0.0} when capacity is zero. */
    default double usedPercent() {
        return capacityBytes() == 0 ? 0.0 : (double) usedBytes() / capacityBytes();
    }
}
