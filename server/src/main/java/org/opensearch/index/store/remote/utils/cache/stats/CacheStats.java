/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.store.remote.utils.cache.RefCountedCache;

import java.util.Objects;

/**
 * Statistics about the performance of a {@link RefCountedCache}.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public final class CacheStats {
    private final long hitCount;
    private final long missCount;
    private final long removeCount;
    private final long removeWeight;
    private final long replaceCount;
    private final long evictionCount;
    private final long evictionWeight;
    private final long usage;
    private final long activeUsage;
    private final FullFileStats fullFileStats;

    /**
     * Represents Stats about FullFiles in a {@link RefCountedCache}.
     */
    @ExperimentalApi
    public class FullFileStats {
        private final long hitCount;
        private final long removeCount;
        private final long removeWeight;
        private final long replaceCount;
        private final long evictionCount;
        private final long evictionWeight;
        private final long usage;
        private final long activeUsage;

        FullFileStats(
            long hitCount,
            long removeCount,
            long removeWeight,
            long replaceCount,
            long evictionCount,
            long evictionWeight,
            long usage,
            long activeUsage
        ) {

            if ((hitCount < 0)
                || (removeCount < 0)
                || (removeWeight < 0)
                || (replaceCount < 0)
                || (evictionCount < 0)
                || (evictionWeight < 0)) {
                throw new IllegalArgumentException();
            }
            this.hitCount = hitCount;
            this.removeCount = removeCount;
            this.removeWeight = removeWeight;
            this.replaceCount = replaceCount;
            this.evictionCount = evictionCount;
            this.evictionWeight = evictionWeight;
            this.usage = usage;
            this.activeUsage = activeUsage;

        }

        public long getActiveUsage() {
            return activeUsage;
        }

        public long getUsage() {
            return usage;
        }

        public long getEvictionWeight() {
            return evictionWeight;
        }

        public long getEvictionCount() {
            return evictionCount;
        }

        public long getReplaceCount() {
            return replaceCount;
        }

        public long getRemoveWeight() {
            return removeWeight;
        }

        public long getRemoveCount() {
            return removeCount;
        }

        public long getHitCount() {
            return hitCount;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            } else if (!(o instanceof FullFileStats)) {
                return false;
            }
            FullFileStats other = (FullFileStats) o;
            return hitCount == other.hitCount
                && removeCount == other.removeCount
                && removeWeight == other.removeWeight
                && replaceCount == other.replaceCount
                && evictionCount == other.evictionCount
                && evictionWeight == other.evictionWeight
                && usage == other.usage
                && activeUsage == other.activeUsage;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hitCount, removeCount, removeWeight, replaceCount, evictionCount, evictionWeight, usage, activeUsage);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()
                + '{'
                + "hitCount="
                + hitCount
                + ", "
                + "missCount="
                + missCount
                + ", "
                + "removeCount="
                + removeCount
                + ", "
                + "removeWeight="
                + removeWeight
                + ", "
                + "replaceCount="
                + replaceCount
                + ", "
                + "evictionCount="
                + evictionCount
                + ", "
                + "evictionWeight="
                + evictionWeight
                + ", "
                + "usage="
                + usage
                + ", "
                + "activeUsage="
                + activeUsage
                + '}';
        }

        //
    }

    /**
     * Constructs a new {@code CacheStats} instance.
     * <p>
     * Many parameters of the same type in a row is a bad thing, but this class is not constructed
     * by end users and is too fine-grained for a builder.
     *
     * @param hitCount       the number of cache hits
     * @param missCount      the number of cache misses*
     * @param removeCount    the number of entries removed from the cache
     * @param removeWeight   the sum of weights of entries removed from the cache
     * @param replaceCount   the number of entries replaced explicitly from the cache
     * @param evictionCount  the number of entries evicted from the cache
     * @param evictionWeight the sum of weights of entries evicted from the cache
     */
    public CacheStats(
        long hitCount,
        long missCount,
        long removeCount,
        long removeWeight,
        long replaceCount,
        long evictionCount,
        long evictionWeight,
        long usage,
        long activeUsage,
        long fullFileHitCount,
        long fullFileRemoveCount,
        long fullFileRemoveWeight,
        long fullFileReplaceCount,
        long fullFileEvictionCount,
        long fullFileEvictionWeight,
        long fullFileUsage,
        long fullFileActiveUsage
    ) {
        if ((hitCount < 0)
            || (missCount < 0)
            || (removeCount < 0)
            || (removeWeight < 0)
            || (replaceCount < 0)
            || (evictionCount < 0)
            || (evictionWeight < 0)) {
            throw new IllegalArgumentException();
        }
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.removeCount = removeCount;
        this.removeWeight = removeWeight;
        this.replaceCount = replaceCount;
        this.evictionCount = evictionCount;
        this.evictionWeight = evictionWeight;
        this.usage = usage;
        this.activeUsage = activeUsage;
        this.fullFileStats = new FullFileStats(
            fullFileHitCount,
            fullFileRemoveCount,
            fullFileRemoveWeight,
            fullFileReplaceCount,
            fullFileEvictionCount,
            fullFileEvictionWeight,
            fullFileUsage,
            fullFileActiveUsage
        );
    }

    /**
     * Returns the number of times {@link RefCountedCache} lookup methods have returned either a cached or
     * uncached value. This is defined as {@code hitCount + missCount}.
     *
     * @return the {@code hitCount + missCount}
     */
    public long requestCount() {
        return hitCount + missCount;
    }

    /**
     * Returns the number of times {@link RefCountedCache} lookup methods have returned a cached value.
     *
     * @return the number of times {@link RefCountedCache} lookup methods have returned a cached value
     */
    public long hitCount() {
        return hitCount;
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
        return (requestCount == 0) ? 1.0 : (double) hitCount / requestCount;
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
        return missCount;
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
        return (requestCount == 0) ? 0.0 : (double) missCount / requestCount;
    }

    /**
     * Returns the number of times an entry has been removed explicitly.
     *
     * @return the number of times an entry has been removed
     */
    public long removeCount() {
        return removeCount;
    }

    /**
     * Returns the sum of weights of explicitly removed entries.
     *
     * @return the sum of weights of explicitly removed entries
     */
    public long removeWeight() {
        return removeWeight;
    }

    /**
     * Returns the number of times an entry has been replaced.
     *
     * @return the number of times an entry has been replaced
     */
    public long replaceCount() {
        return replaceCount;
    }

    /**
     * Returns the number of times an entry has been evicted. This count does not include manual
     * {@linkplain RefCountedCache#remove removals}.
     *
     * @return the number of times an entry has been evicted
     */
    public long evictionCount() {
        return evictionCount;
    }

    /**
     * Returns the sum of weights of evicted entries. This total does not include manual
     * {@linkplain RefCountedCache#remove removals}.
     *
     * @return the sum of weights of evicted entities
     */
    public long evictionWeight() {
        return evictionWeight;
    }

    /**
     * Returns the total weight of the cache.
     *
     * @return the total weight of the cache
     */
    public long usage() {
        return usage;
    }

    /**
     * Returns the total active weight of the cache.
     *
     * @return the total active weight of the cache
     */
    public long activeUsage() {
        return activeUsage;
    }

    /**
     * Returns full file stats for the cache.
     * @return
     */
    public FullFileStats fullFileStats() {
        return fullFileStats;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            hitCount,
            missCount,
            removeCount,
            removeWeight,
            replaceCount,
            evictionCount,
            evictionWeight,
            usage,
            activeUsage,
            fullFileStats
        );
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof CacheStats)) {
            return false;
        }
        CacheStats other = (CacheStats) o;
        return hitCount == other.hitCount
            && missCount == other.missCount
            && removeCount == other.removeCount
            && removeWeight == other.removeWeight
            && replaceCount == other.replaceCount
            && evictionCount == other.evictionCount
            && evictionWeight == other.evictionWeight
            && usage == other.usage
            && activeUsage == other.activeUsage
            && fullFileStats.equals(other.fullFileStats);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + '{'
            + "hitCount="
            + hitCount
            + ", "
            + "missCount="
            + missCount
            + ", "
            + "removeCount="
            + removeCount
            + ", "
            + "removeWeight="
            + removeWeight
            + ", "
            + "replaceCount="
            + replaceCount
            + ", "
            + "evictionCount="
            + evictionCount
            + ", "
            + "evictionWeight="
            + evictionWeight
            + ", "
            + "usage="
            + usage
            + ", "
            + "activeUsage="
            + activeUsage
            + ", "
            + "fullFileStats="
            + fullFileStats.toString()
            + '}';
    }
}
