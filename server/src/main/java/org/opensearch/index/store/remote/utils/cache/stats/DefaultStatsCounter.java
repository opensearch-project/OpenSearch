/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache.stats;

/**
 * A non thread-safe {@link StatsCounter} implementation.
 *
 * @opensearch.internal
 */
public class DefaultStatsCounter<K, V> implements StatsCounter<K, V> {
    private long hitCount;
    private long missCount;
    private long removeCount;
    private long removeWeight;
    private long replaceCount;
    private long evictionCount;
    private long evictionWeight;
    /**
     * this tracks cache usage on the system (as long as cache entry is in the cache)
     */
    private long usage;

    /**
     * this tracks cache usage only by entries which are being referred.
     */
    private long activeUsage;

    public DefaultStatsCounter() {
        this.hitCount = 0L;
        this.missCount = 0L;
        this.removeCount = 0L;
        this.removeWeight = 0L;
        this.replaceCount = 0L;
        this.evictionCount = 0L;
        this.evictionWeight = 0L;
        this.usage = 0L;
        this.activeUsage = 0L;
    }

    @Override
    public void recordHits(K key, V value, int count) {
        hitCount += count;
    }

    @Override
    public void recordMisses(K key, int count) {
        missCount += count;
    }

    @Override
    public void recordRemoval(V value, long weight) {
        removeCount++;
        removeWeight += weight;
        usage -= weight;
    }

    @Override
    public void recordReplacement(V oldValue, V newValue, long oldWeight, long newWeight, boolean shouldUpdateActiveUsage) {
        replaceCount++;
        if (shouldUpdateActiveUsage) activeUsage = activeUsage - oldWeight + newWeight;
        usage = usage - oldWeight + newWeight;

    }

    @Override
    public void recordEviction(V value, long weight) {
        evictionCount++;
        evictionWeight += weight;
        usage -= weight;
    }

    @Override
    public void recordUsage(V value, long weight, boolean shouldDecrease) {
        weight = shouldDecrease ? -1 * weight : weight;
        usage += weight;
    }

    @Override
    public void recordActiveUsage(V value, long weight, boolean shouldDecrease) {
        weight = shouldDecrease ? -1 * weight : weight;
        activeUsage += weight;
    }

    @Override
    public void resetActiveUsage() {
        this.activeUsage = 0;
    }

    @Override
    public void resetUsage() {
        this.usage = 0;
    }

    @Override
    public long activeUsage() {
        return this.activeUsage;
    }

    @Override
    public long usage() {
        return this.usage;
    }

    @Override
    public IRefCountedCacheStats snapshot() {
        return new RefCountedCacheStats(
            hitCount,
            missCount,
            removeCount,
            removeWeight,
            replaceCount,
            evictionCount,
            evictionWeight,
            usage,
            activeUsage
        );
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }

}
