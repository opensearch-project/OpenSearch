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

    /**
     * this tracks cache usage only by pinned entries.
     */
    private long pinnedUsage;

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
    public void recordHits(K key, V value, boolean pinned, int count) {
        hitCount += count;
    }

    @Override
    public void recordMisses(K key, int count) {
        missCount += count;
    }

    @Override
    public void recordRemoval(V value, boolean pinned, long weight) {
        removeCount++;
        removeWeight += weight;
        usage -= weight;
    }

    @Override
    public void recordReplacement(
        V oldValue,
        V newValue,
        long oldWeight,
        long newWeight,
        boolean shouldUpdateActiveUsage,
        boolean isPinned
    ) {
        replaceCount++;
        if (shouldUpdateActiveUsage) activeUsage = activeUsage - oldWeight + newWeight;
        if (isPinned) pinnedUsage = pinnedUsage - oldWeight + newWeight;
        usage = usage - oldWeight + newWeight;

    }

    @Override
    public void recordEviction(V value, long weight) {
        evictionCount++;
        evictionWeight += weight;
        usage -= weight;
    }

    @Override
    public void recordUsage(V value, long weight, boolean pinned, boolean shouldDecrease) {
        weight = shouldDecrease ? -1 * weight : weight;
        usage += weight;
    }

    @Override
    public void recordActiveUsage(V value, long weight, boolean pinned, boolean shouldDecrease) {
        weight = shouldDecrease ? -1 * weight : weight;
        activeUsage += weight;
    }

    /**
     * Records the cache usage by entries which are pinned.
     * This should be called when an entry is pinned/unpinned in the cache.
     *
     * @param weight         Weight of the entry.
     * @param shouldDecrease Should the pinned usage of the cache be decreased or not.
     */
    @Override
    public void recordPinnedUsage(V value, long weight, boolean shouldDecrease) {
        weight = shouldDecrease ? -1 * weight : weight;
        pinnedUsage += weight;
    }

    @Override
    public void resetActiveUsage() {
        this.activeUsage = 0;
    }

    /**
     * Resets the cache usage by entries which are pinned.
     * This should be called when cache is cleared.
     */
    @Override
    public void resetPinnedUsage() {
        this.pinnedUsage = 0L;
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

    /**
     * Returns the pinned usage of the cache.
     *
     * @return Pinned usage of the cache.
     */
    @Override
    public long pinnedUsage() {
        return this.pinnedUsage;
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
            activeUsage,
            pinnedUsage
        );
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }

}
