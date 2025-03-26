/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache.stats;

import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;

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

    // Stats Counters for Full File.
    private long fullFileHitCount;
    private long fullFileRemoveCount;
    private long fullFileRemoveWeight;
    private long fullFileReplaceCount;
    private long fullFileEvictionCount;
    private long fullFileEvictionWeight;
    private long fullFileUsage;
    private long fullFileActiveUsage;

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
        this.fullFileHitCount = 0L;
        this.fullFileRemoveCount = 0L;
        this.fullFileRemoveWeight = 0L;
        this.fullFileReplaceCount = 0L;
        this.fullFileEvictionCount = 0L;
        this.fullFileEvictionWeight = 0L;
        this.fullFileUsage = 0L;
        this.fullFileActiveUsage = 0L;
    }

    @Override
    public void recordHits(K key, V value, int count) {
        hitCount += count;

        if (isFullFile(value)) fullFileHitCount++;
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

        if (isFullFile(value)) {
            fullFileRemoveCount++;
            fullFileRemoveWeight += weight;
            fullFileUsage -= weight;
        }
    }

    @Override
    public void recordReplacement(V oldValue, V newValue, long oldWeight, long newWeight, boolean shouldUpdateActiveUsage) {
        replaceCount++;
        if (isFullFile(oldValue)) fullFileReplaceCount++;

        boolean isOldFullFile = isFullFile(oldValue);
        boolean isNewFullFile = isFullFile(newValue);

        if (shouldUpdateActiveUsage) activeUsage = activeUsage - oldWeight + newWeight;
        usage = usage - oldWeight + newWeight;

        if (isOldFullFile == false && isNewFullFile == true) {
            if (shouldUpdateActiveUsage) fullFileActiveUsage = fullFileActiveUsage + newWeight;
            fullFileUsage = fullFileUsage + newWeight;
        } else if (isOldFullFile == true && isNewFullFile == false) {
            if (shouldUpdateActiveUsage) fullFileActiveUsage = fullFileActiveUsage - oldWeight;
            fullFileUsage = fullFileUsage - oldWeight;
        } else if (isOldFullFile == true && isNewFullFile == true) {
            if (shouldUpdateActiveUsage) fullFileActiveUsage = fullFileActiveUsage - oldWeight + newWeight;
            fullFileUsage = fullFileUsage - oldWeight + newWeight;
        }

    }

    @Override
    public void recordEviction(V value, long weight) {
        evictionCount++;
        evictionWeight += weight;
        usage -= weight;

        if (isFullFile(value)) {
            fullFileEvictionCount++;
            fullFileEvictionWeight += weight;
            fullFileUsage -= weight;
        }
    }

    @Override
    public void recordUsage(V value, long weight, boolean shouldDecrease) {
        weight = shouldDecrease ? -1 * weight : weight;
        usage += weight;
        if (isFullFile(value)) fullFileUsage += weight;
    }

    @Override
    public void recordActiveUsage(V value, long weight, boolean shouldDecrease) {
        weight = shouldDecrease ? -1 * weight : weight;

        activeUsage += weight;
        if (isFullFile(value)) fullFileActiveUsage += weight;
    }

    @Override
    public void resetActiveUsage() {
        this.activeUsage = 0;
        this.fullFileActiveUsage = 0;
    }

    @Override
    public void resetUsage() {
        this.usage = 0;
        this.fullFileUsage = 0;
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
    public CacheStats snapshot() {
        return new CacheStats(
            hitCount,
            missCount,
            removeCount,
            removeWeight,
            replaceCount,
            evictionCount,
            evictionWeight,
            usage,
            activeUsage,
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

    @Override
    public String toString() {
        return snapshot().toString();
    }

    private boolean isFullFile(V value) {
        return value instanceof CachedFullFileIndexInput;
    }
}
