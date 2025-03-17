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
 * A non thread-safe {@link StatsCounter} implementation which aggregates multiple {@link DefaultStatsCounter}.
 *
 * @opensearch.internal
 */
public class FileStatsCounter<K, V> implements StatsCounter<K, V> {

    private final DefaultStatsCounter<K, V> overallStatsCounter;
    private final DefaultStatsCounter<K, V> fullFileStatsCounter;
    private final DefaultStatsCounter<K, V> blockFileStatsCounter;

    public FileStatsCounter() {
        overallStatsCounter = new DefaultStatsCounter<>();
        fullFileStatsCounter = new DefaultStatsCounter<>();
        blockFileStatsCounter = new DefaultStatsCounter<>();
    }

    @Override
    public void recordHits(K key, V value, int count) {
        overallStatsCounter.recordHits(key, value, count);
        if (isFullFile(value)) fullFileStatsCounter.recordHits(key, value, count);
        else blockFileStatsCounter.recordHits(key, value, count);
    }

    @Override
    public void recordMisses(K key, int count) {
        overallStatsCounter.recordMisses(key, count);
        // we haven't added a check for full file here because we don't expect full file to ever have misses.
        blockFileStatsCounter.recordMisses(key, count);
    }

    @Override
    public void recordRemoval(V value, long weight) {

        overallStatsCounter.recordRemoval(value, weight);
        if (isFullFile(value)) fullFileStatsCounter.recordRemoval(value, weight);
        else blockFileStatsCounter.recordRemoval(value, weight);
    }

    @Override
    public void recordReplacement(V oldValue, V newValue, long oldWeight, long newWeight, boolean shouldUpdateActiveUsage) {

        boolean isOldFullFile = isFullFile(oldValue);
        boolean isNewFullFile = isFullFile(newValue);

        overallStatsCounter.recordReplacement(oldValue, newValue, oldWeight, newWeight, shouldUpdateActiveUsage);
        fullFileStatsCounter.recordReplacement(
            oldValue,
            newValue,
            isOldFullFile ? oldWeight : 0,
            isNewFullFile ? newWeight : 0,
            shouldUpdateActiveUsage
        );
        blockFileStatsCounter.recordReplacement(
            oldValue,
            newValue,
            isOldFullFile ? 0 : oldWeight,
            isNewFullFile ? 0 : newWeight,
            shouldUpdateActiveUsage
        );
    }

    @Override
    public void recordEviction(V value, long weight) {

        overallStatsCounter.recordEviction(value, weight);
        if (isFullFile(value)) fullFileStatsCounter.recordEviction(value, weight);
        else blockFileStatsCounter.recordEviction(value, weight);
    }

    @Override
    public void recordUsage(V value, long weight, boolean shouldDecrease) {

        overallStatsCounter.recordUsage(value, weight, shouldDecrease);
        if (isFullFile(value)) fullFileStatsCounter.recordUsage(value, weight, shouldDecrease);
        else blockFileStatsCounter.recordUsage(value, weight, shouldDecrease);
    }

    @Override
    public void recordActiveUsage(V value, long weight, boolean shouldDecrease) {

        overallStatsCounter.recordActiveUsage(value, weight, shouldDecrease);
        if (isFullFile(value)) fullFileStatsCounter.recordActiveUsage(value, weight, shouldDecrease);
        else blockFileStatsCounter.recordActiveUsage(value, weight, shouldDecrease);
    }

    @Override
    public void resetActiveUsage() {
        overallStatsCounter.resetActiveUsage();
        fullFileStatsCounter.resetActiveUsage();
        blockFileStatsCounter.resetActiveUsage();

    }

    @Override
    public void resetUsage() {
        overallStatsCounter.resetUsage();
        fullFileStatsCounter.resetUsage();
        blockFileStatsCounter.resetUsage();
    }

    @Override
    public long activeUsage() {
        return overallStatsCounter.activeUsage();
    }

    @Override
    public long usage() {
        return overallStatsCounter.usage();
    }

    @Override
    public IRefCountedCacheStats snapshot() {
        return new AggregateRefCountedCacheStats(
            (RefCountedCacheStats) overallStatsCounter.snapshot(),
            (RefCountedCacheStats) fullFileStatsCounter.snapshot(),
            (RefCountedCacheStats) blockFileStatsCounter.snapshot()
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
