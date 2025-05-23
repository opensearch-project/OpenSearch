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
    private final DefaultStatsCounter<K, V> pinnedFileStatsCounter;

    public FileStatsCounter() {
        overallStatsCounter = new DefaultStatsCounter<>();
        fullFileStatsCounter = new DefaultStatsCounter<>();
        blockFileStatsCounter = new DefaultStatsCounter<>();
        pinnedFileStatsCounter = new DefaultStatsCounter<>();
    }

    @Override
    public void recordHits(K key, V value, boolean pinned, int count) {
        overallStatsCounter.recordHits(key, value, pinned, count);
        if (isFullFile(value)) fullFileStatsCounter.recordHits(key, value, pinned, count);
        else blockFileStatsCounter.recordHits(key, value, pinned, count);
        if (pinned) pinnedFileStatsCounter.recordHits(key, value, pinned, count);
    }

    @Override
    public void recordMisses(K key, int count) {
        overallStatsCounter.recordMisses(key, count);
        // we haven't added a check for full file here because we don't expect full file to ever have misses.
        blockFileStatsCounter.recordMisses(key, count);
    }

    @Override
    public void recordRemoval(V value, boolean pinned, long weight) {

        overallStatsCounter.recordRemoval(value, pinned, weight);
        if (isFullFile(value)) fullFileStatsCounter.recordRemoval(value, pinned, weight);
        else blockFileStatsCounter.recordRemoval(value, pinned, weight);
        if (pinned) pinnedFileStatsCounter.recordRemoval(value, pinned, weight);

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

        boolean isOldFullFile = isFullFile(oldValue);
        boolean isNewFullFile = isFullFile(newValue);

        overallStatsCounter.recordReplacement(oldValue, newValue, oldWeight, newWeight, shouldUpdateActiveUsage, isPinned);
        fullFileStatsCounter.recordReplacement(
            oldValue,
            newValue,
            isOldFullFile ? oldWeight : 0,
            isNewFullFile ? newWeight : 0,
            shouldUpdateActiveUsage,
            isPinned
        );
        blockFileStatsCounter.recordReplacement(
            oldValue,
            newValue,
            isOldFullFile ? 0 : oldWeight,
            isNewFullFile ? 0 : newWeight,
            shouldUpdateActiveUsage,
            isPinned
        );
    }

    @Override
    public void recordEviction(V value, long weight) {

        overallStatsCounter.recordEviction(value, weight);
        if (isFullFile(value)) fullFileStatsCounter.recordEviction(value, weight);
        else blockFileStatsCounter.recordEviction(value, weight);
    }

    @Override
    public void recordUsage(V value, long weight, boolean pinned, boolean shouldDecrease) {

        overallStatsCounter.recordUsage(value, weight, pinned, shouldDecrease);
        if (isFullFile(value)) fullFileStatsCounter.recordUsage(value, weight, pinned, shouldDecrease);
        else blockFileStatsCounter.recordUsage(value, weight, pinned, shouldDecrease);
        if (pinned) pinnedFileStatsCounter.recordUsage(value, weight, pinned, shouldDecrease);
    }

    @Override
    public void recordActiveUsage(V value, long weight, boolean pinned, boolean shouldDecrease) {

        overallStatsCounter.recordActiveUsage(value, weight, pinned, shouldDecrease);
        if (isFullFile(value)) fullFileStatsCounter.recordActiveUsage(value, weight, pinned, shouldDecrease);
        else blockFileStatsCounter.recordActiveUsage(value, weight, pinned, shouldDecrease);
        if (pinned) pinnedFileStatsCounter.recordActiveUsage(value, weight, pinned, shouldDecrease);
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
        overallStatsCounter.recordPinnedUsage(value, weight, shouldDecrease);
        if (isFullFile(value)) fullFileStatsCounter.recordPinnedUsage(value, weight, shouldDecrease);
        else blockFileStatsCounter.recordPinnedUsage(value, weight, shouldDecrease);
        pinnedFileStatsCounter.recordPinnedUsage(value, weight, shouldDecrease);
    }

    @Override
    public void resetActiveUsage() {
        overallStatsCounter.resetActiveUsage();
        fullFileStatsCounter.resetActiveUsage();
        blockFileStatsCounter.resetActiveUsage();
        pinnedFileStatsCounter.resetActiveUsage();
    }

    /**
     * Resets the cache usage by entries which are pinned.
     * This should be called when cache is cleared.
     */
    @Override
    public void resetPinnedUsage() {
        overallStatsCounter.resetPinnedUsage();
        fullFileStatsCounter.resetPinnedUsage();
        blockFileStatsCounter.resetPinnedUsage();
        pinnedFileStatsCounter.resetPinnedUsage();
    }

    @Override
    public void resetUsage() {
        overallStatsCounter.resetUsage();
        fullFileStatsCounter.resetUsage();
        blockFileStatsCounter.resetUsage();
        pinnedFileStatsCounter.resetUsage();
    }

    @Override
    public long activeUsage() {
        return overallStatsCounter.activeUsage();
    }

    @Override
    public long usage() {
        return overallStatsCounter.usage();
    }

    /**
     * Returns the pinned usage of the cache.
     *
     * @return Pinned usage of the cache.
     */
    @Override
    public long pinnedUsage() {
        return pinnedFileStatsCounter.pinnedUsage();
    }

    @Override
    public IRefCountedCacheStats snapshot() {
        return new AggregateRefCountedCacheStats(
            (RefCountedCacheStats) overallStatsCounter.snapshot(),
            (RefCountedCacheStats) fullFileStatsCounter.snapshot(),
            (RefCountedCacheStats) blockFileStatsCounter.snapshot(),
            (RefCountedCacheStats) pinnedFileStatsCounter.snapshot()
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
