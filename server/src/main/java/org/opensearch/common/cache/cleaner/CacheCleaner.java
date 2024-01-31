/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.cleaner;

import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.function.Predicate;

public class CacheCleaner<K, V> implements ICacheCleaner<K, V> {
    ThreadPool threadPool;
    TimeValue cleanInterval;
    Predicate<K> keyCleanupCondition;
    StoreAwareCache<K, V> cache;
    Double threshold;
    long staleCount; // TODO is this the right place ?

    public CacheCleaner(
        ThreadPool threadPool,
        TimeValue cleanInterval,
        Predicate<K> keyCleanupCondition,
        StoreAwareCache<K, V> cache,
        Double threshold
    ) {
        this.threadPool = threadPool;
        this.cleanInterval = cleanInterval;
        this.keyCleanupCondition = keyCleanupCondition;
        this.cache = cache;
        this.threshold = threshold; // TODO gather from settings and send it here
        this.staleCount = 0;
        threadPool.schedule(this::clean, this.cleanInterval, ThreadPool.Names.SAME);
    }

    @Override
    public void clean() {
        if(!crossesThreshold()) {
            return;
        }
        for (Iterator<K> iterator = cache.keys().iterator(); iterator.hasNext(); ) {
            K key = iterator.next();
            if (keyCleanupCondition.test(key)) {
                iterator.remove();
            }
        }
    }

    private boolean crossesThreshold() {
        double staleThreshold = (double) staleCount / cache.stats().count();
        return staleThreshold > this.threshold;
    }

    private void incrementStaleCount(long count) {
        this.staleCount += count;
    }

    public static class Builder<K, V> {
        private ThreadPool threadPool;
        private TimeValue cleanInterval;
        private Predicate<K> keyCleanupCondition;
        private StoreAwareCache<K, V> cache;
        private Double threshold;

        public Builder<K, V> setThreadPool(ThreadPool threadPool) {
            this.threadPool = threadPool;
            return this;
        }

        public Builder<K, V> setCleanInterval(TimeValue cleanInterval) {
            this.cleanInterval = cleanInterval;
            return this;
        }

        public Builder<K, V> setKeyCleanupCondition(Predicate<K> keyCleanupCondition) {
            this.keyCleanupCondition = keyCleanupCondition;
            return this;
        }

        public Builder<K, V> setCache(StoreAwareCache<K, V> cache) {
            this.cache = cache;
            return this;
        }

        public Builder<K, V> setThreshold(Double threshold) {
            this.threshold = threshold;
            return this;
        }

        public CacheCleaner<K, V> build() {
            if (threadPool == null || cleanInterval == null || keyCleanupCondition == null || cache == null || threshold == null) {
                throw new IllegalStateException("All fields must be set");
            }
            return new CacheCleaner<>(threadPool, cleanInterval, keyCleanupCondition, cache, threshold);
        }
    }
}
