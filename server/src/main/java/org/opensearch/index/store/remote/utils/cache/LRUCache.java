/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.Weigher;
import org.opensearch.index.store.remote.utils.cache.stats.FileStatsCounter;
import org.opensearch.index.store.remote.utils.cache.stats.IRefCountedCacheStats;
import org.opensearch.index.store.remote.utils.cache.stats.StatsCounter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * LRU implementation of {@link RefCountedCache}.
 * As long as {@link Node#refCount} greater than 0 or is pinned then node is not eligible for eviction.
 * So this is the best effort lazy cache to maintain capacity. <br>
 * For more context why in-house cache implementation exist look at
 * <a href="https://github.com/opensearch-project/OpenSearch/issues/4964#issuecomment-1421689586">this comment</a> and
 * <a href="https://github.com/opensearch-project/OpenSearch/issues/6225">this ticket for future plans</a>
 * <br>
 * This cache implementation meets these requirements:
 * <ul>
 * <li>This cache has max capacity and this cache will best-effort maintain it's size to not exceed defined capacity</li>
 * <li>Cache capacity is computed as the sum of all {@link Weigher#weightOf(Object)}</li>
 * <li>Supports RemovalListener</li>
 * <li>Supports Cache Pinning.</li>
 * <li>Cache maintains it's capacity using LRU Eviction while ignoring entries with {@link Node#refCount} greater than 0 from eviction</li>
 * </ul>
 * @see RefCountedCache
 *
 * @opensearch.internal
 */
class LRUCache<K, V> implements RefCountedCache<K, V> {
    private static final Logger logger = LogManager.getLogger(LRUCache.class);
    private final long capacity;

    private final HashMap<K, Node<K, V>> data;

    /** the LRU list */
    private final LinkedHashMap<K, Node<K, V>> lru;

    private final RemovalListener<K, V> listener;

    private final Weigher<V> weigher;

    private final StatsCounter<K, V> statsCounter;

    private final ReentrantLock lock;

    static class Node<K, V> {
        final K key;

        V value;

        long weight;

        int refCount;

        boolean pinned;

        Node(K key, V value, long weight) {
            this.key = key;
            this.value = value;
            this.weight = weight;
            this.refCount = 0;
            this.pinned = false;
        }

        public boolean evictable() {
            return ((refCount == 0) && (pinned == false));
        }
    }

    public LRUCache(long capacity, RemovalListener<K, V> listener, Weigher<V> weigher) {
        this.capacity = capacity;
        this.listener = listener;
        this.weigher = weigher;
        this.data = new HashMap<>();
        this.lru = new LinkedHashMap<>();
        this.lock = new ReentrantLock();
        this.statsCounter = new FileStatsCounter<>();

    }

    @Override
    public V get(K key) {
        Objects.requireNonNull(key);
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            // miss
            if (node == null) {
                statsCounter.recordMisses(key, 1);
                return null;
            }
            // hit
            incRef(key);
            statsCounter.recordHits(key, node.value, node.pinned, 1);
            return node.value;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null) {
                final V oldValue = node.value;
                replaceNode(node, value);
                return oldValue;
            } else {
                addNode(key, false, value);
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(remappingFunction);
        lock.lock();
        try {
            final Node<K, V> node = data.get(key);
            if (node == null) {
                final V newValue = remappingFunction.apply(key, null);
                if (newValue == null) {
                    // Remapping function asked for removal, but nothing to remove
                    return null;
                } else {
                    addNode(key, false, newValue);
                    statsCounter.recordMisses(key, 1);
                    return newValue;
                }
            } else {
                final V newValue = remappingFunction.apply(key, node.value);
                if (newValue == null) {
                    removeNode(key);
                    return null;
                } else {
                    statsCounter.recordHits(key, node.value, node.pinned, 1);
                    replaceNode(node, newValue);
                    return newValue;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(K key) {
        Objects.requireNonNull(key);
        lock.lock();
        try {
            removeNode(key);
        } finally {
            lock.unlock();
        }
    }

    // To be used only in testing framework.
    public void closeIndexInputReferences() {
        lock.lock();
        try {
            int closedEntries = 0;
            final Iterator<Node<K, V>> iterator = data.values().iterator();
            while (iterator.hasNext()) {
                closedEntries++;
                Node<K, V> node = iterator.next();
                iterator.remove();
                listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.RESTARTED));
            }
            logger.trace("Reference cleanup completed - Total entries: {}", closedEntries);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            lru.clear();
            final Iterator<Node<K, V>> iterator = data.values().iterator();
            while (iterator.hasNext()) {
                Node<K, V> node = iterator.next();
                iterator.remove();
                statsCounter.recordRemoval(node.value, node.pinned, node.weight);
                listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.EXPLICIT));
            }
            statsCounter.resetUsage();
            statsCounter.resetActiveUsage();
            statsCounter.resetPinnedUsage();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long size() {
        return data.size();
    }

    @Override
    public void incRef(K key) {
        Objects.requireNonNull(key);
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null) {
                if (node.refCount == 0) {
                    // if it was inactive, we should add the weight to active usage from now
                    statsCounter.recordActiveUsage(node.value, node.weight, node.pinned, false);
                }

                if (node.evictable()) {
                    // since it become active, we should remove it from eviction list
                    lru.remove(node.key);
                }

                node.refCount++;
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void decRef(K key) {
        Objects.requireNonNull(key);
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null && node.refCount > 0) {
                node.refCount--;

                if (node.evictable()) {
                    // if it becomes evictable, we should add it to eviction list
                    lru.put(node.key, node);
                    evict(); // If cache usage is already overflowing trigger evictions
                }

                if (node.refCount == 0) {
                    // if it was active, we should remove its weight from active usage
                    statsCounter.recordActiveUsage(node.value, node.weight, node.pinned, true);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Pins the key in the cache, preventing it from being evicted.
     *
     * @param key
     */
    @Override
    public void pin(K key) {
        Objects.requireNonNull(key);
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null) {
                if (node.pinned == false) {
                    statsCounter.recordPinnedUsage(node.value, node.weight, false);
                }

                if (node.evictable()) {
                    // since its pinned, we should remove it from eviction list
                    lru.remove(node.key, node);
                }

                node.pinned = true;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unpins the key in the cache, allowing it to be evicted.
     *
     * @param key
     */
    @Override
    public void unpin(K key) {
        Objects.requireNonNull(key);
        lock.lock();

        try {
            Node<K, V> node = data.get(key);
            if (node != null && (node.pinned == true)) {

                node.pinned = false;

                if (node.evictable()) {
                    // if it becomes evictable, we should add it to eviction list
                    lru.put(node.key, node);
                }

                statsCounter.recordPinnedUsage(node.value, node.weight, true);
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public Integer getRef(K key) {
        Objects.requireNonNull(key);
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null) {
                return node.refCount;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long prune(Predicate<K> keyPredicate) {
        long sum = 0L;
        lock.lock();
        try {
            final Iterator<Node<K, V>> iterator = lru.values().iterator();
            while (iterator.hasNext()) {
                final Node<K, V> node = iterator.next();
                if (keyPredicate != null && !keyPredicate.test(node.key)) {
                    continue;
                }
                iterator.remove();
                data.remove(node.key, node);
                sum += node.weight;
                statsCounter.recordRemoval(node.value, node.pinned, node.weight);
                listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.EXPLICIT));
            }
        } finally {
            lock.unlock();
        }
        return sum;
    }

    @Override
    public long usage() {
        lock.lock();
        try {
            return statsCounter.usage();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long activeUsage() {
        lock.lock();
        try {
            return statsCounter.activeUsage();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the pinned usage of this cache.
     *
     * @return the combined pinned weight of the values in this cache.
     */
    @Override
    public long pinnedUsage() {
        lock.lock();
        try {
            return statsCounter.pinnedUsage();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IRefCountedCacheStats stats() {
        lock.lock();
        try {
            return statsCounter.snapshot();
        } finally {
            lock.unlock();
        }
    }

    // To be used only for debugging purposes
    public void logCurrentState() {
        lock.lock();
        try {
            final StringBuilder allFiles = new StringBuilder("\n");
            for (Map.Entry<K, Node<K, V>> entry : data.entrySet()) {
                String path = entry.getKey().toString();
                String file = path.substring(path.lastIndexOf('/'));
                allFiles.append(file)
                    .append(" [RefCount: ")
                    .append(entry.getValue().refCount)
                    .append(" , Weight: ")
                    .append(entry.getValue().weight)
                    .append(" ]\n");
            }
            if (allFiles.length() > 1) {
                logger.trace(() -> "Cache entries : " + allFiles);
            }
        } finally {
            lock.unlock();
        }
    }

    private void addNode(K key, boolean pinned, V value) {
        final long weight = weigher.weightOf(value);
        Node<K, V> newNode = new Node<>(key, value, weight);
        data.put(key, newNode);
        statsCounter.recordUsage(value, weight, pinned, false);
        incRef(key);
        evict();
    }

    private void replaceNode(Node<K, V> node, V newValue) {
        if (node.value != newValue) { // replace if new value is not the same instance as existing value
            final V oldValue = node.value;
            final long oldWeight = node.weight;
            final long newWeight = weigher.weightOf(newValue);
            // update the value and weight
            node.value = newValue;
            node.weight = newWeight;

            // update stats
            statsCounter.recordReplacement(oldValue, newValue, oldWeight, newWeight, node.refCount > 0, node.pinned);
            listener.onRemoval(new RemovalNotification<>(node.key, oldValue, RemovalReason.REPLACED));
        }
        incRef(node.key);
        evict();
    }

    private void removeNode(K key) {
        Node<K, V> node = data.remove(key);
        if (node != null) {
            if (node.refCount > 0) {
                statsCounter.recordActiveUsage(node.value, node.weight, node.pinned, true);
            }
            if (node.evictable()) {
                lru.remove(node.key);
            }

            if (node.pinned) {
                statsCounter.recordPinnedUsage(node.value, node.weight, true);
            }

            statsCounter.recordRemoval(node.value, node.pinned, node.weight);
            listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.EXPLICIT));
        }
    }

    private boolean hasOverflowed() {
        return statsCounter.usage() >= capacity;
    }

    private void evict() {
        // Attempts to evict entries from the cache if it exceeds the maximum
        // capacity.
        final Iterator<Node<K, V>> iterator = lru.values().iterator();
        while (hasOverflowed() && iterator.hasNext()) {
            final Node<K, V> node = iterator.next();
            iterator.remove();
            // Notify the listener only if the entry was evicted
            data.remove(node.key, node);
            statsCounter.recordEviction(node.value, node.weight);
            listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.CAPACITY));
        }
    }
}
