/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.Weigher;
import org.opensearch.index.store.remote.utils.cache.stats.CacheStats;
import org.opensearch.index.store.remote.utils.cache.stats.DefaultStatsCounter;
import org.opensearch.index.store.remote.utils.cache.stats.StatsCounter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

/**
 * LRU implementation of {@link RefCountedCache}. As long as {@link Node#refCount} greater than 0 then node is not eligible for eviction.
 * So this is best effort lazy cache to maintain capacity. <br>
 * For more context why in-house cache implementation exist look at
 * <a href="https://github.com/opensearch-project/OpenSearch/issues/4964#issuecomment-1421689586">this comment</a> and
 * <a href="https://github.com/opensearch-project/OpenSearch/issues/6225">this ticket for future plans</a>
 * <br>
 * This cache implementation meets these requirements:
 * <ul>
 * <li>This cache has max capacity and this cache will best-effort maintain it's size to not exceed defined capacity</li>
 * <li>Cache capacity is computed as the sum of all {@link Weigher#weightOf(Object)}</li>
 * <li>Supports RemovalListener</li>
 * <li>Cache maintains it's capacity using LRU Eviction while ignoring entries with {@link Node#refCount} greater than 0 from eviction</li>
 * </ul>
 * @see RefCountedCache
 *
 * @opensearch.internal
 */
class LRUCache<K, V> implements RefCountedCache<K, V> {
    private final long capacity;

    private final HashMap<K, Node<K, V>> data;

    /** the LRU list */
    private final LinkedDeque<Node<K, V>> lru;

    private final RemovalListener<K, V> listener;

    private final Weigher<V> weigher;

    private final StatsCounter<K> statsCounter;

    private volatile ReentrantLock lock;

    /**
     * this tracks cache usage on the system (as long as cache entry is in the cache)
     */
    private long usage;

    /**
     * this tracks cache usage only by entries which are being referred ({@link Node#refCount > 0})
     */
    private long activeUsage;

    static class Node<K, V> implements Linked<Node<K, V>> {
        final K key;

        V value;

        long weight;

        Node<K, V> prev;

        Node<K, V> next;

        int refCount;

        Node(K key, V value, long weight) {
            this.key = key;
            this.value = value;
            this.weight = weight;
            this.prev = null;
            this.next = null;
            this.refCount = 0;
        }

        public Node<K, V> getPrevious() {
            return prev;
        }

        public void setPrevious(Node<K, V> prev) {
            this.prev = prev;
        }

        public Node<K, V> getNext() {
            return next;
        }

        public void setNext(Node<K, V> next) {
            this.next = next;
        }

        public boolean evictable() {
            return (refCount == 0);
        }

        V getValue() {
            return value;
        }
    }

    public LRUCache(long capacity, RemovalListener<K, V> listener, Weigher<V> weigher) {
        this.capacity = capacity;
        this.listener = listener;
        this.weigher = weigher;
        this.data = new HashMap<>();
        this.lru = new LinkedDeque<>();
        this.lock = new ReentrantLock();
        this.statsCounter = new DefaultStatsCounter<>();

    }

    @Override
    public V get(K key) {
        Objects.requireNonNull(key);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            // miss
            if (node == null) {
                statsCounter.recordMisses(key, 1);
                return null;
            }
            // hit
            if (node.evictable()) {
                lru.moveToBack(node);
            }
            statsCounter.recordHits(key, 1);
            return node.value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * If put a new item to the cache, it's zero referenced.
     * Otherwise, just replace the node with new value and new weight.
     */
    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        final long weight = weigher.weightOf(value);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null) {
                final V oldValue = node.value;
                final long oldWeight = node.weight;
                // update the value and weight
                node.value = value;
                node.weight = weight;
                // update usage
                final long weightDiff = weight - oldWeight;
                if (node.refCount > 0) {
                    activeUsage += weightDiff;
                }
                if (node.evictable()) {
                    lru.moveToBack(node);
                }
                usage += weightDiff;
                // call listeners
                statsCounter.recordReplacement();
                listener.onRemoval(new RemovalNotification<>(key, oldValue, RemovalReason.REPLACED));
                evict();
                return oldValue;
            } else {
                Node<K, V> newNode = new Node<>(key, value, weight);
                data.put(key, newNode);
                lru.add(newNode);
                usage += weight;
                evict();
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
            put(e.getKey(), e.getValue());
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        Objects.requireNonNull(key);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null && node.value != null) {
                V v = remappingFunction.apply(key, node.value);
                if (v != null) {
                    final V oldValue = node.value;
                    final long oldWeight = node.weight;
                    final long weight = weigher.weightOf(v);
                    // update the value and weight
                    node.value = v;
                    node.weight = weight;

                    // update usage
                    final long weightDiff = weight - oldWeight;
                    if (node.evictable()) {
                        lru.moveToBack(node);
                    }

                    if (node.refCount > 0) {
                        activeUsage += weightDiff;
                    }

                    usage += weightDiff;
                    statsCounter.recordHits(key, 1);
                    if (oldValue != node.value) {
                        statsCounter.recordReplacement();
                        listener.onRemoval(new RemovalNotification<>(node.key, oldValue, RemovalReason.REPLACED));
                    }
                    evict();
                    return v;
                } else {
                    // is v is null, remove the item
                    data.remove(key);
                    if (node.refCount > 0) {
                        activeUsage -= node.weight;
                    }
                    usage -= node.weight;
                    if (node.evictable()) {
                        lru.remove(node);
                    }
                    statsCounter.recordRemoval(node.weight);
                    listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.EXPLICIT));
                }
            }

            statsCounter.recordMisses(key, 1);
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(K key) {
        Objects.requireNonNull(key);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Node<K, V> node = data.remove(key);
            if (node != null) {
                if (node.refCount > 0) {
                    activeUsage -= node.weight;
                }
                usage -= node.weight;
                if (node.evictable()) {
                    lru.remove(node);
                }
                statsCounter.recordRemoval(node.weight);
                listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.EXPLICIT));
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void removeAll(Iterable<? extends K> keys) {
        for (K key : keys) {
            remove(key);
        }
    }

    @Override
    public void clear() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            usage = 0L;
            activeUsage = 0L;
            lru.clear();
            for (Node<K, V> node : data.values()) {
                data.remove(node.key);
                statsCounter.recordRemoval(node.weight);
                listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.EXPLICIT));
            }
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
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null) {
                if (node.refCount == 0) {
                    // if it was inactive, we should add the weight to active usage from now
                    activeUsage += node.weight;
                }

                if (node.evictable()) {
                    // since it become active, we should remove it from eviction list
                    lru.remove(node);
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
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Node<K, V> node = data.get(key);
            if (node != null && node.refCount > 0) {
                node.refCount--;

                if (node.evictable()) {
                    // if it becomes evictable, we should add it to eviction list
                    lru.add(node);
                }

                if (node.refCount == 0) {
                    // if it was active, we should remove its weight from active usage
                    activeUsage -= node.weight;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long prune() {
        long sum = 0L;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Node<K, V> node = lru.peek();
            // If weighted values are used, then the pending operations will adjust
            // the size to reflect the correct weight
            while (node != null) {
                data.remove(node.key, node);
                sum += node.weight;
                statsCounter.recordRemoval(node.weight);
                listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.EXPLICIT));
                Node<K, V> tmp = node;
                node = node.getNext();
                lru.remove(tmp);
            }

            usage -= sum;
        } finally {
            lock.unlock();
        }
        return sum;
    }

    @Override
    public CacheUsage usage() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return new CacheUsage(usage, activeUsage);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CacheStats stats() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return statsCounter.snapshot();
        } finally {
            lock.unlock();
        }
    }

    boolean hasOverflowed() {
        return usage >= capacity;
    }

    void evict() {
        // Attempts to evict entries from the cache if it exceeds the maximum
        // capacity.
        while (hasOverflowed()) {
            final Node<K, V> node = lru.poll();

            if (node == null) {
                return;
            }

            // Notify the listener only if the entry was evicted
            data.remove(node.key, node);
            usage -= node.weight;
            statsCounter.recordEviction(node.weight);
            listener.onRemoval(new RemovalNotification<>(node.key, node.value, RemovalReason.CAPACITY));
        }
    }
}
