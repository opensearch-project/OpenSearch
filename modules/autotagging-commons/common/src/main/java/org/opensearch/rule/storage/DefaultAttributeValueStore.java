/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.opensearch.rule.MatchLabel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is a patricia trie based implementation of AttributeValueStore
 * We are choosing patricia trie because it provides very fast search operations on prefix matches as well as range
 * lookups. It provides a very efficient storage for strings
 * ref: https://commons.apache.org/proper/commons-collections/javadocs/api-4.4/org/apache/commons/collections4/trie/PatriciaTrie.html
 */
public class DefaultAttributeValueStore<K extends String, V> implements AttributeValueStore<K, V> {
    private final PatriciaTrie<Set<V>> trie;
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private static final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    /**
     * Default constructor
     */
    public DefaultAttributeValueStore() {
        this(new PatriciaTrie<>());
    }

    /**
     * Main constructor
     * @param trie A Patricia Trie
     */
    public DefaultAttributeValueStore(PatriciaTrie<Set<V>> trie) {
        this.trie = trie;
    }

    @Override
    public void put(K key, V value) {
        writeLock.lock();
        try {
            trie.computeIfAbsent(key, k -> new HashSet<>()).add(value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void remove(K key, V value) {
        writeLock.lock();
        try {
            trie.computeIfPresent(key, (k, values) -> {
                values.remove(value);
                return values.isEmpty() ? null : values;
            });
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void remove(K key) {
        throw new UnsupportedOperationException("This remove(K key) function is not supported within DefaultAttributeValueStore.");
    }

    @Override
    public Optional<V> get(K key) {
        throw new UnsupportedOperationException("This get(K key) function is not supported within DefaultAttributeValueStore.");
    }

    @Override
    public List<MatchLabel<V>> getExactMatch(K key) {
        readLock.lock();
        try {
            List<MatchLabel<V>> results = new ArrayList<>();
            addMatches(results, trie.get(key), 1f);
            addMatches(results, trie.get(""), 0f);
            return results;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<MatchLabel<V>> getMatches(String key) {
        readLock.lock();
        try {
            List<MatchLabel<V>> results = new ArrayList<>();
            StringBuilder prefixBuilder = new StringBuilder(key);

            for (int i = key.length(); i >= 0; i--) {
                Set<V> values = trie.get(prefixBuilder.toString());
                if (values != null && !values.isEmpty()) {
                    float score = (float) prefixBuilder.length() / key.length();
                    addMatches(results, values, score);
                }
                if (!prefixBuilder.isEmpty()) {
                    prefixBuilder.deleteCharAt(prefixBuilder.length() - 1);
                }
            }
            return results;
        } finally {
            readLock.unlock();
        }
    }

    private void addMatches(List<MatchLabel<V>> results, Set<V> values, float score) {
        if (values == null || values.isEmpty()) {
            return;
        }
        for (V label : values) {
            results.add(new MatchLabel<>(label, score));
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            trie.clear();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public int size() {
        return trie.size();
    }
}
