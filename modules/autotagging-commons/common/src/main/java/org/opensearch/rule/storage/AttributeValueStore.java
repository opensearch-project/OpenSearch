/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.opensearch.rule.MatchLabel;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This interface provides apis to store Rule attribute values
 */
public interface AttributeValueStore<K, V> {
    /**
     * Adds the value to attribute value store
     * @param key to be added
     * @param value to be added
     */
    void put(K key, V value);

    /**
     * removes the key and associated value from attribute value store
     * @param key key of the value to be removed
     * @param value to be removed
     */
    default void remove(K key, V value) {
        remove(key);
    }

    /**
     * removes the key and associated value from attribute value store
     * @param key to be removed
     */
    void remove(K key);

    /**
     * Returns the values associated with the given key, including prefix matches,
     * along with their match scores.
     * For example, searching for "str" may also return results for "st" and "s".
     * @param key the key to look up
     */
    default List<MatchLabel<V>> getMatches(K key) {
        return new ArrayList<>();
    }

    /**
     * Returns the values that exactly match the given key.
     * @param key the key to look up
     */
    default List<MatchLabel<V>> getExactMatch(K key) {
        return new ArrayList<>();
    }

    /**
     * Returns the value associated with the key
     * @param key in the data structure
     */
    Optional<V> get(K key);

    /**
     * Clears all the keys and their associated values from the attribute value store
     */
    void clear();

    /**
     * It returns the number of values stored
     * @return count of key,val pairs in the store
     */
    int size();
}
