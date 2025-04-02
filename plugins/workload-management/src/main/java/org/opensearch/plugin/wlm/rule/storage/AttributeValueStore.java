/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.storage;

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
     * @param key to be removed
     */
    void remove(K key);

    /**
     * Returns the value associated with the key
     * @param key in the data structure
     * @return
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
