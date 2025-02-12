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
public interface AttributeValueStore {
    /**
     * Adds the value to the data structure
     * @param key to be added
     * @param value to be added
     */
    void addValue(String key, String value);

    /**
     * removes the key and associated value from the data structure
     * @param key to be removed
     */
    void removeValue(String key);

    /**
     * Returns the value associated with the key
     * @param key in the data structure
     * @return
     */
    Optional<String> getValue(String key);

    /**
     * Clears all the keys and their associated values from the structure
     */
    void clear();

    /**
     * It returns the number of values stored
     * @return count of key,val pairs in the store
     */
    int size();
}
