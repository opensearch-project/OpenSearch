/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

import java.util.List;

/**
 * Common interface which exposes methods to add/search/delete Rule attributes
 */
public interface FastPrefixMatchingStructure {
    /**
     * Inserts the rule output against the attribute value denoted by key
     * @param key
     * @param value
     */
    void insert(String key, String value);

    /**
     * Searches for a key in structure.
     *
     * @param key The key to search for.
     * @return A list of string values associated with the key or its prefixes.
     *         Returns an empty list if no matches are found.
     */
    List<String> search(String key);

    /**
     * Deletes a key from the structure.
     *
     * @param key The key to be deleted.
     * @return true if the key was successfully deleted, false otherwise.
     */
    boolean delete(String key);
}
