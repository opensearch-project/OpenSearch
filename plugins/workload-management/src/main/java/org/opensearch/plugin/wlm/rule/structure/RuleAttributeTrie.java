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
 * Per attribute in memory storage structure for Rules
 */
public class RuleAttributeTrie implements FastPrefixMatchingStructure {
    private static final String ALLOWED_ATTRIBUTE_VALUES = "^[a-zA-Z0-9-_]+\\*?$";
    private static final int ATTRIBUTE_MAX_LENGTH = 100;
    private TrieNode root;

    /**
     * Constructs an empty AugmentedTrie.
     */
    public RuleAttributeTrie() {
        root = new TrieNode("");
    }

    /**
     * Inserts a key-value pair into the trie.
     *
     * @param key   The key to be inserted.
     * @param value The value associated with the key.
     */
    public void insert(String key, String value) {
        if (!isValidValue(value)) {
            throw new IllegalArgumentException(
                "Invalid attribute value: " + value + " it should match the regex " + ALLOWED_ATTRIBUTE_VALUES
            );
        }
        TrieInserter inserter = new TrieInserter(root, key, value);
        root = inserter.insert();
    }

    private boolean isValidValue(String value) {
        return value.length() <= ATTRIBUTE_MAX_LENGTH && value.matches(ALLOWED_ATTRIBUTE_VALUES);
    }

    /**
     * Searches for a key in the trie.
     *
     * @param key The key to search for.
     * @return A list of string values associated with the key or its prefixes.
     *         Returns an empty list if no matches are found.
     */
    public List<String> search(String key) {
        TrieSearcher searcher = new TrieSearcher(root, key);
        return searcher.search();
    }

    /**
     * Deletes a key from the trie.
     *
     * @param key The key to be deleted.
     * @return true if the key was successfully deleted, false otherwise.
     */
    public boolean delete(String key) {
        TrieDeleter deleter = new TrieDeleter(root, key);
        return deleter.delete();
    }
}
