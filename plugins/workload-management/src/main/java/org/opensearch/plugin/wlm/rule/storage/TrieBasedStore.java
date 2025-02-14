/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.storage;

import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.Map;
import java.util.Optional;

/**
 * This is a patricia trie based implementation of AttributeValueStore
 */
public class TrieBasedStore implements AttributeValueStore {
    PatriciaTrie<String> trie;

    public TrieBasedStore(PatriciaTrie<String> trie) {
        this.trie = trie;
    }

    @Override
    public void add(String key, String value) {
        trie.put(key, value);
    }

    @Override
    public void remove(String key) {
        trie.remove(key);
    }

    @Override
    public Optional<String> get(String key) {
        /**
         * Since we are inserting prefixes into the trie and searching for larger strings
         * It is important to find the largest matching prefix key in the trie efficiently
         * Hence we can do binary search
         */
        String longestMatchingPrefix = findLongestMatchingPrefix(key);

        /**
         * Now there are following cases for this prefix
         * 1. There is a Rule which has this prefix as one of the attribute values. In this case we should return the
         *     Rule's label otherwise send empty
         */
        for (Map.Entry<String, String> possibleMatch :trie.prefixMap(longestMatchingPrefix).entrySet()) {
            if (key.startsWith(possibleMatch.getKey())) {
                return Optional.of(possibleMatch.getValue());
            }
        }

        return Optional.empty();
    }

    private String findLongestMatchingPrefix(String key) {
        int low = 0;
        int high = key.length()-1;

        while (low < high) {
            int mid = low + (high - low + 1) / 2;
            String possibleMatchingPrefix = key.substring(0, mid);

            if (!trie.prefixMap(possibleMatchingPrefix).isEmpty()) {
                low = mid;
            } else {
                high = mid-1;
            }
        }

        return key.substring(0, low);
    }

    @Override
    public void clear() {
        trie.clear();
    }

    @Override
    public int size() {
        return trie.size();
    }
}
