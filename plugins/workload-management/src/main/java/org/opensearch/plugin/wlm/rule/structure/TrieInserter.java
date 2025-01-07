/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

/**
 * Handles the insertion operation for the Trie.
 */
class TrieInserter {
    private TrieNode root;
    private String key;
    private String value;

    /**
     * Constructs a TrieInserter with the given root, key, and value.
     *
     * @param root  The root node of the trie.
     * @param key   The key to be inserted.
     * @param value The value associated with the key.
     */
    public TrieInserter(TrieNode root, String key, String value) {
        this.root = root;
        this.key = key;
        this.value = value;
    }

    /**
     * Performs the insertion operation.
     * Method should handle 3 cases
     * <ol>
     * <li>Simple addition of new child </li>
     * <li>insert splits a node</li>
     * <li>inserted key is a prefix to existing key|s, this could either mark a node as endOfWord or it could also split the node</li>
     * </ol>
     * @return The root node of the trie after insertion.
     */
    public TrieNode insert() {
        TrieNode current = root;
        String remainingKey = key;
        while (!remainingKey.isEmpty()) {
            TrieNode child = current.findCommonPrefixChild(remainingKey);

            if (child == null) {
                boolean partialMatch = false;
                // partial match
                for (String childKey : current.getChildren().keySet()) {
                    int commonPrefixLength = getLongestCommonPrefixLength(childKey, remainingKey);
                    if (commonPrefixLength > 0) {
                        TrieNode newNode = current.splitNode(childKey, commonPrefixLength);

                        remainingKey = remainingKey.substring(commonPrefixLength);

                        current = newNode;
                        partialMatch = true;
                        break;
                    }
                }
                // no match
                if (!partialMatch) {
                    current = current.addNewChild(remainingKey);
                    remainingKey = "";
                }
            } else {
                current = child;
                remainingKey = remainingKey.substring(child.getKey().length());
            }
        }
        updateNodeValue(current);
        return root;
    }

    private void updateNodeValue(TrieNode node) {
        node.setValue(value);
        node.setEndOfWord(true);
    }

    private int getLongestCommonPrefixLength(String str1, String str2) {
        int minLength = Math.min(str1.length(), str2.length());
        for (int i = 0; i < minLength; i++) {
            if (str1.charAt(i) != str2.charAt(i)) {
                return i;
            }
        }
        return minLength;
    }
}
