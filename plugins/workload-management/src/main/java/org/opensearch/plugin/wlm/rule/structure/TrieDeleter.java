/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

/**
 * Handles the deletion operation for the Trie.
 */
class TrieDeleter {
    private TrieNode root;
    private String key;

    /**
     * Constructs a TrieDeleter with the given root and key.
     *
     * @param root The root node of the trie.
     * @param key  The key to be deleted.
     */
    public TrieDeleter(TrieNode root, String key) {
        this.root = root;
        this.key = key;
    }

    /**
     * Performs the deletion operation.
     *
     * @return true if the key was successfully deleted, false otherwise.
     */
    public boolean delete() {
        TrieNode current = root;
        TrieNode parent = null;
        String remainingKey = key;
        while (!remainingKey.isEmpty()) {
            TrieNode childNode = current.findCommonPrefixChild(remainingKey);

            if (childNode == null) {
                return false;
            }
            parent = current;
            current = childNode;
            remainingKey = remainingKey.substring(childNode.getKey().length());
        }
        final boolean deleted = current.isEndOfWord();

        if (deleted) {
            current.setEndOfWord(false);
            current.setValue(null);
            if (current.getChildren().isEmpty()) {
                deleteLeafNode(parent, current);
            }
        }

        return deleted;
    }

    private static void deleteLeafNode(TrieNode parent, TrieNode current) {
        if (parent != null) {
            parent.getChildren().remove(current.getKey());
        }
    }
}
