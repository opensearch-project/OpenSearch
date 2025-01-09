/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Represents a node in the Trie.
 * Each node contains a key, an optional value, and references to child nodes.
 */
class TrieNode {
    public static final int CLOSEST_LIMIT = 5;
    private Map<String, TrieNode> children;
    private String key;
    private String value;
    private boolean isEndOfWord;

    /**
     * Constructs a TrieNode with the given key.
     *
     * @param key The key associated with this node.
     */
    public TrieNode(String key) {
        this.children = new HashMap<>();
        this.key = key;
        this.value = null;
        this.isEndOfWord = false;
    }

    // Getters and setters
    public Map<String, TrieNode> getChildren() {
        return children;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isEndOfWord() {
        return isEndOfWord;
    }

    public void setEndOfWord(boolean endOfWord) {
        isEndOfWord = endOfWord;
    }

    public TrieNode addNewChild(String key) {
        TrieNode newNode = new TrieNode(key);
        newNode.setValue(value);
        newNode.setEndOfWord(true);
        getChildren().put(key, newNode);
        return newNode;
    }

    public TrieNode splitNode(String childKey, int commonPrefixLength) {
        String commonPrefix = childKey.substring(0, commonPrefixLength);
        TrieNode newNode = new TrieNode(commonPrefix);
        TrieNode childNode = getChildren().get(childKey);

        // remove the existing partially matching child node since we will split that
        getChildren().remove(childKey);
        // re-attach common prefix as direct child
        getChildren().put(commonPrefix, newNode);

        childNode.setKey(childKey.substring(commonPrefixLength));

        newNode.getChildren().put(childKey.substring(commonPrefixLength), childNode);
        return newNode;
    }

    public TrieNode findCommonPrefixChild(String key) {
        return getChildren().entrySet()
            .stream()
            .filter(entry -> key.startsWith(entry.getKey()))
            .findFirst()
            .map(Map.Entry::getValue)
            .orElse(null);
    }

    public List<String> findTopFiveClosest() {
        List<String> ans = new ArrayList<>(CLOSEST_LIMIT);
        Queue<TrieNode> queue = new LinkedList<>();
        queue.offer(this);

        while (!queue.isEmpty() && ans.size() < CLOSEST_LIMIT) {
            TrieNode current = queue.poll();
            if (current.isEndOfWord()) {
                ans.add(current.getValue());
            }
            queue.addAll(current.getChildren().values());
        }

        return ans;
    }
}
