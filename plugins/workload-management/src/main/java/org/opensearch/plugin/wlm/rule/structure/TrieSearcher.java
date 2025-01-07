/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Handles the search operation for the Trie.
 */
class TrieSearcher {
    private TrieNode root;
    private String key;

    /**
     * Constructs a TrieSearcher with the given root and key.
     *
     * @param root The root node of the trie.
     * @param key  The key to search for.
     */
    public TrieSearcher(TrieNode root, String key) {
        this.root = root;
        this.key = key;
    }

    /**
     * Performs the search operation.
     *
     * @return The value associated with the key if found, or a list of top 5 closest matches,
     *         or null if no matches found.
     */
    public List<String> search() {
        SearchResult result = findNode();
        return result.matchType.processResult(result.node);
    }

    private SearchResult findNode() {
        TrieNode current = root;
        String remainingKey = key;

        while (!remainingKey.isEmpty()) {
            TrieNode child = findCommonPrefixChild(current, remainingKey);
            if (child == null) {
                return handleNoChildWithProperPrefixCase(current, remainingKey);
            }
            current = child;
            remainingKey = removePrefix(remainingKey, child.getKey());
        }

        return new SearchResult(current, current.isEndOfWord() ? MatchType.EXACT_MATCH : MatchType.PARTIAL_MATCH);
    }

    private static SearchResult handleNoChildWithProperPrefixCase(TrieNode current, String remainingKey) {
        // there are two scenarios now
        // 1. there is no key that starts with the remaining key
        // 2. there might be a key that completely consumes the remaining key as prefix , example key: "apple", remainingKey: "app"
        for (String childKey : current.getChildren().keySet()) {
            if (childKey.startsWith(remainingKey)) {
                return new SearchResult(current.getChildren().get(childKey), MatchType.PARTIAL_MATCH);
            }
        }
        return new SearchResult(null, MatchType.NO_MATCH);
    }

    private TrieNode findCommonPrefixChild(TrieNode node, String key) {
        return node.getChildren()
            .entrySet()
            .stream()
            .filter(entry -> key.startsWith(entry.getKey()))
            .findFirst()
            .map(Map.Entry::getValue)
            .orElse(null);
    }

    private String removePrefix(String str, String prefix) {
        return str.substring(prefix.length());
    }

    private enum MatchType {
        EXACT_MATCH(node -> Collections.singletonList(node.getValue())),
        PARTIAL_MATCH(TrieNode::findTopFiveClosest),
        NO_MATCH(n -> Collections.emptyList());

        final Function<TrieNode, List<String>> resultProcessor;

        MatchType(Function<TrieNode, List<String>> resultProcessor) {
            this.resultProcessor = resultProcessor;
        }

        public List<String> processResult(TrieNode node) {
            return resultProcessor.apply(node);
        }
    }

    private static class SearchResult {
        TrieNode node;
        MatchType matchType;

        SearchResult(TrieNode node, MatchType matchType) {
            this.node = node;
            this.matchType = matchType;
        }
    }
}
