/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.pattern;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Log parser Brain algorithm implementation. See: https://ieeexplore.ieee.org/document/10109145
 */
public class BrainLogParser {

    private static final List<String> defaultFilterPatterns = List.of(
        "(/|)([0-9]+\\.){3}[0-9]+(:[0-9]+|)(:|)", // IP
        "(?<=[^A-Za-z0-9])(\\-?\\+?\\d+)(?=[^A-Za-z0-9])|[0-9]+$" // Numbers
    );
    private static final List<String> defaultDelimiters = List.of(":", "=", "[", "]", "(", ")", "-", "|", ",", "+");
    private static final String variableDenoter = "<*>";
    // counting frequency will be grouped by composite of position and token string
    private static final String positionedTokenKeyFormat = "%d-%s";
    // Token set will be grouped by composite of tokens length per log message, word combination candidate and token position.
    private static final String groupTokenSetKeyFormat = "%d-%s-%d";

    private final Map<String, Long> tokenFreqMap;
    private final Map<String, Set<String>> groupTokenSetMap;
    private final Map<String, String> logIdGroupCandidateMap;
    private final int variableCountThreshold;
    private final float thresholdPercentage;
    private final List<String> filterPatterns;
    private final List<String> delimiters;

    /**
     * Creates new Brain log parser with default parameters
     */
    public BrainLogParser() {
        this(2, 0.0f, defaultFilterPatterns, defaultDelimiters);
    }

    /**
     * Creates new Brain log parser with overridden variableCountThreshold
     * @param variableCountThreshold the threshold to decide whether low frequency token is variable
     */
    public BrainLogParser(int variableCountThreshold) {
        this(variableCountThreshold, 0.0f, defaultFilterPatterns, defaultDelimiters);
    }

    /**
     * Creates new Brain log parser with overridden variableCountThreshold amd thresholdPercentage
     * @param variableCountThreshold the threshold to decide whether low frequency token is variable
     * @param thresholdPercentage the threshold percentage to decide which frequency is representative
     *                            frequency per log message
     */
    public BrainLogParser(int variableCountThreshold, float thresholdPercentage) {
        this(variableCountThreshold, thresholdPercentage, defaultFilterPatterns, defaultDelimiters);
    }

    /**
     * Creates new Brain log parser with overridden variableCountThreshold amd thresholdPercentage and
     * overridden filter patterns and delimiters
     * @param variableCountThreshold the threshold to decide whether low frequency token is variable
     * @param thresholdPercentage the threshold percentage to decide which frequency is representative
     *                            frequency per log message
     * @param filterPatterns a list of regex to replace matched pattern to be replaced with variable denoter
     * @param delimiters a list of delimiters to be replaced with empty string after regex replacement
     */
    public BrainLogParser(int variableCountThreshold, float thresholdPercentage, List<String> filterPatterns, List<String> delimiters) {
        this.tokenFreqMap = new HashMap<>();
        this.groupTokenSetMap = new HashMap<>();
        this.logIdGroupCandidateMap = new HashMap<>();
        this.variableCountThreshold = variableCountThreshold;
        this.thresholdPercentage = thresholdPercentage;
        this.filterPatterns = filterPatterns;
        this.delimiters = delimiters;
    }

    /**
     * Preprocess single line of log message with logId
     * @param logMessage log message body per log
     * @param logId logId of the log
     * @return list of tokens by splitting preprocessed log message
     */
    public List<String> preprocess(String logMessage, String logId) {
        // match regex and replace it with variable denoter
        for (String pattern : filterPatterns) {
            logMessage = logMessage.replaceAll(pattern, variableDenoter);
        }

        for (String delimiter : delimiters) {
            logMessage = logMessage.replace(delimiter, "");
        }

        // Append logId/docId to the end of the split tokens
        logMessage = logMessage.trim() + " " + logId;

        return Arrays.asList(logMessage.split(" "));
    }

    /**
     * Count token frequency per position/index in the token list
     * @param tokens list of tokens from preprocessed log message
     */
    public void processTokenHistogram(List<String> tokens) {
        // Ignore last element since it's designed to be appended logId
        for (int i = 0; i < tokens.size() - 1; i++) {
            String tokenKey = String.format(Locale.ROOT, positionedTokenKeyFormat, i, tokens.get(i));
            tokenFreqMap.put(tokenKey, tokenFreqMap.getOrDefault(tokenKey, 0L) + 1);
        }
    }

    /**
     * Preprocess all lines of log messages with logId list. Empty logId list is allowed as the index within
     * the list will be logId by default
     * @param logMessages list of log messages
     * @param logIds list of logIds corresponded to log message
     * @return list of token lists
     */
    public List<List<String>> preprocessAllLogs(List<String> logMessages, List<String> logIds) {
        List<List<String>> preprocessedLogs = new ArrayList<>();
        int size = logIds.isEmpty() ? logMessages.size() : Math.min(logMessages.size(), logIds.size());

        for (int i = 0; i < size; i++) {
            String logId = logIds.isEmpty() ? String.valueOf(i) : logIds.get(i);
            List<String> tokens = this.preprocess(logMessages.get(i), logId);
            if (tokens.size() > 1) {
                preprocessedLogs.add(tokens);
                this.processTokenHistogram(tokens);
            }
        }

        return preprocessedLogs;
    }

    /**
     * The second process step to calculate initial groups of tokens based on previous token histogram.
     * The group will be represented by the representative word combination of the log message. The word
     * combination usually selects the longest word combination with the same frequency that should be above
     * designed threshold.
     * <p>
     * Within initial group, new group level token set per position is counted for final log pattern calculation
     * @param preprocessedLogs preprocessed list of log messages
     */
    public void calculateGroupTokenFreq(List<List<String>> preprocessedLogs) {
        for (List<String> tokens : preprocessedLogs) {
            Map<Long, Integer> wordOccurrences = this.getWordOccurrences(tokens);
            List<Map.Entry<Long, Integer>> sortedOccurrences = this.getSortedWordCombinations(wordOccurrences);
            Map.Entry<Long, Integer> candidate = this.findCandidate(sortedOccurrences);
            String groupCandidateStr = String.format(Locale.ROOT, "%d,%d", candidate.getKey(), candidate.getValue());
            this.logIdGroupCandidateMap.put(tokens.get(tokens.size() - 1), groupCandidateStr);
            this.updateGroupTokenFreqMap(tokens, groupCandidateStr);
        }
    }

    /**
     * Parse single line of log pattern after preprocess - processTokenHistogram - calculateGroupTokenFreq
     * @param tokens list of tokens for a specific log message
     * @return parsed log pattern that is a list of string
     */
    public List<String> parseLogPattern(List<String> tokens) {
        String logId = tokens.get(tokens.size() - 1);
        String groupCandidateStr = this.logIdGroupCandidateMap.get(logId);
        String[] groupCandidate = groupCandidateStr.split(",");
        Long repFreq = Long.parseLong(groupCandidate[0]); // representative frequency of the group
        return IntStream.range(0, tokens.size() - 1).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, tokens.get(i))).map(entry -> {
            int index = entry.getKey();
            String token = entry.getValue();
            String tokenKey = String.format(Locale.ROOT, positionedTokenKeyFormat, index, token);
            assert this.tokenFreqMap.get(tokenKey) != null : String.format(Locale.ROOT, "Not found token: %s on position %d", token, index);

            boolean isHigherFrequency = this.tokenFreqMap.get(tokenKey) > repFreq;
            boolean isLowerFrequency = this.tokenFreqMap.get(tokenKey) < repFreq;
            String groupTokenKey = String.format(Locale.ROOT, groupTokenSetKeyFormat, tokens.size() - 1, groupCandidateStr, index);
            assert this.groupTokenSetMap.get(groupTokenKey) != null : String.format(
                Locale.ROOT,
                "Not found any token in group: %s",
                groupTokenKey
            );

            if (isHigherFrequency) {
                // For higher frequency token that doesn't belong to word combination, it's likely to be constant token only if
                // it's unique token on that position within the group
                boolean isUniqueToken = this.groupTokenSetMap.get(groupTokenKey).size() == 1;
                if (!isUniqueToken) {
                    return variableDenoter;
                }
            } else if (isLowerFrequency) {
                // For lower frequency token that doesn't belong to word combination, it's likely to be constant token only if
                // it doesn't exceed the preset variable count threshold. For example, some variable are limited number of enums,
                // and sometimes they could be treated as constant tokens.
                if (this.groupTokenSetMap.get(groupTokenKey).size() >= variableCountThreshold) {
                    return variableDenoter;
                }
            }
            return token;
        }).collect(Collectors.toList());
    }

    /**
     * Parse all lines of log messages to generate the log pattern map.
     * @param logMessages all lines of log messages
     * @param logIds corresponding logIds for all lines of log messages
     * @return log pattern map with log pattern string as key, grouped logIds as value
     */
    public Map<String, List<String>> parseAllLogPatterns(List<String> logMessages, List<String> logIds) {
        List<List<String>> processedMessages = this.preprocessAllLogs(logMessages, logIds);

        this.calculateGroupTokenFreq(processedMessages);

        Map<String, List<String>> logPatternMap = new HashMap<>();
        for (int i = 0; i < processedMessages.size(); i++) {
            List<String> processedMessage = processedMessages.get(i);
            String logId = logIds.isEmpty() ? String.valueOf(i) : processedMessage.get(processedMessage.size() - 1);
            List<String> logPattern = this.parseLogPattern(processedMessages.get(i));
            String patternKey = String.join(" ", logPattern);
            logPatternMap.computeIfAbsent(patternKey, k -> new ArrayList<>()).add(logId);
        }
        return logPatternMap;
    }

    /**
     * Get token histogram
     * @return map of token per position key and its frequency
     */
    public Map<String, Long> getTokenFreqMap() {
        return this.tokenFreqMap;
    }

    /**
     * Get group per length per position to its token set map
     * @return map of pattern group per length per position key and its token set
     */
    public Map<String, Set<String>> getGroupTokenSetMap() {
        return this.groupTokenSetMap;
    }

    /**
     * Get logId to its group candidate map
     * @return map of logId and group candidate
     */
    public Map<String, String> getLogIdGroupCandidateMap() {
        return this.logIdGroupCandidateMap;
    }

    private Map<Long, Integer> getWordOccurrences(List<String> tokens) {
        Map<Long, Integer> occurrences = new HashMap<>();
        for (int i = 0; i < tokens.size() - 1; i++) {
            String tokenKey = String.format(Locale.ROOT, positionedTokenKeyFormat, i, tokens.get(i));
            Long tokenFreq = tokenFreqMap.get(tokenKey);
            occurrences.put(tokenFreq, occurrences.getOrDefault(tokenFreq, 0) + 1);
        }
        return occurrences;
    }

    private List<Map.Entry<Long, Integer>> getSortedWordCombinations(Map<Long, Integer> occurrences) {
        List<Map.Entry<Long, Integer>> sortedOccurrences = new ArrayList<>(occurrences.entrySet());
        sortedOccurrences.sort((entry1, entry2) -> {
            int wordCombinationLengthComparison = entry2.getValue().compareTo(entry1.getValue());
            if (wordCombinationLengthComparison != 0) {
                return wordCombinationLengthComparison;
            } else {
                return entry2.getKey().compareTo(entry1.getKey());
            }
        });

        return sortedOccurrences;
    }

    private Map.Entry<Long, Integer> findCandidate(List<Map.Entry<Long, Integer>> sortedWordCombinations) {
        OptionalLong maxFreqOptional = sortedWordCombinations.stream().mapToLong(Map.Entry::getKey).max();
        if (maxFreqOptional.isPresent()) {
            long maxFreq = maxFreqOptional.getAsLong();
            float threshold = maxFreq * this.thresholdPercentage;
            for (Map.Entry<Long, Integer> entry : sortedWordCombinations) {
                if (entry.getKey() > threshold) {
                    return entry;
                }
            }
        }
        return sortedWordCombinations.get(0);
    }

    private void updateGroupTokenFreqMap(List<String> tokens, String groupCandidateStr) {
        int tokensLen = tokens.size() - 1;
        for (int i = 0; i < tokensLen; i++) {
            String groupTokenFreqKey = String.format(Locale.ROOT, groupTokenSetKeyFormat, tokensLen, groupCandidateStr, i);
            this.groupTokenSetMap.computeIfAbsent(groupTokenFreqKey, k -> new HashSet<>()).add(tokens.get(i));
        }
    }
}
