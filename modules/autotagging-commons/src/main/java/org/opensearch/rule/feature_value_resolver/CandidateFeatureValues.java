/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.feature_value_resolver;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents candidate feature values collected from attributes,
 * supporting merging and resolving ties based on occurrence order.
 */
public class CandidateFeatureValues {

    private final List<Set<String>> values;

    private Set<String> flattenedValues = new HashSet<>();

    private Map<String, Integer> firstOccurrenceIndex = new HashMap<>();

    /**
     * Constructs CandidateFeatureValues initialized with given list of value sets.
     * @param initialValues List of sets of candidate values.
     */
    public CandidateFeatureValues(List<Set<String>> initialValues) {
        this.values = new ArrayList<>(initialValues);
        computeFlattenedAndIndex();
    }

    private void computeFlattenedAndIndex() {
        for (int i = 0; i < values.size(); i++) {
            for (String val : values.get(i)) {
                flattenedValues.add(val);
                firstOccurrenceIndex.putIfAbsent(val, i);
            }
        }
    }

    /**
     * Returns the flattened set of all candidate values.
     * @return Set of all candidate values.
     */
    public Set<String> getFlattenedValues() {
        return flattenedValues;
    }

    /**
     * Returns the first occurrence index of the specified value.
     * @param value Candidate value to look up.
     */
    public int getFirstOccurrenceIndex(String value) {
        return firstOccurrenceIndex.getOrDefault(value, Integer.MAX_VALUE);
    }

    /**
     * Merges this CandidateFeatureValues with another based on the specified combination style.
     * @param other            Other CandidateFeatureValues to merge with.
     * @param combinationStyle Combination style (AND / OR) for merging.
     */
    public CandidateFeatureValues merge(CandidateFeatureValues other, AttributeExtractor.CombinationStyle combinationStyle) {
        return switch (combinationStyle) {
            case AND -> mergeAnd(other);
            case OR -> mergeOr(other);
        };
    }

    private CandidateFeatureValues mergeOr(CandidateFeatureValues other) {
        return mergeByIndex(this.values, other.values, null);
    }

    private CandidateFeatureValues mergeAnd(CandidateFeatureValues other) {
        Set<String> elementsInThis = this.values.stream().flatMap(Set::stream).collect(Collectors.toSet());
        Set<String> elementsInOther = other.values.stream().flatMap(Set::stream).collect(Collectors.toSet());

        Set<String> common = new HashSet<>(elementsInThis);
        common.retainAll(elementsInOther);

        return mergeByIndex(this.values, other.values, common);
    }

    private CandidateFeatureValues mergeByIndex(List<Set<String>> list1, List<Set<String>> list2, Set<String> filterElements) {
        List<Set<String>> result = new ArrayList<>();
        int max = Math.max(list1.size(), list2.size());

        for (int i = 0; i < max; i++) {
            Set<String> merged = new HashSet<>();
            if (i < list1.size()) {
                merged.addAll(list1.get(i));
            }
            if (i < list2.size()) {
                merged.addAll(list2.get(i));
            }
            if (filterElements != null) {
                merged.retainAll(filterElements);
            }
            if (!merged.isEmpty()) {
                result.add(merged);
            }
        }
        return new CandidateFeatureValues(result);
    }

    @Override
    public String toString() {
        return "(" + "values=" + values + ')';
    }
}
