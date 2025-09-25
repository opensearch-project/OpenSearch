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
 * Represents candidate feature values for an attribute
 */
public class CandidateFeatureValues {

    /**
     * A list of sets of candidate feature values collected for an attribute
     * The list is ordered from the most specific match to less specific ones. For example:
     * featureValues = [ {"a", "b"}, {"c"} ]
     * Here, {"a", "b"} comes first because these feature values comes from rules with a more specific match
     * e.g. A rule with "username|123" is a more specific match than "username|1" when querying "username|1234".
     */
    private final List<Set<String>> featureValuesBySpecificity;

    /**
     * A flattened set of all candidate values collected across all specificity levels.
     * This set combines all values in 'featureValues' into a single collection for easy access
     * and intersection computations.
     */
    private final Set<String> flattenedValues = new HashSet<>();

    /**
     * Maps each feature value to the index of its first occurrence set in 'featureValues'.
     * This helps in tie-breaking: values appearing earlier in the list (i.e., more specific matches)
     * are considered better matches when resolving the final label.
     */
    private final Map<String, Integer> firstOccurrenceIndex = new HashMap<>();

    /**
     * Constructs CandidateFeatureValues initialized with given list of value sets.
     * @param initialValues List of sets of candidate values.
     */
    public CandidateFeatureValues(List<Set<String>> initialValues) {
        this.featureValuesBySpecificity = new ArrayList<>(initialValues);
        for (int i = 0; i < featureValuesBySpecificity.size(); i++) {
            for (String val : featureValuesBySpecificity.get(i)) {
                flattenedValues.add(val);
                firstOccurrenceIndex.putIfAbsent(val, i);
            }
        }
    }

    /**
     * flattenedValues getter
     */
    public Set<String> getFlattenedValues() {
        return flattenedValues;
    }

    /**
     * firstOccurrenceIndex getter
     * @param value
     */
    public int getFirstOccurrenceIndex(String value) {
        return firstOccurrenceIndex.getOrDefault(value, Integer.MAX_VALUE);
    }

    /**
     * Merges this CandidateFeatureValues with another based on the specified logical operator
     * @param other            Other CandidateFeatureValues to merge with.
     * @param logicalOperator Logical operator (AND / OR) for merging.
     */
    public CandidateFeatureValues merge(CandidateFeatureValues other, AttributeExtractor.LogicalOperator logicalOperator) {
        return switch (logicalOperator) {
            case AND -> mergeAnd(other);
            case OR -> mergeOr(other);
        };
    }

    private CandidateFeatureValues mergeOr(CandidateFeatureValues other) {
        return mergeByIndex(this.featureValuesBySpecificity, other.featureValuesBySpecificity, null);
    }

    private CandidateFeatureValues mergeAnd(CandidateFeatureValues other) {
        Set<String> elementsInThis = this.featureValuesBySpecificity.stream().flatMap(Set::stream).collect(Collectors.toSet());
        Set<String> elementsInOther = other.featureValuesBySpecificity.stream().flatMap(Set::stream).collect(Collectors.toSet());

        Set<String> common = new HashSet<>(elementsInThis);
        common.retainAll(elementsInOther);

        return mergeByIndex(this.featureValuesBySpecificity, other.featureValuesBySpecificity, common);
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
        return "(" + "values=" + featureValuesBySpecificity + ')';
    }

    List<Set<String>> getFeatureValuesBySpecificity() {
        return featureValuesBySpecificity;
    }
}
