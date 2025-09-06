/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.feature_value_resolver;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.rule.storage.AttributeValueStoreFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This class collects candidate feature values for attributes using
 * attribute extractors and computes the intersection of collected values
 * across all extractors.
 */
public class FeatureValueAggregator {
    private final AttributeValueStoreFactory storeFactory;

    /**
     * Constructor for FeatureValueAggregator
     * @param storeFactory
     */
    public FeatureValueAggregator(AttributeValueStoreFactory storeFactory) {
        this.storeFactory = storeFactory;
    }

    /**
     * Collects feature values from the given list of attribute extractors,
     * returning the aggregation result including all candidate values and
     * their intersection.
     * @param extractors List of attribute extractors to collect values from.
     */
    public AggregationResult collect(List<AttributeExtractor<String>> extractors) {
        List<CandidateFeatureValues> candidateFeatureValueList = new ArrayList<>();
        Set<String> intersection = null;

        for (AttributeExtractor<String> extractor : extractors) {
            Set<String> values = collectValuesForAttribute(extractor, candidateFeatureValueList);

            if (intersection == null) {
                intersection = values;
            } else {
                intersection.retainAll(values);
            }
            if (intersection.isEmpty()) {
                break;
            }
        }

        return new AggregationResult(candidateFeatureValueList, intersection);
    }

    /**
     * Collects feature values for a single attribute extractor.
     * @param extractor The attribute extractor to collect values for.
     * @param candidateFeatureValueList List to which candidate values are added.
     */
    private Set<String> collectValuesForAttribute(
        AttributeExtractor<String> extractor,
        List<CandidateFeatureValues> candidateFeatureValueList
    ) {
        Attribute attr = extractor.getAttribute();
        AttributeValueStore<String, String> store = storeFactory.getAttributeValueStore(attr);
        List<String> subfields = attr.getPrioritizedSubfields();
        if (subfields.isEmpty()) {
            subfields = List.of("");
        }

        Set<String> collected = new HashSet<>();
        for (String subfield : subfields) {
            FeatureValueMatcher matcher = new FeatureValueMatcher(store, extractor, subfield);
            CandidateFeatureValues match = matcher.match();
            if (match != null) {
                candidateFeatureValueList.add(match);
                collected.addAll(match.getFlattenedValues());
            }
        }
        return collected;
    }

    /**
     * Encapsulates the result of feature value aggregation, including
     * all candidate feature values and their intersection.
     */
    public static class AggregationResult {
        private final List<CandidateFeatureValues> allFeatureValues;
        private final Set<String> candidates;

        /**
         * Constructs an AggregationResult.
         * @param allFeatureValues List of all candidate feature values collected.
         * @param candidates Set of values that are common to all candidates (intersection).
         */
        public AggregationResult(List<CandidateFeatureValues> allFeatureValues, Set<String> candidates) {
            this.allFeatureValues = allFeatureValues;
            this.candidates = candidates;
        }

        /**
         * Resolves the final label, or empty if no label can be determined.
         */
        public Optional<String> resolveLabel() {
            if (candidates.isEmpty()) {
                return Optional.empty();
            }
            if (candidates.size() == 1) {
                String res = candidates.iterator().next();
                return Optional.of(res);
            }
            return breakTie();
        }

        /**
         * Breaks ties among multiple candidate labels by examining the priorities and
         * positions in the CandidateFeatureValues list.
         */
        private Optional<String> breakTie() {
            Set<String> remaining = new HashSet<>(candidates);
            for (CandidateFeatureValues values : allFeatureValues) {
                String best = null;
                int bestIndex = Integer.MAX_VALUE;
                Set<String> tied = new HashSet<>();
                for (String val : remaining) {
                    int index = values.getFirstOccurrenceIndex(val);
                    if (index < bestIndex) {
                        tied.clear();
                        tied.add(val);
                        best = val;
                        bestIndex = index;
                    } else if (index == bestIndex) {
                        tied.add(val);
                    }
                }
                if (tied.size() == 1 && best != null) {
                    return Optional.of(best);
                }
                remaining = tied;
            }

            return Optional.empty();
        }
    }
}
