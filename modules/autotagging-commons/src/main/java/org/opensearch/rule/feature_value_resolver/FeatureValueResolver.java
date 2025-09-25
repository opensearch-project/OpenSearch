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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class is responsible for collecting candidate feature values
 * from multiple {@link AttributeExtractor}s and determining the final feature value
 * by computing the intersection of candidate feature values across all extractors.
 * The workflow is as follows:
 *   Each AttributeExtractor is used to fetch candidate feature values for its attribute.
 *   Candidate feature values are collected for all extractors.
 *   An intersection of all candidate feature values is computed.
 *   The intersection is then reduced to a final feature value using tie-breaking logic.
 */
public class FeatureValueResolver {
    private final AttributeValueStoreFactory storeFactory;

    /**
     * Constructor for FeatureValueAggregator
     * @param storeFactory
     */
    public FeatureValueResolver(AttributeValueStoreFactory storeFactory) {
        this.storeFactory = storeFactory;
    }

    /**
     * Key entry function for the class.
     * This function collects candidate feature values from the given list of attribute extractors,
     * returning the FeatureValueResolutionResult including all candidate values and
     * their intersection.
     * @param extractors list of attribute extractors to collect values from
     */
    public FeatureValueResolutionResult resolve(List<AttributeExtractor<String>> extractors) {
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

        return new FeatureValueResolutionResult(candidateFeatureValueList, intersection);
    }

    /**
     * Collects candidate feature values for a single attribute extractor.
     * @param extractor The attribute extractor to collect values for.
     * @param candidateFeatureValueList List to which candidate values are added.
     */
    private Set<String> collectValuesForAttribute(
        AttributeExtractor<String> extractor,
        List<CandidateFeatureValues> candidateFeatureValueList
    ) {
        Attribute attr = extractor.getAttribute();
        AttributeValueStore<String, String> store = storeFactory.getAttributeValueStore(attr);
        TreeMap<Integer, String> subfields = attr.getPrioritizedSubfields();
        if (subfields.isEmpty()) {
            subfields = new TreeMap<>(Map.of(1, ""));
        }

        // Iterate through all the subfield attributes of the attribute extractor, and take the union of the collected
        // feature values for each subfield attribute because the relationship between subfields is "OR".
        // e.g. An request comes from username_a, who has role_b. Let's say rule A matches the request because it has
        // attribute username_a, and Rule B matches because it has attribute role_b, then both rule A and rule B are
        // qualified (OR relationship between feature values from different attribute)
        Set<String> res = new HashSet<>();
        for (Map.Entry<Integer, String> subfield : subfields.entrySet()) {
            FeatureValueCollector featureValueCollector = new FeatureValueCollector(store, extractor, subfield.getValue());
            CandidateFeatureValues valuesForSubfieldAttribute = featureValueCollector.collect();
            if (valuesForSubfieldAttribute != null) {
                candidateFeatureValueList.add(valuesForSubfieldAttribute);
                res.addAll(valuesForSubfieldAttribute.getFlattenedValues());
            }
        }
        return res;
    }

    /**
     * Encapsulates the result of feature value aggregation, including
     * all candidate feature values and their intersection.
     */
    public static class FeatureValueResolutionResult {
        private final List<CandidateFeatureValues> candidateFeatureValuesList;
        private final Set<String> intersectedFeatureValues;

        /**
         * Constructs an FeatureValueResolutionResult.
         * @param candidateFeatureValuesList List of all candidate feature values collected.
         * @param intersectedFeatureValues Set of values that are common to all candidates (intersection).
         */
        public FeatureValueResolutionResult(List<CandidateFeatureValues> candidateFeatureValuesList, Set<String> intersectedFeatureValues) {
            this.candidateFeatureValuesList = candidateFeatureValuesList;
            this.intersectedFeatureValues = intersectedFeatureValues;
        }

        /**
         * Resolves the final label (feature value), or empty if no label can be determined.
         */
        public Optional<String> resolveLabel() {
            if (intersectedFeatureValues == null || intersectedFeatureValues.isEmpty()) {
                return Optional.empty();
            }
            if (intersectedFeatureValues.size() == 1) {
                String res = intersectedFeatureValues.iterator().next();
                return Optional.of(res);
            }
            return breakTie();
        }

        /**
         * Breaks ties among multiple candidate labels by examining the priorities and
         * positions in the CandidateFeatureValues list.
         */
        private Optional<String> breakTie() {
            Set<String> remaining = new HashSet<>(intersectedFeatureValues);
            for (CandidateFeatureValues values : candidateFeatureValuesList) {
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
