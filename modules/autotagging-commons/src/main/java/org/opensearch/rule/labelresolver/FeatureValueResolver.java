/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.labelresolver;

import org.opensearch.rule.MatchLabel;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.rule.storage.AttributeValueStoreFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is responsible for collecting feature values matches
 * from multiple {@link AttributeExtractor}s and determining the final feature value
 * by computing the intersection of feature values matches across all extractors.
 * The workflow is as follows:
 *   Each AttributeExtractor is used to fetch candidate feature values for its attribute.
 *   A list of feature values matches are collected for each attribute.
 *   An intersection of all candidate feature values is computed.
 *   The intersection is then reduced to a final feature value using tie-breaking logic.
 * When the stores hold rule ids, the matches are rule ids, mapped to feature values via featureValueOf.
 */
public class FeatureValueResolver {
    private final AttributeValueStoreFactory storeFactory;
    private final List<AttributeExtractor<String>> orderedExtractors;
    private final Function<String, String> featureValueOf;
    private final Map<Attribute, List<MatchLabel<String>>> matchLabelMap = new HashMap<>();
    private Set<String> intersection = null;
    final float EPSILON = 1e-6f;

    /**
     * Constructor for FeatureValueResolver, for stores that hold feature values
     * @param storeFactory
     * @param orderedExtractors
     */
    public FeatureValueResolver(AttributeValueStoreFactory storeFactory, List<AttributeExtractor<String>> orderedExtractors) {
        this(storeFactory, orderedExtractors, Function.identity());
    }

    /**
     * Constructor for FeatureValueResolver, for stores that hold rule ids so that only one rule's attributes match together
     * @param storeFactory
     * @param orderedExtractors
     * @param featureValueOf maps a stored rule id to its feature value
     */
    public FeatureValueResolver(
        AttributeValueStoreFactory storeFactory,
        List<AttributeExtractor<String>> orderedExtractors,
        Function<String, String> featureValueOf
    ) {
        this.storeFactory = storeFactory;
        this.orderedExtractors = orderedExtractors;
        this.featureValueOf = featureValueOf;
    }

    /**
     * Key entry function for the class.
     * This function collects feature value matches from the given list of attribute extractors,
     * returning the final label for the request.
     */
    public Optional<String> resolve() {
        for (AttributeExtractor<String> extractor : orderedExtractors) {
            Attribute attr = extractor.getAttribute();
            AttributeValueStore<String, String> store = storeFactory.getAttributeValueStore(attr);
            List<MatchLabel<String>> matchLabels = attr.findAttributeMatches(extractor, store);
            matchLabelMap.put(attr, matchLabels);
            Set<String> flattenedValues = matchLabels.stream().map(MatchLabel::getFeatureValue).collect(Collectors.toSet());
            if (intersection == null) {
                intersection = flattenedValues;
            } else {
                intersection.retainAll(flattenedValues);
            }
            if (intersection.isEmpty()) {
                return Optional.empty();
            }
        }

        if (intersection == null || intersection.isEmpty()) {
            return Optional.empty();
        }
        Optional<String> featureValue = singleFeatureValue(intersection);
        if (featureValue.isPresent()) {
            return featureValue;
        }

        return breakTie();
    }

    /**
     * Resolves ties when multiple feature values match for all attributes.
     * Iterates through the ordered extractors and selects the value with the highest match score.
     */
    private Optional<String> breakTie() {
        for (AttributeExtractor<String> extractor : orderedExtractors) {
            Set<String> nextIntersection = getTopScoringMatches(extractor.getAttribute());

            Optional<String> featureValue = singleFeatureValue(nextIntersection);
            if (featureValue.isPresent()) {
                return featureValue;
            } else {
                intersection = nextIntersection;
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the feature value if all the given matches map to the same one
     */
    private Optional<String> singleFeatureValue(Set<String> matches) {
        Set<String> featureValues = matches.stream().map(featureValueOf).filter(Objects::nonNull).collect(Collectors.toSet());
        return featureValues.size() == 1 ? Optional.of(featureValues.iterator().next()) : Optional.empty();
    }

    /**
     * Finds all values from the given extractor that are in the current intersection
     * and have the top match score.
     */
    private Set<String> getTopScoringMatches(Attribute attribute) {
        Set<String> topValues = new HashSet<>();
        List<MatchLabel<String>> matches = matchLabelMap.get(attribute);
        if (matches == null || matches.isEmpty()) {
            return topValues;
        }

        for (int i = 0; i < matches.size(); i++) {
            MatchLabel<String> curr = matches.get(i);
            if (!intersection.contains(curr.getFeatureValue())) {
                continue;
            }
            float topScore = curr.getMatchScore();
            topValues.addAll(collectAllTopScoringValues(matches, i, topScore));
            break; // only consider top score group
        }
        return topValues;
    }

    private Set<String> collectAllTopScoringValues(List<MatchLabel<String>> matches, int startIndex, float topScore) {
        Set<String> values = new HashSet<>();
        for (int j = startIndex; j < matches.size(); j++) {
            MatchLabel<String> m = matches.get(j);
            if (Math.abs(m.getMatchScore() - topScore) >= EPSILON) {
                break;
            }
            if (intersection.contains(m.getFeatureValue())) {
                values.add(m.getFeatureValue());
            }
        }
        return values;
    }
}
