/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.feature_value_resolver;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.storage.AttributeValueStore;

import java.util.List;
import java.util.Set;

/**
 * Matches feature values for a given attribute extractor and subfield
 * by querying an attribute value store.
 */
public class FeatureValueMatcher {

    private final AttributeValueStore<String, String> attributeValueStore;
    private final AttributeExtractor<String> attributeExtractor;
    private final String subField;

    /**
     * Constructs a FeatureValueMatcher with the given store, extractor, and subfield.
     * @param attributeValueStore The store to retrieve candidate labels from.
     * @param attributeExtractor  The extractor to extract attribute values.
     * @param subField            The subfield prefix used for filtering values.
     */
    public FeatureValueMatcher(
        AttributeValueStore<String, String> attributeValueStore,
        AttributeExtractor<String> attributeExtractor,
        String subField
    ) {
        this.attributeValueStore = attributeValueStore;
        this.attributeExtractor = attributeExtractor;
        this.subField = subField;
    }

    /**
     * Matches and aggregates candidate feature values from the attribute extractor
     * whose values start with the specified subfield.
     */
    public CandidateFeatureValues match() {
        CandidateFeatureValues result = null;
        for (String value : attributeExtractor.extract()) {
            if (value.startsWith(subField)) {
                List<Set<String>> candidateLabels = attributeValueStore.get(value);
                CandidateFeatureValues candidateValues = new CandidateFeatureValues(candidateLabels);
                if (result == null) {
                    result = candidateValues;
                } else {
                    result = candidateValues.merge(result, attributeExtractor.getCombinationStyle());
                }
            }
        }
        return result;
    }
}
