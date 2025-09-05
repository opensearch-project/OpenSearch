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
 * Collects candidate feature values for a specified subfield of a given attribute extractor.
 * For example, the "principal" attribute may contain subfields such as "username" and "role":
 * principal: {
 *   "username": ["alice", "bob"],
 *   "role": ["admin"]
 * }
 * If the attribute does not define any subfields, then the subfield name is represented
 * by an empty string ""
 */
public class FeatureValueCollector {

    private final AttributeValueStore<String, String> attributeValueStore;
    private final AttributeExtractor<String> attributeExtractor;
    private final String subfield;

    /**
     * Constructs a FeatureValueCollector with the given store, extractor, and subfield.
     * @param attributeValueStore The store to retrieve candidate feature values from.
     * @param attributeExtractor  The extractor to extract attribute values.
     * @param subfield            The subfield attribute
     */
    public FeatureValueCollector(
        AttributeValueStore<String, String> attributeValueStore,
        AttributeExtractor<String> attributeExtractor,
        String subfield
    ) {
        this.attributeValueStore = attributeValueStore;
        this.attributeExtractor = attributeExtractor;
        this.subfield = subfield;
    }

    /**
     * Collects feature values for the subfield from the attribute extractor.
     */
    public CandidateFeatureValues collect() {
        CandidateFeatureValues result = null;
        for (String value : attributeExtractor.extract()) {
            if (value.startsWith(subfield)) {
                List<Set<String>> candidateLabels = attributeValueStore.get(value);
                CandidateFeatureValues candidateValues = new CandidateFeatureValues(candidateLabels);
                if (result == null) {
                    result = candidateValues;
                } else {
                    result = candidateValues.merge(result, attributeExtractor.getLogicalOperator());
                }
            }
        }
        return result;
    }
}
