/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.rule.storage.AttributeValueStoreFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * This class is responsible for managing in-memory view of Rules and Find matching Rule for the request
 * Each auto-tagging feature should use a separate instance of this class as this avoid potential concurrency overhead
 * in case of dynamic updates and attribute sharing scenarios
 */
public class InMemoryRuleProcessingService {

    private final AttributeValueStoreFactory attributeValueStoreFactory;

    /**
     *  Constrcutor
     * @param featureType
     * @param attributeValueStoreSupplier
     */
    public InMemoryRuleProcessingService(
        FeatureType featureType,
        Supplier<AttributeValueStore<String, String>> attributeValueStoreSupplier
    ) {
        attributeValueStoreFactory = new AttributeValueStoreFactory(featureType, attributeValueStoreSupplier);
    }

    /**
     * Adds the rule to in-memory view
     * @param rule to be added
     */
    public void add(final Rule rule) {
        perform(rule, this::addOperation);
    }

    /**
     * Removes the rule from in-memory view
     * @param rule to be removed
     */
    public void remove(final Rule rule) {
        perform(rule, this::removeOperation);
    }

    private void perform(Rule rule, BiConsumer<Map.Entry<Attribute, Set<String>>, Rule> ruleOperation) {
        for (Map.Entry<Attribute, Set<String>> attributeEntry : rule.getAttributeMap().entrySet()) {
            ruleOperation.accept(attributeEntry, rule);
        }
    }

    private void removeOperation(Map.Entry<Attribute, Set<String>> attributeEntry, Rule rule) {
        AttributeValueStore<String, String> valueStore = attributeValueStoreFactory.getAttributeValueStore(attributeEntry.getKey());
        for (String value : attributeEntry.getValue()) {
            valueStore.remove(value);
        }
    }

    private void addOperation(Map.Entry<Attribute, Set<String>> attributeEntry, Rule rule) {
        AttributeValueStore<String, String> valueStore = attributeValueStoreFactory.getAttributeValueStore(attributeEntry.getKey());
        for (String value : attributeEntry.getValue()) {
            valueStore.put(value, rule.getFeatureValue());
        }
    }

    /**
     * Evaluates the label for the current request. It finds the matches for each attribute value and then it is an
     * intersection of all the matches
     * @param attributeExtractors list of extractors which are used to get the attribute values to find the
     *                           matching rule
     * @return a label if there is unique label otherwise empty
     */
    public Optional<String> evaluateLabel(List<AttributeExtractor<String>> attributeExtractors) {
        assert attributeValueStoreFactory != null;
        Optional<String> result = Optional.empty();
        for (AttributeExtractor<String> attributeExtractor : attributeExtractors) {
            AttributeValueStore<String, String> valueStore = attributeValueStoreFactory.getAttributeValueStore(
                attributeExtractor.getAttribute()
            );
            for (String value : attributeExtractor.extract()) {
                Optional<String> possibleMatch = valueStore.get(value);

                if (possibleMatch.isEmpty()) {
                    return Optional.empty();
                }

                if (result.isEmpty()) {
                    result = possibleMatch;
                } else {
                    boolean isThePossibleMatchEqualResult = possibleMatch.get().equals(result.get());
                    if (!isThePossibleMatchEqualResult) {
                        return Optional.empty();
                    }
                }
            }
        }
        return result;
    }
}
