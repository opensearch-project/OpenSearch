/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.feature_value_resolver.FeatureValueAggregator;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.rule.storage.AttributeValueStoreFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * This class is responsible for managing in-memory view of Rules and Find matching Rule for the request
 * Each auto-tagging feature should use a separate instance of this class as this avoid potential concurrency overhead
 * in case of dynamic updates and attribute sharing scenarios
 */
public class InMemoryRuleProcessingService {

    /**
     * Wildcard character which will be removed as we only support prefix based search rather than pattern match based
     */
    public static final String WILDCARD = "*";
    private final AttributeValueStoreFactory attributeValueStoreFactory;
    /**
     * List of attributes from most priority to least priority.
     */
    private final List<Attribute> prioritizedAttributes;

    /**
     * Constructs an InMemoryRuleProcessingService with the given
     * attribute value store factory and a prioritized list of attributes.
     * @param attributeValueStoreFactory Factory to create attribute value stores.
     * @param prioritizedAttributes      List of attributes ordered by priority for processing.
     */
    public InMemoryRuleProcessingService(AttributeValueStoreFactory attributeValueStoreFactory, List<Attribute> prioritizedAttributes) {
        this.attributeValueStoreFactory = attributeValueStoreFactory;
        this.prioritizedAttributes = prioritizedAttributes;
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
        for (Attribute attribute : rule.getFeatureType().getAllowedAttributesRegistry().values()) {
            Set<String> attributeValues;
            if (rule.getAttributeMap().containsKey(attribute)) {
                attributeValues = rule.getAttributeMap().get(attribute);
            } else {
                attributeValues = Set.of("");
            }
            ruleOperation.accept(Map.entry(attribute, attributeValues), rule);
        }
    }

    private void removeOperation(Map.Entry<Attribute, Set<String>> attributeEntry, Rule rule) {
        AttributeValueStore<String, String> valueStore = attributeValueStoreFactory.getAttributeValueStore(attributeEntry.getKey());
        for (String value : attributeEntry.getValue()) {
            valueStore.remove(value.replace(WILDCARD, ""), rule.getFeatureValue());
        }
    }

    private void addOperation(Map.Entry<Attribute, Set<String>> attributeEntry, Rule rule) {
        AttributeValueStore<String, String> valueStore = attributeValueStoreFactory.getAttributeValueStore(attributeEntry.getKey());
        for (String value : attributeEntry.getValue()) {
            valueStore.put(value.replace(WILDCARD, ""), rule.getFeatureValue());
            List<Set<String>> result = valueStore.get("my-index");
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
        sortExtractorsByPriority(attributeExtractors);
        FeatureValueAggregator aggregator = new FeatureValueAggregator(attributeValueStoreFactory);
        FeatureValueAggregator.AggregationResult result = aggregator.collect(attributeExtractors);
        return result.resolveLabel();
    }

    private void sortExtractorsByPriority(List<AttributeExtractor<String>> attributeExtractors) {
        Map<Attribute, Integer> priorityMap = new HashMap<>();
        for (int i = 0; i < prioritizedAttributes.size(); i++) {
            priorityMap.put(prioritizedAttributes.get(i), i);
        }
        attributeExtractors.sort(
            Comparator.comparingInt(extractor -> priorityMap.getOrDefault(extractor.getAttribute(), Integer.MAX_VALUE))
        );
    }
}
