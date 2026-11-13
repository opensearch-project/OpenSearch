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
import org.opensearch.rule.labelresolver.FeatureValueResolver;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.rule.storage.AttributeValueStoreFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * This class is responsible for managing in-memory view of Rules and Find matching Rule for the request
 * Each auto-tagging feature should use a separate instance of this class as this avoid potential concurrency overhead
 * in case of dynamic updates and attribute sharing scenarios
 */
public class InMemoryRuleProcessingService {

    private final AttributeValueStoreFactory attributeValueStoreFactory;
    /**
     * Map of prioritized attributes
     */
    private final Map<Attribute, Integer> prioritizedAttributes;
    /**
     * Feature value of each rule, as the attribute value stores hold rule ids. While an update re-adds a rule under the
     * same id, a request evaluated in between can briefly see the rule's old attributes with its new feature value.
     */
    private final Map<String, String> ruleFeatureValues = new ConcurrentHashMap<>();

    /**
     * Constructs an InMemoryRuleProcessingService with the given
     * attribute value store factory and a prioritized list of attributes.
     * @param attributeValueStoreFactory Factory to create attribute value stores.
     * @param prioritizedAttributes      Map of prioritized attributes
     */
    public InMemoryRuleProcessingService(
        AttributeValueStoreFactory attributeValueStoreFactory,
        Map<Attribute, Integer> prioritizedAttributes
    ) {
        this.attributeValueStoreFactory = attributeValueStoreFactory;
        this.prioritizedAttributes = prioritizedAttributes;
    }

    /**
     * Adds the rule to in-memory view
     * @param rule to be added
     */
    public void add(final Rule rule) {
        ruleFeatureValues.put(rule.getId(), rule.getFeatureValue());
        perform(rule, this::addOperation);
    }

    /**
     * Removes the rule from in-memory view
     * @param rule to be removed
     */
    public void remove(final Rule rule) {
        perform(rule, this::removeOperation);
        ruleFeatureValues.remove(rule.getId());
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
            valueStore.remove(value, rule.getId());
        }
    }

    private void addOperation(Map.Entry<Attribute, Set<String>> attributeEntry, Rule rule) {
        AttributeValueStore<String, String> valueStore = attributeValueStoreFactory.getAttributeValueStore(attributeEntry.getKey());
        for (String value : attributeEntry.getValue()) {
            valueStore.put(value, rule.getId());
        }
    }

    /**
     * Determines the final feature value for the given request
     * @param attributeExtractors list of attribute extractors
     */
    public Optional<String> evaluateLabel(List<AttributeExtractor<String>> attributeExtractors) {
        attributeExtractors.sort(
            Comparator.comparingInt(extractor -> prioritizedAttributes.getOrDefault(extractor.getAttribute(), Integer.MAX_VALUE))
        );
        FeatureValueResolver featureValueResolver = new FeatureValueResolver(
            attributeValueStoreFactory,
            attributeExtractors,
            ruleFeatureValues::get
        );
        return featureValueResolver.resolve();
    }
}
