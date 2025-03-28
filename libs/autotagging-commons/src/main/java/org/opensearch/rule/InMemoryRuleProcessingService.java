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

/**
 * This class is responsible for managing in-memory view of Rules and Find matching Rule for the request
 */
public class InMemoryRuleProcessingService {

    /**
     *  Main constructor which should be initialised with auto-tagging features
     *  the feature specific implementations of some constructs will remain with plugins
     * @param enabledFeatures
     */
    public InMemoryRuleProcessingService(List<FeatureType> enabledFeatures) {
        AttributeValueStoreFactory.init(enabledFeatures);
    }

    /**
     * Adds the rule to in-memory view
     * @param rule to be added
     */
    public synchronized void add(final Rule rule) {
        new AddRuleOperation(rule).perform();
    }

    /**
     * Removes the rule from in-memory view
     * @param rule to be removed
     */
    public synchronized void remove(final Rule rule) {
        new DeleteRuleOperation(rule).perform();
    }

    /**
     * Evaluates the label for the current request. It finds the matches for each attribute value and then it is an
     * intersection of all the matches
     * @param attributeExtractors list of extractors which are used to get the attribute values to find the
     *                           matching rule
     * @return a label if there is unique label otherwise empty
     */
    public Optional<String> evaluateLabel(List<AttributeExtractor<String>> attributeExtractors) {
        Optional<String> result = Optional.empty();
        for (AttributeExtractor<String> attributeExtractor : attributeExtractors) {
            AttributeValueStore<String, String> valueStore = AttributeValueStoreFactory.getAttributeValueStore(
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

    private static abstract class RuleProcessingOperation {
        protected final Rule rule;

        public RuleProcessingOperation(Rule rule) {
            this.rule = rule;
        }

        void perform() {
            final FeatureType feature = rule.getFeatureType();
            final String label = rule.getFeatureValue();

            for (Map.Entry<Attribute, Set<String>> attributeEntry : rule.getAttributeMap().entrySet()) {
                processAttributeEntry(attributeEntry);
            }
        }

        protected static AttributeValueStore<String, String> getAttributeValueStore(final Attribute attribute) {
            return AttributeValueStoreFactory.getAttributeValueStore(attribute);
        }

        protected abstract void processAttributeEntry(Map.Entry<Attribute, Set<String>> attributeEntry);
    }

    private static class DeleteRuleOperation extends RuleProcessingOperation {
        public DeleteRuleOperation(Rule rule) {
            super(rule);
        }

        @Override
        protected void processAttributeEntry(Map.Entry<Attribute, Set<String>> attributeEntry) {
            AttributeValueStore<String, String> valueStore = getAttributeValueStore(attributeEntry.getKey());
            for (String value : attributeEntry.getValue()) {
                valueStore.remove(value);
            }
        }
    }

    private static class AddRuleOperation extends RuleProcessingOperation {
        public AddRuleOperation(Rule rule) {
            super(rule);
        }

        @Override
        protected void processAttributeEntry(Map.Entry<Attribute, Set<String>> attributeEntry) {
            AttributeValueStore<String, String> valueStore = getAttributeValueStore(attributeEntry.getKey());
            for (String value : attributeEntry.getValue()) {
                valueStore.put(value, this.rule.getFeatureValue());
            }
        }
    }
}
