/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.opensearch.plugin.wlm.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.plugin.wlm.rule.storage.AttributeValueStore;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is responsible for managing in-memory view of Rules and Find matching Rule for the request
 */
public class InMemoryRuleProcessingService {

    /**
     * Default constructor
     */
    public InMemoryRuleProcessingService() {}

    /**
     * Adds the rule to in-memory view
     * @param rule to be added
     */
    public void add(final Rule rule) {
        new AddRuleOperation(rule).perform();
    }

    /**
     * Removes the rule from in-memory view
     * @param rule to be removed
     */
    public void remove(final Rule rule) {
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
            AttributeValueStore<String, String> valueStore = attributeExtractor.getAttribute().getValueStore();
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
            final Rule.Feature feature = rule.getFeature();
            final String label = rule.getLabel();

            for (Map.Entry<Rule.Attribute, List<String>> attributeEntry : rule.getAttributeMap().entrySet()) {
                processAttributeEntry(attributeEntry);
            }
        }

        protected abstract void processAttributeEntry(Map.Entry<Rule.Attribute, List<String>> attributeEntry);
    }

    private static class DeleteRuleOperation extends RuleProcessingOperation {
        public DeleteRuleOperation(Rule rule) {
            super(rule);
        }

        @Override
        protected void processAttributeEntry(Map.Entry<Rule.Attribute, List<String>> attributeEntry) {
            Rule.Attribute attribute = attributeEntry.getKey();
            assert attribute.getValueStore() != null;

            for (String value : attributeEntry.getValue()) {
                attribute.getValueStore().remove(value);
            }
        }
    }

    private static class AddRuleOperation extends RuleProcessingOperation {
        public AddRuleOperation(Rule rule) {
            super(rule);
        }

        @Override
        protected void processAttributeEntry(Map.Entry<Rule.Attribute, List<String>> attributeEntry) {
            Rule.Attribute attribute = attributeEntry.getKey();
            assert attribute.getValueStore() != null;

            for (String value : attributeEntry.getValue()) {
                attribute.getValueStore().put(value, this.rule.getLabel());
            }
        }
    }
}
