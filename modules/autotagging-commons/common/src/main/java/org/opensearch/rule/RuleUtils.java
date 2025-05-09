/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.Rule;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Utility class for operations related to {@link Rule} objects.
 * @opensearch.experimental
 */
@ExperimentalApi
public class RuleUtils {

    /**
     * constructor for RuleUtils
     */
    public RuleUtils() {}

    /**
     * Checks if a duplicate rule exists and returns its id.
     * Two rules are considered to be duplicate when meeting all the criteria below
     * 1. They have the same feature type
     * 2. They have the exact same attributes
     * 3. For each attribute, the sets of values must intersect — i.e., at least one common value must exist
     *  between the current rule and the one being checked.
     *
     * @param rule The rule to be validated against ruleMap.
     * @param ruleMap This map contains existing rules to be checked
     */
    public static Optional<String> getDuplicateRuleId(Rule rule, Map<String, Rule> ruleMap) {
        Map<Attribute, Set<String>> targetAttributeMap = rule.getAttributeMap();
        for (Map.Entry<String, Rule> entry : ruleMap.entrySet()) {
            Rule currRule = entry.getValue();
            Map<Attribute, Set<String>> existingAttributeMap = currRule.getAttributeMap();

            if (rule.getFeatureType() != currRule.getFeatureType() || targetAttributeMap.size() != existingAttributeMap.size()) {
                continue;
            }
            boolean allAttributesIntersect = true;
            for (Attribute attribute : targetAttributeMap.keySet()) {
                Set<String> targetAttributeValues = targetAttributeMap.get(attribute);
                Set<String> existingAttributeValues = existingAttributeMap.get(attribute);
                if (existingAttributeValues == null || Collections.disjoint(targetAttributeValues, existingAttributeValues)) {
                    allAttributesIntersect = false;
                    break;
                }
            }
            if (allAttributesIntersect) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }
}
