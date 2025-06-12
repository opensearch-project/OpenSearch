/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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
     * Computes a UUID-based hash string for a rule based on its key attributes.
     * @param description   the rule's description
     * @param featureType   the rule's feature type
     * @param attributeMap  the rule's attribute map (will use its toString representation)
     * @param featureValue  the rule's feature value
     */
    public static String computeRuleHash(
        String description,
        FeatureType featureType,
        Map<Attribute, Set<String>> attributeMap,
        String featureValue
    ) {
        String combined = description + "|" + featureType.getName() + "|" + attributeMap.toString() + "|" + featureValue;
        UUID uuid = UUID.nameUUIDFromBytes(combined.getBytes(StandardCharsets.UTF_8));
        return uuid.toString();
    }

    /**
     * Checks if a duplicate rule exists and returns its id.
     * Two rules are considered to be duplicate when meeting all the criteria below
     * 1. They have the same feature type
     * 2. They have the exact same attributes
     * 3. For each attribute, the sets of values must intersect — i.e., at least one common value must exist
     *  between the current rule and the one being checked.
     *
     * @param rule The rule to be validated against ruleMap.
     * @param ruleList This list contains existing rules to be checked
     */
    public static Optional<String> getDuplicateRuleId(Rule rule, List<Rule> ruleList) {
        Map<Attribute, Set<String>> targetAttributeMap = rule.getAttributeMap();
        for (Rule currRule : ruleList) {
            String currRuleId = currRule.getId();
            if (currRuleId.equals(rule.getId())) {
                continue;
            }

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
                return Optional.of(currRuleId);
            }
        }
        return Optional.empty();
    }

    /**
     * Creates an updated {@link Rule} object by applying non-null fields from the given {@link UpdateRuleRequest}
     * to the original rule. Fields not provided in the request will retain their values from the original rule.
     * @param originalRule the original rule to update
     * @param request the request containing the new values for the rule
     * @param featureType the feature type to assign to the updated rule
     */
    public static Rule composeUpdatedRule(Rule originalRule, UpdateRuleRequest request, FeatureType featureType) {
        String requestDescription = request.getDescription();
        Map<Attribute, Set<String>> requestMap = request.getAttributeMap();
        String requestLabel = request.getFeatureValue();
        return new Rule(
            originalRule.getId(),
            requestDescription == null ? originalRule.getDescription() : requestDescription,
            requestMap == null || requestMap.isEmpty() ? originalRule.getAttributeMap() : requestMap,
            featureType,
            requestLabel == null ? originalRule.getFeatureValue() : requestLabel,
            Instant.now().toString()
        );
    }
}
