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
import java.util.HashMap;
import java.util.HashSet;
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
    private static final String PIPE_DELIMITER = "\\|";
    private static final String DOT_DELIMITER = ".";

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
        String requestLabel = request.getFeatureValue();
        Map<Attribute, Set<String>> requestMap = request.getAttributeMap();
        Map<Attribute, Set<String>> updatedAttributeMap = new HashMap<>(originalRule.getAttributeMap());
        if (requestMap != null && !requestMap.isEmpty()) {
            updatedAttributeMap.putAll(requestMap);
        }
        return new Rule(
            originalRule.getId(),
            requestDescription == null ? originalRule.getDescription() : requestDescription,
            updatedAttributeMap,
            featureType,
            requestLabel == null ? originalRule.getFeatureValue() : requestLabel,
            Instant.now().toString()
        );
    }

    /**
     * Builds a flattened map of attribute filters from a {@link Rule}.
     * This method reformats nested or prioritized subfields (e.g., values containing "|" for sub-attributes)
     * into top-level attribute keys. For example, an attribute "principal" with value "username|admin" will
     * become "principal.username" -> "admin" in the resulting map. Attributes without prioritized subfields
     * remain unchanged.
     * The resulting map is structured to make querying rules from the index easier.
     * @param rule the rule whose attributes are to be flattened
     */
    public static Map<String, Set<String>> buildAttributeFilters(Rule rule) {
        Map<String, Set<String>> attributeFilters = new HashMap<>();

        for (Map.Entry<Attribute, Set<String>> entry : rule.getAttributeMap().entrySet()) {
            Attribute attribute = entry.getKey();
            Set<String> values = entry.getValue();
            if (hasSubfields(attribute)) {
                for (String value : values) {
                    String[] parts = value.split(PIPE_DELIMITER);
                    String topLevelAttribute = attribute.getName() + DOT_DELIMITER + parts[0];
                    attributeFilters.computeIfAbsent(topLevelAttribute, k -> new HashSet<>()).add(parts[1]);
                }
            } else {
                attributeFilters.put(attribute.getName(), values);
            }
        }
        return attributeFilters;
    }

    private static boolean hasSubfields(Attribute attribute) {
        return !attribute.getWeightedSubfields().isEmpty();
    }
}
