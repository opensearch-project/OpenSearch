/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.utils;

import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * Utility class that provides methods for the lifecycle of rules.
 * @opensearch.experimental
 */
public class IndexStoredRuleUtils {

    /**
     * constructor for IndexStoredRuleUtils
     */
    private IndexStoredRuleUtils() {}

    /**
     * Builds a Boolean query to retrieve a rule by its ID or attribute filters.
     * @param id               The ID of the rule to search for. If null, no ID-based filtering is applied.
     * @param attributeFilters A map of attributes and their corresponding filter values. This allows filtering by specific attribute values.
     * @param featureType      The feature type that is required in the query.
     */
    public static BoolQueryBuilder buildGetRuleQuery(String id, Map<Attribute, Set<String>> attributeFilters, FeatureType featureType) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (id != null) {
            return boolQuery.must(QueryBuilders.termQuery(_ID_STRING, id));
        }
        for (Map.Entry<Attribute, Set<String>> entry : attributeFilters.entrySet()) {
            Attribute attribute = entry.getKey();
            Set<String> values = entry.getValue();
            if (values != null && !values.isEmpty()) {
                BoolQueryBuilder attributeQuery = QueryBuilders.boolQuery();
                for (String value : values) {
                    attributeQuery.should(QueryBuilders.matchQuery(attribute.getName(), value));
                }
                boolQuery.must(attributeQuery);
            }
        }
        boolQuery.filter(QueryBuilders.existsQuery(featureType.getName()));
        return boolQuery;
    }

    /**
     * Checks if a duplicate rule exists based on the attribute map.
     * A rule is considered a duplicate when the attribute value already exists in the index, and the number of
     * attributes in the new rule is equal to the number of attributes in an existing rule.
     *
     * For example, if an existing rule has:
     *   attribute1 = ['a'] and attribute2 = ['c']
     * And we are creating a new rule with:
     *   attribute1 = ['a']
     * Then it's not a duplicate because the existing rule has attribute2 and is more granular
     *
     * @param rule The rule to be validated against ruleMap.
     * @param ruleMap This map entries are Rules that contain the attribute values from rule, meaning they
     *                have a partial or complete overlap with the new rule being created.
     */
    public static Optional<String> getDuplicateRuleId(Rule rule, Map<String, Rule> ruleMap) {
        Map<Attribute, Set<String>> attributeMapToValidate = rule.getAttributeMap();
        for (Map.Entry<String, Rule> entry : ruleMap.entrySet()) {
            String ruleId = entry.getKey();
            Rule currRule = entry.getValue();
            // Compare the size of the attribute maps to ensure we only check for duplicates with the same number of attributes.
            if (attributeMapToValidate.size() == currRule.getAttributeMap().size()) {
                return Optional.of(ruleId);
            }
        }
        return Optional.empty();
    }
}
