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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

import java.util.Map;
import java.util.Set;

import jdk.jfr.Experimental;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * Utility class that provides methods for the lifecycle of rules.
 * @opensearch.experimental
 */
@Experimental
public class IndexStoredRuleUtils {

    /**
     * constructor for IndexStoredRuleUtils
     */
    public IndexStoredRuleUtils() {}

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
}
