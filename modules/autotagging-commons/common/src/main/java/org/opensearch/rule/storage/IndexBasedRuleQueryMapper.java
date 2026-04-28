/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rule.RuleQueryMapper;
import org.opensearch.rule.action.GetRuleRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to build opensearch index based query object
 */
@ExperimentalApi
public class IndexBasedRuleQueryMapper implements RuleQueryMapper<QueryBuilder> {

    /**
     * Default constructor
     */
    public IndexBasedRuleQueryMapper() {}

    @Override
    public QueryBuilder from(GetRuleRequest request) {
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        final Map<String, Set<String>> attributeFilters = request.getAttributeFilters();
        final String id = request.getId();

        boolQuery.filter(QueryBuilders.existsQuery(request.getFeatureType().getName()));
        if (id != null) {
            return boolQuery.must(QueryBuilders.termQuery("_id", id));
        }
        Map<String, BoolQueryBuilder> groupedQueries = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : attributeFilters.entrySet()) {
            String attribute = entry.getKey();
            Set<String> values = entry.getValue();
            if (values != null && !values.isEmpty()) {
                String topLevelAttribute = attribute.contains(".") ? attribute.substring(0, attribute.indexOf('.')) : attribute;
                BoolQueryBuilder groupQuery = groupedQueries.computeIfAbsent(topLevelAttribute, k -> QueryBuilders.boolQuery());
                for (String value : values) {
                    groupQuery.should(QueryBuilders.termQuery(attribute + ".keyword", value));
                }
            }
        }
        for (BoolQueryBuilder groupQuery : groupedQueries.values()) {
            boolQuery.must(groupQuery);
        }
        return boolQuery;
    }

    @Override
    public QueryBuilder getCardinalityQuery() {
        return QueryBuilders.matchAllQuery();
    }
}
