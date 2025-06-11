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
import org.opensearch.rule.autotagging.Attribute;

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
        final Map<Attribute, Set<String>> attributeFilters = request.getAttributeFilters();
        final String id = request.getId();

        boolQuery.filter(QueryBuilders.existsQuery(request.getFeatureType().getName()));
        if (id != null) {
            return boolQuery.must(QueryBuilders.termQuery("_id", id));
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
        return boolQuery;
    }
}
