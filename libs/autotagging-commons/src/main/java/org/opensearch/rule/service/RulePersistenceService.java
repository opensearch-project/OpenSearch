/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.util.Map;
import java.util.Set;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * This class encapsulates the logic to manage the lifecycle of rules at index level
 * @opensearch.experimental
 */
public interface RulePersistenceService {
    /**
     * The default maximum number of results allowed per GET request
     */
    int MAX_RETURN_SIZE_ALLOWED_PER_GET_REQUEST = 50;

    /**
     * The system index name used for storing rules
     */
    String getIndexName();

    /**
     * client getter
     */
    Client getClient();

    /**
     * clusterService getter
     */
    ClusterService getClusterService();

    /**
     * Get the max return size allowed for page for get request when pagination is needed
     */
    default int getMaxReturnSizeAllowedPerGetRequest() {
        return MAX_RETURN_SIZE_ALLOWED_PER_GET_REQUEST;
    }

    /**
     * logger for RulePersistenceService
     */
    Logger logger = LogManager.getLogger(RulePersistenceService.class);

    /**
     * Entry point for the get rule api logic in persistence service. If id is provided, we only get a single rule.
     * Otherwise, we get all rules that satisfy the attributeFilters.
     * @param id - The id of the rule to get.
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     * @param listener - ActionListener for GetRuleResponse
     */
    default void getRule(
        String id,
        Map<Attribute, Set<String>> attributeFilters,
        String searchAfter,
        ActionListener<SearchResponse> listener
    ) {
        // Stash the current thread context when interacting with system index to perform
        // operations as the system itself, bypassing authorization checks. This ensures that
        // actions within this block are trusted and executed with system-level privileges.
        try (ThreadContext.StoredContext context = getContext()) {
            BoolQueryBuilder boolQuery = buildGetRuleQuery(id, attributeFilters);
            SearchRequestBuilder searchRequest = getClient().prepareSearch(getIndexName())
                .setQuery(boolQuery)
                .setSize(getMaxReturnSizeAllowedPerGetRequest());
            if (searchAfter != null) {
                searchRequest.addSort(_ID_STRING, SortOrder.ASC).searchAfter(new Object[] { searchAfter });
            }
            searchRequest.execute(ActionListener.wrap(listener::onResponse, e -> {
                logger.error("Failed to fetch all rules: {}", e.getMessage());
                listener.onFailure(e);
            }));
        }
    }

    /**
     * Builds a bool query to retrieve rules from the rules index, applying attribute-based filters
     * when needed and ensuring that the rules are associated with the query group feature type.
     * @param id              The ID of the rule to fetch. If not null, the search will return only this specific rule.
     * @param attributeFilters A map of attributes to their associated set of values used to filter the rules.
     */
    default BoolQueryBuilder buildGetRuleQuery(String id, Map<Attribute, Set<String>> attributeFilters) {
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
        boolQuery.filter(QueryBuilders.existsQuery(retrieveFeatureTypeInstance().getName()));
        return boolQuery;
    }

    /**
     * Abstract method for subclasses to provide specific FeatureType Instance
     */
    FeatureType retrieveFeatureTypeInstance();

    private ThreadContext.StoredContext getContext() {
        return getClient().threadPool().getThreadContext().stashContext();
    }
}
