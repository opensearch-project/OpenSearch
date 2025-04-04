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
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.utils.IndexStoredRuleParser;
import org.opensearch.rule.utils.IndexStoredRuleUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * This class encapsulates the logic to manage the lifecycle of rules at index level
 * @opensearch.experimental
 */
public class IndexStoredRulePersistenceService implements RulePersistenceService {
    /**
     * The system index name used for storing rules
     */
    private final String indexName;
    private final Client client;
    private final FeatureType featureType;
    private final int maxRulesPerPage;
    private static final Logger logger = LogManager.getLogger(IndexStoredRulePersistenceService.class);

    /**
     * Constructs an instance of {@link IndexStoredRulePersistenceService} with the specified parameters.
     * This service handles persistence and retrieval of stored rules within an OpenSearch index.
     * @param indexName - The name of the OpenSearch index where the rules are stored.
     * @param client - The OpenSearch client used to interact with the OpenSearch cluster.
     * @param featureType - The feature type associated with the stored rules.
     * @param maxRulesPerPage - The maximum number of rules that can be returned in a single get request.
     */
    public IndexStoredRulePersistenceService(String indexName, Client client, FeatureType featureType, int maxRulesPerPage) {
        this.indexName = indexName;
        this.client = client;
        this.featureType = featureType;
        this.maxRulesPerPage = maxRulesPerPage;
    }

    /**
     * Entry point for the get rule api logic in persistence service.
     * @param getRuleRequest the getRuleRequest to process.
     * @param listener the listener for GetRuleResponse.
     */
    public void getRule(GetRuleRequest getRuleRequest, ActionListener<GetRuleResponse> listener) {
        getRuleFromIndex(getRuleRequest.getId(), getRuleRequest.getAttributeFilters(), getRuleRequest.getSearchAfter(), listener);
    }

    /**
     * Get rules from index. If id is provided, we only get a single rule.
     * Otherwise, we get all rules that satisfy the attributeFilters.
     * @param id - The id of the rule to get.
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     * @param listener - ActionListener for GetRuleResponse
     */
    public void getRuleFromIndex(
        String id,
        Map<Attribute, Set<String>> attributeFilters,
        String searchAfter,
        ActionListener<GetRuleResponse> listener
    ) {
        // Stash the current thread context when interacting with system index to perform
        // operations as the system itself, bypassing authorization checks. This ensures that
        // actions within this block are trusted and executed with system-level privileges.
        try (ThreadContext.StoredContext context = getContext()) {
            BoolQueryBuilder boolQuery = IndexStoredRuleUtils.buildGetRuleQuery(id, attributeFilters, featureType);
            SearchRequestBuilder searchRequest = client.prepareSearch(indexName).setQuery(boolQuery).setSize(maxRulesPerPage);
            if (searchAfter != null) {
                searchRequest.addSort(_ID_STRING, SortOrder.ASC).searchAfter(new Object[] { searchAfter });
            }
            searchRequest.execute(ActionListener.wrap(searchResponse -> handleGetRuleResponse(id, searchResponse, listener), e -> {
                logger.error("Failed to fetch all rules: {}", e.getMessage());
                listener.onFailure(e);
            }));
        }
    }

    /**
     * Process searchResponse from index and send a GetRuleResponse
     * @param searchResponse - Response received from index
     * @param listener - ActionListener for GetRuleResponse
     */
    void handleGetRuleResponse(String id, SearchResponse searchResponse, ActionListener<GetRuleResponse> listener) {
        List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
        if (id != null && hits.isEmpty()) {
            logger.error("Rule with ID " + id + " not found.");
            listener.onFailure(new ResourceNotFoundException("Rule with ID " + id + " doesn't exist in the .rules index."));
            return;
        }
        Map<String, Rule> ruleMap = hits.stream()
            .collect(Collectors.toMap(SearchHit::getId, hit -> IndexStoredRuleParser.parseRule(hit.getSourceAsString(), featureType)));
        String nextSearchAfter = hits.isEmpty() ? null : hits.get(hits.size() - 1).getId();
        listener.onResponse(new GetRuleResponse(ruleMap, nextSearchAfter, RestStatus.OK));
    }

    private ThreadContext.StoredContext getContext() {
        return client.threadPool().getThreadContext().stashContext();
    }

    /**
     * client getter
     */
    public Client getClient() {
        return client;
    }
}
