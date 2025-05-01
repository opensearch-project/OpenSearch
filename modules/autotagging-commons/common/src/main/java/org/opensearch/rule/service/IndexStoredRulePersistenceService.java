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
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rule.DeleteRuleRequest;
import org.opensearch.rule.DeleteRuleResponse;
import org.opensearch.rule.GetRuleRequest;
import org.opensearch.rule.GetRuleResponse;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RuleQueryMapper;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.rule.autotagging.Rule._ID_STRING;

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
    private final int maxRulesPerPage;
    private final RuleEntityParser parser;
    private final RuleQueryMapper<QueryBuilder> queryBuilder;
    private static final Logger logger = LogManager.getLogger(IndexStoredRulePersistenceService.class);

    /**
     * Constructs an instance of {@link IndexStoredRulePersistenceService} with the specified parameters.
     * This service handles persistence and retrieval of stored rules within an OpenSearch index.
     * @param indexName - The name of the OpenSearch index where the rules are stored.
     * @param client - The OpenSearch client used to interact with the OpenSearch cluster.
     * @param maxRulesPerPage - The maximum number of rules that can be returned in a single get request.
     * @param parser
     * @param queryBuilder
     */
    public IndexStoredRulePersistenceService(
        String indexName,
        Client client,
        int maxRulesPerPage,
        RuleEntityParser parser,
        RuleQueryMapper<QueryBuilder> queryBuilder
    ) {
        this.indexName = indexName;
        this.client = client;
        this.maxRulesPerPage = maxRulesPerPage;
        this.parser = parser;
        this.queryBuilder = queryBuilder;
    }

    /**
     * Entry point for the get rule api logic in persistence service.
     * @param getRuleRequest the getRuleRequest to process.
     * @param listener the listener for GetRuleResponse.
     */
    public void getRule(GetRuleRequest getRuleRequest, ActionListener<GetRuleResponse> listener) {
        final QueryBuilder getQueryBuilder = queryBuilder.from(getRuleRequest)
            .filter(QueryBuilders.existsQuery(getRuleRequest.getFeatureType().getName()));
        getRuleFromIndex(getRuleRequest.getId(), getQueryBuilder, getRuleRequest.getSearchAfter(), listener);
    }

    /**
     * Get rules from index. If id is provided, we only get a single rule.
     * Otherwise, we get all rules that satisfy the attributeFilters.
     * @param queryBuilder query object
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     * @param listener - ActionListener for GetRuleResponse
     */
    private void getRuleFromIndex(String id, QueryBuilder queryBuilder, String searchAfter, ActionListener<GetRuleResponse> listener) {
        // Stash the current thread context when interacting with system index to perform
        // operations as the system itself, bypassing authorization checks. This ensures that
        // actions within this block are trusted and executed with system-level privileges.
        try (ThreadContext.StoredContext context = getContext()) {
            SearchRequestBuilder searchRequest = client.prepareSearch(indexName).setQuery(queryBuilder).setSize(maxRulesPerPage);
            if (searchAfter != null) {
                searchRequest.addSort(_ID_STRING, SortOrder.ASC).searchAfter(new Object[] { searchAfter });
            }
            searchRequest.execute(ActionListener.wrap(searchResponse -> {
                List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
                if (hasNoResults(id, listener, hits)) return;
                handleGetRuleResponse(hits, listener);
            }, e -> {
                logger.error("Failed to fetch all rules: {}", e.getMessage());
                listener.onFailure(e);
            }));
        }
    }

    private static boolean hasNoResults(String id, ActionListener<GetRuleResponse> listener, List<SearchHit> hits) {
        if (id != null && hits.isEmpty()) {
            logger.error("Rule with ID " + id + " not found.");
            listener.onFailure(new ResourceNotFoundException("Rule with ID " + id + " not found."));
            return true;
        }
        return false;
    }

    /**
     * Process searchResponse from index and send a GetRuleResponse
     * @param hits - Response received from index
     * @param listener - ActionListener for GetRuleResponse
     */
    void handleGetRuleResponse(List<SearchHit> hits, ActionListener<GetRuleResponse> listener) {
        Map<String, Rule> ruleMap = hits.stream().collect(Collectors.toMap(SearchHit::getId, hit -> parser.parse(hit.getSourceAsString())));
        String nextSearchAfter = hits.isEmpty() ? null : hits.get(hits.size() - 1).getId();
        listener.onResponse(new GetRuleResponse(ruleMap, nextSearchAfter));
    }

    private ThreadContext.StoredContext getContext() {
        return client.threadPool().getThreadContext().stashContext();
    }

    @Override
    public void deleteRule(DeleteRuleRequest request, ActionListener<DeleteRuleResponse> listener) {
        try (ThreadContext.StoredContext context = getContext()) {
            DeleteRequest deleteRequest = new DeleteRequest(indexName).id(request.getRuleId());
            client.delete(deleteRequest, ActionListener.wrap(deleteResponse -> {
                boolean acknowledged = deleteResponse.getResult() == DeleteResponse.Result.DELETED;
                if (!acknowledged) {
                    logger.warn("Rule with ID " + request.getRuleId() + " was not found or already deleted.");
                }
                listener.onResponse(new DeleteRuleResponse(acknowledged));
            }, e -> {
                if (e instanceof DocumentMissingException) {
                    logger.error("Rule with ID " + request.getRuleId() + " not found.");
                    listener.onFailure(new ResourceNotFoundException("Rule with ID " + request.getRuleId() + " not found."));
                } else {
                    logger.error("Failed to delete rule: {}", e.getMessage());
                    listener.onFailure(e);
                }
            }));
        }
    }
}
