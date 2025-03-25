/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.Rule;
import org.opensearch.autotagging.Rule.Builder;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.DeleteRuleResponse;
import org.opensearch.plugin.wlm.rule.action.GetRuleResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * This class encapsulates the logic to manage the lifecycle of rules at index level
 * @opensearch.experimental
 */
public class RulePersistenceService {
    public static final String RULES_INDEX = ".rules";
    private final Client client;
    private final ClusterService clusterService;
    private static final Logger logger = LogManager.getLogger(RulePersistenceService.class);
    public static final int MAX_RETURN_SIZE_ALLOWED_PER_GET_REQUEST = 50;

    @Inject
    public RulePersistenceService(final ClusterService clusterService, final Client client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * Entry point for the get rule api logic in persistence service.
     * @param id - The id of the rule to get. Get all matching rules when id is null
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     * @param listener - ActionListener for GetRuleResponse
     */
    public void getRule(
        String id,
        Map<Attribute, Set<String>> attributeFilters,
        String searchAfter,
        ActionListener<GetRuleResponse> listener
    ) {
        if (id != null) {
            fetchRuleById(id, listener);
        } else {
            fetchAllRules(attributeFilters, searchAfter, listener);
        }
    }

    /**
     * Fetch a single rule from system index using id
     * @param id - The id of the rule to get
     * @param listener - ActionListener for GetRuleResponse
     */
    void fetchRuleById(String id, ActionListener<GetRuleResponse> listener) {
        try (ThreadContext.StoredContext context = getContext()) {
            client.prepareGet(RULES_INDEX, id)
                .execute(ActionListener.wrap(getResponse -> handleGetOneRuleResponse(id, getResponse, listener), e -> {
                    logger.error("Failed to fetch rule with ID {}: {}", id, e.getMessage());
                    listener.onFailure(e);
                }));
        }
    }

    /**
     * Process getResponse from index and send a GetRuleResponse
     * @param id - The id of the rule to get
     * @param getResponse - Response received from index
     * @param listener - ActionListener for GetRuleResponse
     */
    private void handleGetOneRuleResponse(String id, GetResponse getResponse, ActionListener<GetRuleResponse> listener) {
        if (getResponse.isExists()) {
            try (ThreadContext.StoredContext context = getContext()) {
                XContentParser parser = MediaTypeRegistry.JSON.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        getResponse.getSourceAsString()
                    );
                listener.onResponse(
                    new GetRuleResponse(
                        Map.of(id, Builder.fromXContent(parser, QueryGroupFeatureType.INSTANCE).build()),
                        null,
                        RestStatus.OK
                    )
                );
            } catch (IOException e) {
                logger.error("Error parsing rule with ID {}: {}", id, e.getMessage());
                listener.onFailure(e);
            }
        } else {
            listener.onFailure(new ResourceNotFoundException("Rule with ID " + id + " not found."));
        }
    }

    /**
     * Fetch all rule from system index based on attributeFilters.
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     * @param listener - ActionListener for GetRuleResponse
     */
    private void fetchAllRules(Map<Attribute, Set<String>> attributeFilters, String searchAfter, ActionListener<GetRuleResponse> listener) {
        try (ThreadContext.StoredContext context = getContext()) {
            client.prepareSearch(RULES_INDEX)
                .setSize(0)
                .execute(
                    ActionListener.wrap(countResponse -> handleCountResponse(countResponse, attributeFilters, searchAfter, listener), e -> {
                        logger.error("Failed to check if index is empty: {}", e.getMessage());
                        listener.onFailure(e);
                    })
                );
        }
    }

    /**
     * Processes the count response from a search query on the rules index.
     * If no rules exist, it responds with an empty result.
     * Otherwise, it constructs and executes a search request to retrieve all rules.
     * @param countResponse   The response from the count query on the rules index.
     * @param attributeFilters A map of attribute filters to apply in the search query.
     * @param searchAfter     The searchAfter parameter for pagination.
     * @param listener        The action listener to handle the final response or failure.
     */
    void handleCountResponse(
        SearchResponse countResponse,
        Map<Attribute, Set<String>> attributeFilters,
        String searchAfter,
        ActionListener<GetRuleResponse> listener
    ) {
        try (ThreadContext.StoredContext context = getContext()) {
            if (countResponse.getHits().getTotalHits().value() == 0) {
                listener.onResponse(new GetRuleResponse(new HashMap<>(), null, RestStatus.OK));
                return;
            }
            SearchRequestBuilder searchRequest = buildGetAllRuleSearchRequest(attributeFilters, searchAfter);
            searchRequest.execute(ActionListener.wrap(searchResponse -> handleGetAllRuleResponse(searchResponse, listener), e -> {
                logger.error("Failed to fetch all rules: {}", e.getMessage());
                listener.onFailure(e);
            }));
        }
    }

    /**
     * Builds a search request to retrieve all rules from the rules index, applying attribute-based filters
     * and ensuring that the rules are associated with the query group feature type.
     * @param attributeFilters A map of attributes to their associated set of values used to filter the rules.
     * @param searchAfter      A cursor to enable pagination, used to fetch results after a specific document.
     */
    SearchRequestBuilder buildGetAllRuleSearchRequest(Map<Attribute, Set<String>> attributeFilters, String searchAfter) {
        try (ThreadContext.StoredContext context = getContext()) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
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
            boolQuery.filter(QueryBuilders.existsQuery(QueryGroupFeatureType.NAME));
            SearchRequestBuilder searchRequest = client.prepareSearch(RULES_INDEX)
                .setQuery(boolQuery)
                .setSize(MAX_RETURN_SIZE_ALLOWED_PER_GET_REQUEST)
                .addSort(_ID_STRING, SortOrder.ASC);
            if (searchAfter != null) {
                searchRequest.searchAfter(new Object[] { searchAfter });
            }
            return searchRequest;
        }
    }

    /**
     * Process searchResponse from index and send a GetRuleResponse
     * @param searchResponse - Response received from index
     * @param listener - ActionListener for GetRuleResponse
     */
    void handleGetAllRuleResponse(SearchResponse searchResponse, ActionListener<GetRuleResponse> listener) {
        List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
        try (ThreadContext.StoredContext context = getContext()) {
            Map<String, Rule> ruleMap = hits.stream().map(hit -> {
                String hitId = hit.getId();
                try {
                    XContentParser parser = MediaTypeRegistry.JSON.xContent()
                        .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, hit.getSourceAsString());
                    return Map.entry(hitId, Builder.fromXContent(parser, QueryGroupFeatureType.INSTANCE).build());
                } catch (IOException e) {
                    logger.info(
                        "Issue met when parsing rule from hit, the feature type for rule id {} is probably not query_group: {}",
                        hitId,
                        e.getMessage()
                    );
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            String nextSearchAfter = hits.isEmpty() ? null : hits.get(hits.size() - 1).getId();
            listener.onResponse(new GetRuleResponse(ruleMap, nextSearchAfter, RestStatus.OK));
        }
    }

    private ThreadContext.StoredContext getContext() {
        return client.threadPool().getThreadContext().stashContext();
    }

    private boolean isExistingQueryGroup(String queryGroupId) {
        return clusterService.state().metadata().queryGroups().containsKey(queryGroupId);
    }

    public Client getClient() {
        return client;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    /**
     * Deletes a rule from the system index by ID.
     * @param id - The ID of the rule to delete
     * @param listener - ActionListener for DeleteRuleResponse
     */
    public void deleteRule(String id, ActionListener<DeleteRuleResponse> listener) {
        try (ThreadContext.StoredContext context = getContext()) {
            client.prepareDelete(RULES_INDEX, id)
                .execute(ActionListener.wrap(deleteResponse -> {
                    boolean found = deleteResponse.getResult().getLowercase().equals("deleted");
                    if (found) {
                        listener.onResponse(new DeleteRuleResponse(true, RestStatus.OK));
                    } else {
                        listener.onFailure(new ResourceNotFoundException("Rule with ID " + id + " not found."));
                    }
                }, e -> {
                    logger.error("Failed to delete rule with ID {}: {}", id, e.getMessage());
                    listener.onFailure(e);
                }));
        }
    }
}
