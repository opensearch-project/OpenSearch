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
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.utils.IndexStoredRuleParser;
import org.opensearch.rule.utils.IndexStoredRuleUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final ClusterService clusterService;
    private final Client client;
    private final FeatureType featureType;
    private final int maxRulesPerPage;
    private static final Logger logger = LogManager.getLogger(IndexStoredRulePersistenceService.class);

    /**
     * Constructs an instance of {@link IndexStoredRulePersistenceService} with the specified parameters.
     * This service handles persistence and retrieval of stored rules within an OpenSearch index.
     * @param indexName - The name of the OpenSearch index where the rules are stored.
     * @param clusterService - The clusterService used in IndexStoredRulePersistenceService.
     * @param client - The OpenSearch client used to interact with the OpenSearch cluster.
     * @param featureType - The feature type associated with the stored rules.
     * @param maxRulesPerPage - The maximum number of rules that can be returned in a single get request.
     */
    public IndexStoredRulePersistenceService(
        String indexName,
        ClusterService clusterService,
        Client client,
        FeatureType featureType,
        int maxRulesPerPage
    ) {
        this.indexName = indexName;
        this.clusterService = clusterService;
        this.client = client;
        this.featureType = featureType;
        this.maxRulesPerPage = maxRulesPerPage;
    }

    /**
     * Entry point for the update rule api logic in persistence service.
     * @param request - The UpdateRuleRequest
     * @param listener - ActionListener for UpdateRuleResponse
     */
    public void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        String ruleId = request.get_id();
        try (ThreadContext.StoredContext context = getContext()) {
            getRuleFromIndex(ruleId, new HashMap<>(), null, new ActionListener<>() {
                @Override
                public void onResponse(GetRuleResponse getRuleResponse) {
                    if (getRuleResponse == null || getRuleResponse.getRules().isEmpty()) {
                        listener.onFailure(new ResourceNotFoundException("Rule with ID " + ruleId + " not found."));
                        return;
                    }
                    Rule updatedRule = IndexStoredRuleUtils.composeUpdatedRule(
                        getRuleResponse.getRules().get(ruleId),
                        request,
                        featureType
                    );
                    validateNoDuplicateRule(updatedRule, new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            persistUpdatedRule(ruleId, updatedRule, listener);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    /**
     * Validates that no duplicate rule exists with the same attribute map.
     * If a conflict is found, fails the listener
     * @param rule - the rule we check duplicate against
     * @param listener - listener for validateNoDuplicateRule response
     */
    private void validateNoDuplicateRule(Rule rule, ActionListener<Void> listener) {
        try (ThreadContext.StoredContext ctx = getContext()) {
            getRuleFromIndex(null, rule.getAttributeMap(), null, new ActionListener<>() {
                @Override
                public void onResponse(GetRuleResponse getRuleResponse) {
                    Optional<String> duplicateRuleId = IndexStoredRuleUtils.getDuplicateRuleId(rule, getRuleResponse.getRules());
                    duplicateRuleId.ifPresentOrElse(
                        id -> listener.onFailure(new IllegalArgumentException("Rule already exists under rule id " + id)),
                        () -> listener.onResponse(null)
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    /**
     * Persist the updated rule in index
     * @param ruleId - the rule id to update
     * @param updatedRule - the rule we update to
     * @param listener - ActionListener for UpdateRuleResponse
     */
    private void persistUpdatedRule(String ruleId, Rule updatedRule, ActionListener<UpdateRuleResponse> listener) {
        try (ThreadContext.StoredContext context = getContext()) {
            UpdateRequest updateRequest = new UpdateRequest(indexName, ruleId).doc(
                updatedRule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            );
            client.update(
                updateRequest,
                ActionListener.wrap(updateResponse -> { listener.onResponse(new UpdateRuleResponse(ruleId, updatedRule)); }, e -> {
                    logger.error("Failed to update Rule object due to error: {}", e.getMessage());
                    listener.onFailure(e);
                })
            );
        } catch (IOException e) {
            logger.error("Error updating rule in index: {}", indexName);
            listener.onFailure(new RuntimeException("Failed to update rule to index."));
        }
    }

    /**
     * Entry point for the get rule api logic in persistence service. If id is provided, we only get a single rule.
     * Otherwise, we get all rules that satisfy the attributeFilters.
     * @param id - The id of the rule to get.
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     * @param listener - ActionListener for GetRuleResponse
     */
    private void getRuleFromIndex(
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
    private void handleGetRuleResponse(String id, SearchResponse searchResponse, ActionListener<GetRuleResponse> listener) {
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

    /**
     * clusterService getter
     */
    public ClusterService getClusterService() {
        return clusterService;
    }
}
