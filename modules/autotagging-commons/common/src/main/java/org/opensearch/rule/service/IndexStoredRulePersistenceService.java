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
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RuleQueryMapper;
import org.opensearch.rule.RuleUtils;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This class encapsulates the logic to manage the lifecycle of rules at index level
 * @opensearch.experimental
 */
public class IndexStoredRulePersistenceService implements RulePersistenceService {
    /**
     * Default value for max rules count
     */
    public static final int DEFAULT_MAX_ALLOWED_RULE_COUNT = 200;

    /**
     *  max wlm rules setting name
     */
    public static final String MAX_RULES_COUNT_SETTING_NAME = "wlm.autotagging.max_rules";

    /**
     *  max wlm rules setting
     */
    public static final Setting<Integer> MAX_WLM_RULES_SETTING = Setting.intSetting(
        MAX_RULES_COUNT_SETTING_NAME,
        DEFAULT_MAX_ALLOWED_RULE_COUNT,
        10,
        500,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private int maxAllowedRulesCount;
    private final String indexName;
    private final Client client;
    private final ClusterService clusterService;
    private final int maxRulesPerPage;
    private final RuleEntityParser parser;
    private final RuleQueryMapper<QueryBuilder> queryBuilder;
    private static final Logger logger = LogManager.getLogger(IndexStoredRulePersistenceService.class);

    /**
     * Constructs an instance of {@link IndexStoredRulePersistenceService} with the specified parameters.
     * This service handles persistence and retrieval of stored rules within an OpenSearch index.
     * @param indexName - The name of the OpenSearch index where the rules are stored.
     * @param client - The OpenSearch client used to interact with the OpenSearch cluster.
     * @param clusterService
     * @param maxRulesPerPage - The maximum number of rules that can be returned in a single get request.
     * @param parser
     * @param queryBuilder
     */
    public IndexStoredRulePersistenceService(
        String indexName,
        Client client,
        ClusterService clusterService,
        int maxRulesPerPage,
        RuleEntityParser parser,
        RuleQueryMapper<QueryBuilder> queryBuilder
    ) {
        this.indexName = indexName;
        this.client = client;
        this.clusterService = clusterService;
        this.maxRulesPerPage = maxRulesPerPage;
        this.parser = parser;
        this.queryBuilder = queryBuilder;
        this.maxAllowedRulesCount = MAX_WLM_RULES_SETTING.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_WLM_RULES_SETTING, this::setMaxAllowedRules);
    }

    private void setMaxAllowedRules(int maxAllowedRules) {
        this.maxAllowedRulesCount = maxAllowedRules;
    }

    /**
     * Entry point for the create rule API logic in persistence service.
     * It ensures the index exists, validates for duplicate rules, and persists the new rule.
     * @param request  The CreateRuleRequest
     * @param listener ActionListener for CreateRuleResponse
     */
    public void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        try (ThreadContext.StoredContext ctx = stashContext()) {
            if (!clusterService.state().metadata().hasIndex(indexName)) {
                logger.error("Index {} does not exist", indexName);
                listener.onFailure(new IllegalStateException("Index" + indexName + " does not exist"));
            } else {
                performCardinalityCheck(listener);
                Rule rule = request.getRule();
                validateNoDuplicateRule(rule, ActionListener.wrap(unused -> persistRule(rule, listener), listener::onFailure));
            }
        }
    }

    private void performCardinalityCheck(ActionListener<CreateRuleResponse> listener) {
        SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(queryBuilder.getCardinalityQuery()).get();
        if (searchResponse.getHits().getTotalHits() != null && searchResponse.getHits().getTotalHits().value() >= maxAllowedRulesCount) {
            listener.onFailure(
                new OpenSearchRejectedExecutionException(
                    "This create operation will violate"
                        + " the cardinality limit of "
                        + DEFAULT_MAX_ALLOWED_RULE_COUNT
                        + ". Please delete some stale or redundant rules first"
                )
            );
        }
    }

    /**
     * Validates that no existing rule has the same attribute map as the given rule.
     * This validation must be performed one at a time to prevent writing duplicate rules.
     * @param rule - the rule we check duplicate against
     * @param listener - listener for validateNoDuplicateRule response
     */
    private void validateNoDuplicateRule(Rule rule, ActionListener<Void> listener) {
        Map<String, Set<String>> attributeFilters = RuleUtils.buildAttributeFilters(rule);
        QueryBuilder query = queryBuilder.from(new GetRuleRequest(null, attributeFilters, null, rule.getFeatureType()));
        getRuleFromIndex(null, query, null, new ActionListener<>() {
            @Override
            public void onResponse(GetRuleResponse getRuleResponse) {
                Optional<String> duplicateRuleId = RuleUtils.getDuplicateRuleId(rule, getRuleResponse.getRules());
                duplicateRuleId.ifPresentOrElse(
                    id -> listener.onFailure(new IllegalArgumentException("Duplicate rule exists under id " + id)),
                    () -> listener.onResponse(null)
                );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Persist the rule in the index
     * @param rule - The rule to update.
     * @param listener - ActionListener for CreateRuleResponse
     */
    private void persistRule(Rule rule, ActionListener<CreateRuleResponse> listener) {
        try {
            IndexRequest indexRequest = new IndexRequest(indexName).id(rule.getId())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(rule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
            client.index(indexRequest).get();
            listener.onResponse(new CreateRuleResponse(rule));
        } catch (Exception e) {
            logger.error("Error saving rule to index: {}", indexName);
            listener.onFailure(new RuntimeException("Failed to save rule to index."));
        }
    }

    /**
     * Entry point for the get rule api logic in persistence service.
     * @param getRuleRequest the getRuleRequest to process.
     * @param listener the listener for GetRuleResponse.
     */
    public void getRule(GetRuleRequest getRuleRequest, ActionListener<GetRuleResponse> listener) {
        try (ThreadContext.StoredContext context = stashContext()) {
            final QueryBuilder getQueryBuilder = queryBuilder.from(getRuleRequest);
            getRuleFromIndex(getRuleRequest.getId(), getQueryBuilder, getRuleRequest.getSearchAfter(), listener);
        }
    }

    /**
     * Get rules from index. If id is provided, we only get a single rule.
     * Otherwise, we get all rules that satisfy the attributeFilters.
     * @param queryBuilder query object
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     * @param listener - ActionListener for GetRuleResponse
     */
    private void getRuleFromIndex(String id, QueryBuilder queryBuilder, String searchAfter, ActionListener<GetRuleResponse> listener) {
        try {
            SearchRequestBuilder searchRequest = client.prepareSearch(indexName).setQuery(queryBuilder).setSize(maxRulesPerPage);
            if (searchAfter != null) {
                searchRequest.addSort("_id", SortOrder.ASC).searchAfter(new Object[] { searchAfter });
            }

            SearchResponse searchResponse = searchRequest.get();
            List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
            if (hasNoResults(id, listener, hits)) return;
            handleGetRuleResponse(hits, listener);
        } catch (Exception e) {
            if (e instanceof IndexNotFoundException) {
                logger.debug("Failed to get rule from index [{}]: index doesn't exist.", indexName);
                handleGetRuleResponse(new ArrayList<>(), listener);
                return;
            }
            logger.error("Failed to fetch all rules: {}", e.getMessage());
            listener.onFailure(e);
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
        List<Rule> ruleList = hits.stream().map(hit -> parser.parse(hit.getSourceAsString())).toList();
        String nextSearchAfter = hits.isEmpty() || hits.size() < maxRulesPerPage ? null : hits.get(hits.size() - 1).getId();
        listener.onResponse(new GetRuleResponse(ruleList, nextSearchAfter));
    }

    @Override
    public void deleteRule(DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener) {
        try (ThreadContext.StoredContext context = stashContext()) {
            DeleteRequest deleteRequest = new DeleteRequest(indexName).id(request.getRuleId());
            client.delete(deleteRequest, ActionListener.wrap(deleteResponse -> {
                boolean acknowledged = deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                if (!acknowledged) {
                    logger.warn("Rule with ID " + request.getRuleId() + " was not found or already deleted.");
                }
                listener.onResponse(new AcknowledgedResponse(acknowledged));
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

    /**
     * Entry point for the update rule api logic in persistence service.
     * @param request - The UpdateRuleRequest
     * @param listener - ActionListener for UpdateRuleResponse
     */
    public void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        String ruleId = request.getId();
        FeatureType featureType = request.getFeatureType();
        try (ThreadContext.StoredContext context = stashContext()) {
            QueryBuilder query = queryBuilder.from(new GetRuleRequest(ruleId, new HashMap<>(), null, featureType));
            getRuleFromIndex(ruleId, query, null, new ActionListener<>() {
                @Override
                public void onResponse(GetRuleResponse getRuleResponse) {
                    if (getRuleResponse == null || getRuleResponse.getRules().isEmpty()) {
                        listener.onFailure(new ResourceNotFoundException("Rule with ID " + ruleId + " not found."));
                        return;
                    }
                    List<Rule> ruleList = getRuleResponse.getRules();
                    assert ruleList.size() == 1;
                    Rule updatedRule = RuleUtils.composeUpdatedRule(ruleList.get(0), request, featureType);
                    validateNoDuplicateRule(
                        updatedRule,
                        ActionListener.wrap(unused -> persistUpdatedRule(ruleId, updatedRule, listener), listener::onFailure)
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
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, ruleId).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .doc(updatedRule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
            client.update(updateRequest).get();
            listener.onResponse(new UpdateRuleResponse(updatedRule));
        } catch (Exception e) {
            logger.error("Error updating rule in index: {}", indexName);
            listener.onFailure(new RuntimeException("Failed to update rule to index."));
        }
    }

    private ThreadContext.StoredContext stashContext() {
        return client.threadPool().getThreadContext().stashContext();
    }
}
