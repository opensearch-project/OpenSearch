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
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
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
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.utils.IndexStoredRuleParser;
import org.opensearch.rule.utils.IndexStoredRuleUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Arrays;
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
    private static final Map<String, Object> indexSettings = Map.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");

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
     * Entry point for the create rule API logic in persistence service.
     * It ensures the index exists, validates for duplicate rules, and persists the new rule.
     * @param request  The CreateRuleRequest
     * @param listener ActionListener for CreateRuleResponse
     */
    public void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        try (ThreadContext.StoredContext ctx = getContext()) {
            createIndexIfAbsent(new ActionListener<>() {
                @Override
                public void onResponse(Boolean indexCreated) {
                    if (!indexCreated) {
                        listener.onFailure(new IllegalStateException(indexName + " index creation failed and rule cannot be persisted"));
                        return;
                    }
                    validateNoDuplicateRule(request.getRule(), new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            persistRule(request.getRule(), listener);
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
     * Persist the rule in the index
     * @param rule - The rule to update.
     * @param listener - ActionListener for CreateRuleResponse
     */
    private void persistRule(Rule rule, ActionListener<CreateRuleResponse> listener) {
        try (ThreadContext.StoredContext ctx = getContext()) {
            IndexRequest indexRequest = new IndexRequest(indexName).source(
                rule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            );
            client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                listener.onResponse(new CreateRuleResponse(indexResponse.getId(), rule));
            }, e -> {
                logger.warn("Failed to save Rule object due to error: {}", e.getMessage());
                listener.onFailure(e);
            }));
        } catch (IOException e) {
            logger.error("Error saving rule to index: {}", indexName);
            listener.onFailure(new RuntimeException("Failed to save rule to index."));
        }
    }

    /**
     * Creates the system index if it doesn't exist
     * @param listener - ActionListener for CreateRuleResponse
     */
    private void createIndexIfAbsent(ActionListener<Boolean> listener) {
        try (ThreadContext.StoredContext ctx = getContext()) {
            if (clusterService.state().metadata().hasIndex(indexName)) {
                listener.onResponse(true);
                return;
            }
            final CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(indexSettings);
            client.admin().indices().create(createIndexRequest, new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    logger.info("Index {} created?: {}", indexName, response.isAcknowledged());
                    listener.onResponse(response.isAcknowledged());
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        logger.trace("Index {} already exists", indexName);
                        listener.onResponse(true);
                    } else {
                        logger.error("Failed to create index {}: {}", indexName, e.getMessage());
                        listener.onFailure(e);
                    }
                }
            });
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
            listener.onFailure(new ResourceNotFoundException("Rule with ID " + id + " doesn't exist in the index."));
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
