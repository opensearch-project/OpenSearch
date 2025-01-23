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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.wlm.rule.action.GetRuleResponse;
import org.opensearch.wlm.Rule;
import org.opensearch.wlm.Rule.Builder;
import org.opensearch.wlm.Rule.RuleAttribute;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class defines the functions for Rule persistence
 * @opensearch.experimental
 */
public class RulePersistenceService {
    public static final String RULE_INDEX = ".rule";
    private final Client client;
    private static final Logger logger = LogManager.getLogger(RulePersistenceService.class);
    private static final int MAX_RETURN_SIZE_ALLOWED_PER_GET_REQUEST = 50;

    /**
     * Constructor for RulePersistenceService
     * @param client {@link Client} - The client to be used by RulePersistenceService
     */
    @Inject
    public RulePersistenceService(final Client client) {
        this.client = client;
    }

    /**
     * Entry point for the get rule api logic in persistence service.
     * @param id - The id of the rule to get. Get all matching rules when id is null
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param listener - ActionListener for GetRuleResponse
     */
    public void getRule(String id, Map<RuleAttribute, Set<String>> attributeFilters, ActionListener<GetRuleResponse> listener) {
        if (id != null) {
            fetchRuleById(id, listener);
        } else {
            fetchAllRules(attributeFilters, listener);
        }
    }

    /**
     * Fetch a single rule from system index using id
     * @param id - The id of the rule to get.
     * @param listener - ActionListener for GetRuleResponse
     */
    private void fetchRuleById(String id, ActionListener<GetRuleResponse> listener) {
        ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext().stashContext();
        client.prepareGet(RULE_INDEX, id)
            .execute(ActionListener.wrap(
                getResponse -> {
                    try (ThreadContext.StoredContext context = storedContext) {
                        handleGetOneRuleResponse(id, getResponse, listener);
                    }
                },
                e -> {
                    try (ThreadContext.StoredContext context = storedContext) {
                        logger.error("Failed to fetch rule with ID {}: {}", id, e.getMessage());
                        listener.onFailure(e);
                    }
                }
            ));
    }

    /**
     * Process getResponse from index and send a GetRuleResponse
     * @param id - The id of the rule to get
     * @param getResponse - Response received from index
     * @param listener - ActionListener for GetRuleResponse
     */
    private void handleGetOneRuleResponse(String id, GetResponse getResponse, ActionListener<GetRuleResponse> listener) {
        if (getResponse.isExists()) {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                XContentParser parser = MediaTypeRegistry.JSON.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        getResponse.getSourceAsString()
                    );
                listener.onResponse(new GetRuleResponse(Map.of(id, Builder.fromXContent(parser).build()), RestStatus.OK));
            } catch (IOException e) {
                logger.error("Error parsing rule with ID {}: {}", id, e.getMessage());
                listener.onFailure(e);
            }
        } else {
            listener.onFailure(new ResourceNotFoundException("Rule with ID " + id + " not found."));
        }
    }

    /**
     * Fetch all rule from system index based on attributeFilters
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param listener - ActionListener for GetRuleResponse
     */
    private void fetchAllRules(Map<RuleAttribute, Set<String>> attributeFilters, ActionListener<GetRuleResponse> listener) {
        ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext().stashContext();
        client.prepareSearch(RULE_INDEX)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(MAX_RETURN_SIZE_ALLOWED_PER_GET_REQUEST)
            .execute(ActionListener.wrap(
                searchResponse -> {
                    try (ThreadContext.StoredContext context = storedContext) {
                        handleGetAllRuleResponse(searchResponse, attributeFilters, listener);
                    }
                },
                e -> {
                    try (ThreadContext.StoredContext context = storedContext) {
                        logger.error("Failed to fetch all rules: {}", e.getMessage());
                        listener.onFailure(e);
                    }
                }
            ));
    }

    /**
     * Process searchResponse from index and send a GetRuleResponse
     * @param searchResponse - Response received from index
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param listener - ActionListener for GetRuleResponse
     */
    private void handleGetAllRuleResponse(
        SearchResponse searchResponse,
        Map<RuleAttribute, Set<String>> attributeFilters,
        ActionListener<GetRuleResponse> listener
    ) {
        Map<String, Rule> ruleMap = Arrays.stream(searchResponse.getHits().getHits()).map(hit -> {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                XContentParser parser = MediaTypeRegistry.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, hit.getSourceAsString());
                Rule currRule = Rule.Builder.fromXContent(parser).build();
                if (matchesFilters(currRule, attributeFilters)) {
                    return Map.entry(hit.getId(), currRule);
                }
                return null;
            } catch (IOException e) {
                logger.error("Failed to parse rule from hit: {}", e.getMessage());
                listener.onFailure(e);
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        listener.onResponse(new GetRuleResponse(ruleMap, RestStatus.OK));
    }

    /**
     * Returns true if the rule matches the attributeFilters and should be included in the response
     * @param rule - the rule to be checked against the attribute filters
     * @param attributeFilters - A map containing the attributes that user want to filter on
     */
    private boolean matchesFilters(Rule rule, Map<RuleAttribute, Set<String>> attributeFilters) {
        for (Map.Entry<RuleAttribute, Set<String>> entry : attributeFilters.entrySet()) {
            RuleAttribute attribute = entry.getKey();
            Set<String> expectedValues = entry.getValue();
            Set<String> ruleValues = rule.getAttributeMap().get(attribute);
            if (ruleValues == null || ruleValues.stream().noneMatch(expectedValues::contains)) {
                return false;
            }
        }
        return true;
    }

    /**
     * client getter
     */
    public Client getClient() {
        return client;
    }
}
