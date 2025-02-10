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
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.plugin.wlm.rule.action.DeleteRuleResponse;
import org.opensearch.wlm.Rule;
import org.opensearch.common.inject.Inject;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.*;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;
import org.opensearch.plugin.wlm.rule.action.GetRuleResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class defines the functions for Rule persistence
 * @opensearch.experimental
 */
public class RulePersistenceService {
    public static final String RULE_INDEX = ".rule";
    private final Client client;
    private static final Logger logger = LogManager.getLogger(RulePersistenceService.class);

    /**
     * Constructor for RulePersistenceService
     * @param client {@link Client} - The client to be used by RulePersistenceService
     */
    @Inject
    public RulePersistenceService(
        final Client client
    ) {
        this.client = client;
    }

    public void createRule(Rule rule, ActionListener<CreateRuleResponse> listener) {
        try {
            IndexRequest indexRequest = new IndexRequest(RULE_INDEX)
                .source(rule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));

            client.index(indexRequest, ActionListener.wrap(
                indexResponse -> {
                    CreateRuleResponse createRuleResponse = new CreateRuleResponse(indexResponse.getId(), rule, RestStatus.OK);
                    listener.onResponse(createRuleResponse);
                },
                e -> {
                    logger.warn("Failed to save Rule object due to error: {}", e.getMessage());
                    listener.onFailure(e);
                }
            ));
        } catch (IOException e) {
            logger.error("Error saving rule to index: " + RULE_INDEX, e);
            listener.onFailure(new RuntimeException("Failed to save rule to index."));
        }
    }

    public void getRule(String id, ActionListener<GetRuleResponse> listener) {
        if (id != null) {
            fetchRuleById(id, listener);
        } else {
            fetchAllRules(listener);
        }
    }

    private void fetchRuleById(String id, ActionListener<GetRuleResponse> listener) {
        client.prepareGet(RULE_INDEX, id).execute(ActionListener.wrap(
            getResponse -> handleGetOneRuleResponse(id, getResponse, listener),
            e -> {
                logger.error("Failed to fetch rule with ID {}: {}", id, e.getMessage());
                listener.onFailure(e);
            }
        ));
    }

    private void handleGetOneRuleResponse(String id, GetResponse getResponse, ActionListener<GetRuleResponse> listener) {
        if (getResponse.isExists()) {
            try {
                XContentParser parser = MediaTypeRegistry.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, getResponse.getSourceAsString());
                Rule rule = Rule.Builder.fromXContent(parser).build();
                listener.onResponse(new GetRuleResponse(Map.of(id, rule), RestStatus.OK));
            } catch (IOException e) {
                logger.error("Error parsing rule with ID {}: {}", id, e.getMessage());
                listener.onFailure(e);
            }
        } else {
            listener.onFailure(new ResourceNotFoundException("Rule with ID " + id + " not found."));
        }
    }

    private void fetchAllRules(ActionListener<GetRuleResponse> listener) {
        client.prepareSearch(RULE_INDEX)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(20)
            .execute(ActionListener.wrap(
                searchResponse -> handleGetAllRuleResponse(searchResponse, listener),
                e -> {
                    logger.error("Failed to fetch all rules: {}", e.getMessage());
                    listener.onFailure(e);
                }
            ));
    }

    private void handleGetAllRuleResponse(SearchResponse searchResponse, ActionListener<GetRuleResponse> listener) {
        Map<String, Rule> ruleMap = Arrays.stream(searchResponse.getHits().getHits())
            .map(hit -> {
                try {
                    XContentParser parser = MediaTypeRegistry.JSON.xContent()
                        .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, hit.getSourceAsString());
                    return Map.entry(hit.getId(), Rule.Builder.fromXContent(parser).build());
                } catch (IOException e) {
                    logger.error("Failed to parse rule from hit: {}", e.getMessage());
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        listener.onResponse(new GetRuleResponse(ruleMap, RestStatus.OK));
    }

    public void deleteRule(String id, ActionListener<DeleteRuleResponse> listener) {
        if (id == null) {
            listener.onFailure(new IllegalArgumentException("Rule ID must not be null"));
            return;
        }

        // Check if the document exists before deleting
        GetRequest getRequest = new GetRequest(RULE_INDEX, id);
        client.get(getRequest, ActionListener.wrap(
            getResponse -> {
                if (!getResponse.isExists()) {
                    listener.onFailure(new ResourceNotFoundException("The rule with id " + id + " doesn't exist"));
                } else {
                    // Proceed with deletion
                    DeleteRequest deleteRequest = new DeleteRequest(RULE_INDEX, id);
                    client.delete(deleteRequest, ActionListener.wrap(
                        deleteResponse -> {
                            boolean acknowledged = deleteResponse.getResult() == DeleteResponse.Result.DELETED;
                            listener.onResponse(new DeleteRuleResponse(acknowledged));
                        },
                        listener::onFailure
                    ));
                }
            },
            listener::onFailure
        ));
    }
}
