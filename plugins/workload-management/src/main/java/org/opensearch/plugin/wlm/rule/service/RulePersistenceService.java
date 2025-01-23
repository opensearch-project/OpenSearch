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
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;
import org.opensearch.wlm.Rule;

import java.io.IOException;
import java.util.Map;

/**
 * This class defines the functions for Rule persistence
 * @opensearch.experimental
 */
public class RulePersistenceService {
    public static final String RULE_INDEX = ".rule";
    private final Client client;
    private final ClusterService clusterService;
    private static final Logger logger = LogManager.getLogger(RulePersistenceService.class);

    /**
     * Constructor for RulePersistenceService
     * @param client {@link Client} - The client to be used by RulePersistenceService
     */
    @Inject
    public RulePersistenceService(final ClusterService clusterService, final Client client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * This method is the entry point for create rule logic in persistence service.
     * @param rule - the rule that we're persisting
     * @param listener - ActionListener for CreateRuleResponse
     */
    public void createRule(Rule rule, ActionListener<CreateRuleResponse> listener) {
        createRuleIndexIfAbsent(new ActionListener<>() {
            @Override
            public void onResponse(Boolean indexCreated) {
                persistRule(rule, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * This method handles the core logic to save a rule to the system index.
     * @param rule - the rule that we're persisting
     * @param listener - ActionListener for CreateRuleResponse
     */
    void persistRule(Rule rule, ActionListener<CreateRuleResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            IndexRequest indexRequest = new IndexRequest(RULE_INDEX).source(
                rule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            );

            client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                CreateRuleResponse createRuleResponse = new CreateRuleResponse(indexResponse.getId(), rule, RestStatus.OK);
                listener.onResponse(createRuleResponse);
            }, e -> {
                logger.warn("Failed to save Rule object due to error: {}", e.getMessage());
                listener.onFailure(e);
            }));
        } catch (IOException e) {
            logger.warn("Error saving rule to index: {}", e.getMessage());
            listener.onFailure(new RuntimeException("Failed to save rule to index."));
        }
    }

    /**
     * This method creates the rule system index if it's absent.
     * @param listener - ActionListener for CreateRuleResponse
     */
    void createRuleIndexIfAbsent(ActionListener<Boolean> listener) {
        if (clusterService.state().metadata().hasIndex(RulePersistenceService.RULE_INDEX)) {
            listener.onResponse(true);
            return;
        }
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            final Map<String, Object> indexSettings = Map.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");
            final CreateIndexRequest createIndexRequest = new CreateIndexRequest(RulePersistenceService.RULE_INDEX).settings(indexSettings);
            client.admin().indices().create(createIndexRequest, new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    logger.info("Index {} created?: {}", RulePersistenceService.RULE_INDEX, response.isAcknowledged());
                    listener.onResponse(response.isAcknowledged());
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        logger.info("Index {} already exists", RulePersistenceService.RULE_INDEX);
                        listener.onResponse(true);
                    } else {
                        logger.error("Failed to create index {}: {}", RulePersistenceService.RULE_INDEX, e.getMessage());
                        listener.onFailure(e);
                    }
                }
            });
        }
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
