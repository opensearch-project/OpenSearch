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
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.autotagging.Rule;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;

import java.io.IOException;
import java.util.Map;

/**
 * This class encapsulates the logic to manage the lifecycle of rules at index level
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

    public void createRule(Rule<QueryGroupFeatureType> rule, ActionListener<CreateRuleResponse> listener) {
        String queryGroupId = rule.getLabel();
        if (!clusterService.state().metadata().queryGroups().containsKey(queryGroupId)) {
            listener.onFailure(new ResourceNotFoundException("Couldn't find an existing query group with id: " + queryGroupId));
        }

        final Map<String, Object> indexSettings = Map.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");
        createIfAbsent(indexSettings, new ActionListener<>() {
            @Override
            public void onResponse(Boolean indexCreated) {
                if (!indexCreated) {
                    listener.onFailure(new IllegalStateException(RULE_INDEX + " index creation failed and rule cannot be persisted"));
                    return;
                }
                persistRule(rule, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public void persistRule(Rule<QueryGroupFeatureType> rule, ActionListener<CreateRuleResponse> listener) {
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
            logger.error("Error saving rule to index: {}", RULE_INDEX, e);
            listener.onFailure(new RuntimeException("Failed to save rule to index."));
        }
    }

    private void createIfAbsent(Map<String, Object> indexSettings, ActionListener<Boolean> listener) {
        if (clusterService.state().metadata().hasIndex(RulePersistenceService.RULE_INDEX)) {
            listener.onResponse(true);
            return;
        }
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
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

    public Client getClient() {
        return client;
    }
}
