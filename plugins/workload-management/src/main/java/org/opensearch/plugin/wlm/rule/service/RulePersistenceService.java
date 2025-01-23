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
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.Rule;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;

import java.io.IOException;

/**
 * This class defines the functions for Rule persistence
 */
public class RulePersistenceService {
    static final String SOURCE = "rule-persistence-service";
    public static final String RULE_INDEX = ".rule";
    private final Client client;
    private static final Logger logger = LogManager.getLogger(RulePersistenceService.class);

    /**
     * Constructor for RulePersistenceService
     *
     * @param client {@link Client} - The client to be used by RulePersistenceService
     */
    @Inject
    public RulePersistenceService(
        final Client client
    ) {
        this.client = client;
    }

//    public void createRule(Rule rule, ActionListener<CreateRuleResponse> listener) {
//        boolean indexExists = client.admin().indices().prepareExists(RULE_INDEX).get().isExists();
//        if (!indexExists) {
//            createRuleIndex(listener);
//        }
//        saveRuleToIndex(rule, listener);
//    }

    public void createRule(Rule rule, ActionListener<CreateRuleResponse> listener) {
//        client.admin().indices().prepareExists(RULE_INDEX).execute(new ActionListener<>() {
//            @Override
//            public void onResponse(IndicesExistsResponse indicesExistsResponse) {
//                if (!indicesExistsResponse.isExists()) {
//                    createRuleIndex(ActionListener.wrap(
//                        ignored -> saveRuleToIndex(rule, listener),
//                        listener::onFailure
//                    ));
//                } else {
//                    saveRuleToIndex(rule, listener);
//                }
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                logger.error("Failed to check if index exists: {}", RULE_INDEX, e);
//                listener.onFailure(e);
//            }
//        });
        saveRuleToIndex(rule, listener);
    }

    private void createRuleIndex(ActionListener<CreateRuleResponse> listener) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(RULE_INDEX)
            .settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1));

        client.admin().indices().create(createIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(CreateIndexResponse response) {
                if (response.isAcknowledged()) {
                    logger.info("Successfully created index: " + RULE_INDEX);
                    listener.onResponse(null);
                } else {
                    logger.error("Failed to create index: " + RULE_INDEX);
                    listener.onFailure(new RuntimeException("Failed to create index: " + RULE_INDEX));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Error creating index: " + RULE_INDEX, e);
                listener.onFailure(e);
            }
        });
    }

    private void saveRuleToIndex(Rule rule, ActionListener<CreateRuleResponse> listener) {
        try {
            IndexRequest indexRequest = new IndexRequest(RULE_INDEX)
                .id(rule.get_id())

                .source(rule.toXContentWithoutId(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));

            client.index(indexRequest, ActionListener.wrap(
                indexResponse -> {
                    CreateRuleResponse createRuleResponse = new CreateRuleResponse(rule, RestStatus.OK);
                    listener.onResponse(createRuleResponse);
                },
                e -> {
                    logger.warn("failed to save Rule object due to error: {}", e.getMessage());
                    listener.onFailure(e);
                }
            ));
        } catch (IOException e) {
            logger.error("Error saving rule to index: " + RULE_INDEX, e);
            listener.onFailure(new RuntimeException("Failed to save rule to index."));
        }
    }
}
