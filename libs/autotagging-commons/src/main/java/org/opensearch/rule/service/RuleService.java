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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.autotagging.Rule;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;

import java.util.Map;

/**
 * Service class responsible for processing the business logic for rule lifecycle operations
 * @opensearch.experimental
 */
public class RuleService {
    private final RulePersistenceService rulePersistenceService;
    private final RuleProcessingService ruleProcessingService;
    private final RuleResponseBuilder ruleResponseBuilder;
    private final Logger logger = LogManager.getLogger(RuleService.class);

    /**
     * Constructs a {@link RuleService} with the specified dependencies.
     * @param rulePersistenceService The persistence service for retrieving and storing rules.
     * @param ruleProcessingService  The service responsible for processing rule data.
     * @param ruleResponseBuilder    The builder used to construct rule response objects.
     */
    @Inject
    public RuleService(
        RulePersistenceService rulePersistenceService,
        RuleProcessingService ruleProcessingService,
        RuleResponseBuilder ruleResponseBuilder
    ) {
        this.rulePersistenceService = rulePersistenceService;
        this.ruleProcessingService = ruleProcessingService;
        this.ruleResponseBuilder = ruleResponseBuilder;
    }

    /**
     * Processes a request to retrieve rules based on specified criteria.
     * @param request  The {@link GetRuleRequest} containing rule retrieval parameters.
     * @param listener The {@link ActionListener} that handles the asynchronous response.
     */
    public void processGetRuleRequest(GetRuleRequest request, ActionListener<? extends GetRuleResponse> listener) {
        rulePersistenceService.getRule(request.getId(), request.getAttributeFilters(), request.getSearchAfter(), new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Tuple<Map<String, Rule>, String> responseParams = ruleProcessingService.parseGetRuleResponse(
                    request.getId(),
                    searchResponse
                );
                listener.onResponse(ruleResponseBuilder.buildGetRuleResponse(responseParams.v1(), responseParams.v2(), RestStatus.OK));
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to fetch rules: {}", e.getMessage());
                listener.onFailure(e);
            }
        });
    }
}
