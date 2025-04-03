/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.rule.service.RulePersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to create Rule in workload management
 * @opensearch.experimental
 */
public class TransportCreateWlmRuleAction extends HandledTransportAction<CreateRuleRequest, CreateRuleResponse> {

    private final IndexStoredRulePersistenceService rulePersistenceService;

    /**
     * Constructor for TransportCreateWlmRuleAction
     *
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceService - a {@link RulePersistenceService} object
     */
    @Inject
    public TransportCreateWlmRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexStoredRulePersistenceService rulePersistenceService
    ) {
        super(CreateWlmRuleAction.NAME, transportService, actionFilters, CreateRuleRequest::new);
        this.rulePersistenceService = rulePersistenceService;
    }

    @Override
    protected void doExecute(Task task, CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        String queryGroupId = request.getRule().getFeatureValue();
        if (!rulePersistenceService.getClusterService().state().metadata().queryGroups().containsKey(queryGroupId)) {
            listener.onFailure(new ResourceNotFoundException("Couldn't find an existing query group with id: " + queryGroupId));
            return;
        }
        rulePersistenceService.createRule(request, listener);
    }
}
