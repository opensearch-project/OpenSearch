/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.service.RulePersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to get workload management Rules
 * @opensearch.experimental
 */
public class TransportGetWlmRuleAction extends HandledTransportAction<GetRuleRequest, GetRuleResponse> {

    private final RulePersistenceService rulePersistenceService;

    /**
     * Constructor for TransportGetWlmRuleAction
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceService - a {@link RulePersistenceService} object
     */
    @Inject
    public TransportGetWlmRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RulePersistenceService rulePersistenceService
    ) {
        super(GetWlmRuleAction.NAME, transportService, actionFilters, GetRuleRequest::new);
        this.rulePersistenceService = rulePersistenceService;
    }

    @Override
    protected void doExecute(Task task, GetRuleRequest request, ActionListener<GetRuleResponse> listener) {
        rulePersistenceService.getRule(request, listener);
    }
}
