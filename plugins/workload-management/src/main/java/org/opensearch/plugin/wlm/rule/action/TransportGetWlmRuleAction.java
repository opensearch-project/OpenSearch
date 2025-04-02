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
import org.opensearch.rule.service.RuleService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to get workload management Rules
 * @opensearch.experimental
 */
public class TransportGetWlmRuleAction extends HandledTransportAction<GetWlmRuleRequest, GetWlmRuleResponse> {

    private final RuleService ruleService;

    /**
     * Constructor for TransportGetWlmRuleAction
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param ruleService - a {@link RuleService} object
     */
    @Inject
    public TransportGetWlmRuleAction(TransportService transportService, ActionFilters actionFilters, RuleService ruleService) {
        super(GetWlmRuleAction.NAME, transportService, actionFilters, GetWlmRuleRequest::new);
        this.ruleService = ruleService;
    }

    @Override
    protected void doExecute(Task task, GetWlmRuleRequest request, ActionListener<GetWlmRuleResponse> listener) {
        ruleService.processGetRuleRequest(request, listener);
    }
}
