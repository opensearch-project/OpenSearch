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
import org.opensearch.plugin.wlm.rule.service.RulePersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to delete a Rule.
 * @opensearch.experimental
 */
public class TransportDeleteRuleAction extends HandledTransportAction<DeleteRuleRequest, DeleteRuleResponse> {

    private final RulePersistenceService rulePersistenceService;

    @Inject
    public TransportDeleteRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RulePersistenceService rulePersistenceService
    ) {
        super(DeleteRuleAction.NAME, transportService, actionFilters, DeleteRuleRequest::new);
        this.rulePersistenceService = rulePersistenceService;
    }

    @Override
    protected void doExecute(Task task, DeleteRuleRequest request, ActionListener<DeleteRuleResponse> listener) {
        rulePersistenceService.deleteRule(request.getId(), listener);
    }
}

