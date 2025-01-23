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
 * Transport action to create Rule
 * @opensearch.experimental
 */
public class TransportCreateRuleAction extends HandledTransportAction<CreateRuleRequest, CreateRuleResponse> {

    private final RulePersistenceService rulePersistenceService;

    /**
     * Constructor for TransportCreateRuleAction
     *
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceService - a {@link RulePersistenceService} object
     */
    @Inject
    public TransportCreateRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RulePersistenceService rulePersistenceService
    ) {
        super(CreateRuleAction.NAME, transportService, actionFilters, CreateRuleRequest::new);
        this.rulePersistenceService = rulePersistenceService;
    }

    @Override
    protected void doExecute(Task task, CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        rulePersistenceService.createRule(request.getRule(), listener);
    }
}
