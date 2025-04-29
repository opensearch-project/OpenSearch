/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.UpdateRuleRequest;
import org.opensearch.rule.UpdateRuleResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to update Rules
 * @opensearch.experimental
 */
public class TransportUpdateRuleAction extends HandledTransportAction<UpdateRuleRequest, UpdateRuleResponse> {

    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;

    /**
     * Constructor for TransportUpdateRuleAction
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     */
    @Inject
    public TransportUpdateRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry
    ) {
        super(UpdateRuleAction.NAME, transportService, actionFilters, UpdateRuleRequest::new);
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;
    }

    @Override
    protected void doExecute(Task task, UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        final RulePersistenceService rulePersistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(
            request.getFeatureType()
        );
        rulePersistenceService.updateRule(request, listener);
    }
}
