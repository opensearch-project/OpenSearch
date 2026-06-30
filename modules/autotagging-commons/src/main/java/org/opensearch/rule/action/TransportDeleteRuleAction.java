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
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to delete Rules
 * @opensearch.experimental
 */
public class TransportDeleteRuleAction extends HandledTransportAction<DeleteRuleRequest, AcknowledgedResponse> {

    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;

    /**
     * Constructor for TransportDeleteRuleAction
     *
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     */
    @Inject
    public TransportDeleteRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry
    ) {
        super(DeleteRuleAction.NAME, transportService, actionFilters, DeleteRuleRequest::new);
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;
    }

    @Override
    protected void doExecute(Task task, DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener) {
        RulePersistenceService rulePersistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(request.getFeatureType());
        rulePersistenceService.deleteRule(request, listener);
    }
}
