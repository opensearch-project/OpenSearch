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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.service.RulePersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to update Rule in workload management
 * @opensearch.experimental
 */
public class TransportUpdateWlmRuleAction extends HandledTransportAction<UpdateRuleRequest, UpdateRuleResponse> {

    private final ClusterService clusterService;
    private final RulePersistenceService rulePersistenceService;

    /**
     * Constructor for TransportUpdateWlmRuleAction
     *
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param clusterService - a {@link ClusterService} object}
     * @param rulePersistenceService - a {@link RulePersistenceService} object
     */
    @Inject
    public TransportUpdateWlmRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        RulePersistenceService rulePersistenceService
    ) {
        super(UpdateWlmRuleAction.NAME, transportService, actionFilters, UpdateRuleRequest::new);
        this.clusterService = clusterService;
        this.rulePersistenceService = rulePersistenceService;
    }

    @Override
    protected void doExecute(Task task, UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        String queryGroupId = request.getFeatureValue();
        if (queryGroupId != null && !clusterService.state().metadata().queryGroups().containsKey(queryGroupId)) {
            listener.onFailure(new ResourceNotFoundException("Couldn't find an existing query group with id: " + queryGroupId));
            return;
        }
        rulePersistenceService.updateRule(request, listener);
    }
}
