/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for delete QueryGroup
 *
 * @opensearch.experimental
 */
public class TransportDeleteQueryGroupAction extends HandledTransportAction<DeleteQueryGroupRequest, DeleteQueryGroupResponse> {

    private final QueryGroupPersistenceService queryGroupPersistenceService;

    /**
     * Constructor for TransportDeleteQueryGroupAction
     *
     * @param actionName - action name
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param queryGroupPersistenceService - a {@link QueryGroupPersistenceService} object
     */
    @Inject
    public TransportDeleteQueryGroupAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        QueryGroupPersistenceService queryGroupPersistenceService
    ) {
        super(DeleteQueryGroupAction.NAME, transportService, actionFilters, DeleteQueryGroupRequest::new);
        this.queryGroupPersistenceService = queryGroupPersistenceService;
    }

    @Override
    protected void doExecute(Task task, DeleteQueryGroupRequest request, ActionListener<DeleteQueryGroupResponse> listener) {
        queryGroupPersistenceService.deleteInClusterStateMetadata(request.getName(), listener);
    }
}
