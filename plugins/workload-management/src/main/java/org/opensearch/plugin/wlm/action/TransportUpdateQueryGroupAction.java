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
 * Transport action to update QueryGroup
 *
 * @opensearch.experimental
 */
public class TransportUpdateQueryGroupAction extends HandledTransportAction<UpdateQueryGroupRequest, UpdateQueryGroupResponse> {

    private final QueryGroupPersistenceService queryGroupPersistenceService;

    /**
     * Constructor for TransportUpdateQueryGroupAction
     *
     * @param actionName - action name
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param queryGroupPersistenceService - a {@link QueryGroupPersistenceService} object
     */
    @Inject
    public TransportUpdateQueryGroupAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        QueryGroupPersistenceService queryGroupPersistenceService
    ) {
        super(UpdateQueryGroupAction.NAME, transportService, actionFilters, UpdateQueryGroupRequest::new);
        this.queryGroupPersistenceService = queryGroupPersistenceService;
    }

    @Override
    protected void doExecute(Task task, UpdateQueryGroupRequest request, ActionListener<UpdateQueryGroupResponse> listener) {
        queryGroupPersistenceService.updateInClusterStateMetadata(request, listener);
    }
}
