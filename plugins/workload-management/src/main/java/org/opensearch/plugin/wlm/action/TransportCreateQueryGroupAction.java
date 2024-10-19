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
 * Transport action to create QueryGroup
 *
 * @opensearch.experimental
 */
public class TransportCreateQueryGroupAction extends HandledTransportAction<CreateQueryGroupRequest, CreateQueryGroupResponse> {

    private final QueryGroupPersistenceService queryGroupPersistenceService;

    /**
     * Constructor for TransportCreateQueryGroupAction
     *
     * @param actionName - action name
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param queryGroupPersistenceService - a {@link QueryGroupPersistenceService} object
     */
    @Inject
    public TransportCreateQueryGroupAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        QueryGroupPersistenceService queryGroupPersistenceService
    ) {
        super(CreateQueryGroupAction.NAME, transportService, actionFilters, CreateQueryGroupRequest::new);
        this.queryGroupPersistenceService = queryGroupPersistenceService;
    }

    @Override
    protected void doExecute(Task task, CreateQueryGroupRequest request, ActionListener<CreateQueryGroupResponse> listener) {
        queryGroupPersistenceService.persistInClusterStateMetadata(request.getQueryGroup(), listener);
    }
}
