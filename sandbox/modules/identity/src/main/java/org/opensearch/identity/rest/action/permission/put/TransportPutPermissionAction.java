/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.put;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.identity.rest.service.PermissionService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportPutPermissionAction extends HandledTransportAction<PutPermissionRequest, PutPermissionResponse> {

    private final PermissionService permissionService;

    /**
     * Construct a new transport action for the put permission action. This will then be used to facilitate the execution of the request.
     * @param transportService OpenSearch's main transport service which handles operations on the transport layer
     * @param actionFilters Handles plugin action filter configurations
     * @param permissionService Executes the different permission operations
     */
    public TransportPutPermissionAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PermissionService permissionService
    ) {
        super(PutPermissionAction.NAME, transportService, actionFilters, PutPermissionRequest::new);
        this.permissionService = permissionService;
    }

    /**
     * doExecute connects the transport layer permission service to the action request
     * @param task What OpenSearch is doing -- this is not needed for permission-related doExecute
     * @param request The request object that we want to perform
     * @param listener A listener that notifies the client about the execution progress
     */
    @Override
    protected void doExecute(Task task, PutPermissionRequest request, ActionListener<PutPermissionResponse> listener) {
        String permissionString = request.getPermissionString();
        String username = request.getUsername();
        this.permissionService.putPermission(permissionString, username, listener);
    }
}
