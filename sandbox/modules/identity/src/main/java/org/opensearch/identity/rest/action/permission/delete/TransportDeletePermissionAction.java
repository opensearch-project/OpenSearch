/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.delete;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.identity.rest.action.permission.add.AddPermissionAction;
import org.opensearch.identity.rest.service.PermissionService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportDeletePermissionAction extends HandledTransportAction<DeletePermissionRequest, DeletePermissionResponse> {

    private final PermissionService permissionService;

    public TransportDeletePermissionAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PermissionService permissionService
    ) {
        super(AddPermissionAction.NAME, transportService, actionFilters, DeletePermissionRequest::new);
        this.permissionService = permissionService;
    }

    protected void doExecute(Task task, DeletePermissionRequest request, ActionListener<DeletePermissionResponse> listener) {
        String permissionString = request.getPermissionString();
        String principalString = request.getPrincipalString();
        this.permissionService.deletePermission(permissionString, principalString, listener);
    }
}
