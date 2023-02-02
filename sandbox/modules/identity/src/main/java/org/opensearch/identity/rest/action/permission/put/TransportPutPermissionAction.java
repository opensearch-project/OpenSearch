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

    public TransportPutPermissionAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PermissionService permissionService
    ) {
        super(PutPermissionAction.NAME, transportService, actionFilters, PutPermissionRequest::new);
        this.permissionService = permissionService;
    }

    protected void doExecute(Task task, PutPermissionRequest request, ActionListener<PutPermissionResponse> listener) {
        String permissionString = request.getPermissionString();
        String principalString = request.getPrincipalString();
        this.permissionService.putPermission(permissionString, principalString, listener);
    }
}
