/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.identity.authz.Permission;
import org.opensearch.identity.rest.request.CheckPermissionRequest;
import org.opensearch.identity.rest.response.CheckPermissionResponse;
import org.opensearch.identity.rest.service.UserService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCheckPermissionAction extends HandledTransportAction<CheckPermissionRequest, CheckPermissionResponse> {

    private final UserService userService;

    public TransportCheckPermissionAction(TransportService transportService, ActionFilters actionFilters, UserService userService) {
        super(AddPermissionAction.TYPE, transportService, actionFilters, CheckPermissionRequest::new);
        this.userService = userService;
    }

    protected void doExecute(Task task, CheckPermissionRequest request, ActionListener<CheckPermissionResponse> listener) {
        Permission permissionToAdd = new Permission(request.getPermissionString());
    }
}
