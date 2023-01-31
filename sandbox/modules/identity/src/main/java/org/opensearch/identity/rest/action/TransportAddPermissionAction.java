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
import org.opensearch.identity.rest.request.AddPermissionRequest;
import org.opensearch.identity.rest.response.AddPermissionResponse;
import org.opensearch.identity.rest.service.UserService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportAddPermissionAction extends HandledTransportAction<AddPermissionRequest, AddPermissionResponse> {

    private final UserService userService;

    public TransportAddPermissionAction(TransportService transportService, ActionFilters actionFilters, UserService userService) {
        super(AddPermissionAction.NAME, transportService, actionFilters, AddPermissionRequest::new);
        this.userService = userService;
    }

    protected void doExecute(Task task, AddPermissionRequest request, ActionListener<AddPermissionResponse> listener) {
        Permission permissionToAdd = new Permission(request.getPermissionString());
    }
}
