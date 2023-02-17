/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.delete;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.identity.rest.user.UserService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for Deleting a user
 */
public class TransportDeleteUserAction extends HandledTransportAction<DeleteUserRequest, DeleteUserResponse> {
    private final UserService userService;

    @Inject
    public TransportDeleteUserAction(TransportService transportService, ActionFilters actionFilters, UserService userService) {
        super(DeleteUserAction.NAME, transportService, actionFilters, DeleteUserRequest::new);
        this.userService = userService;
    }

    /**
     * Invokes 'delete a user' workflow
     */
    @Override
    protected void doExecute(Task task, DeleteUserRequest request, ActionListener<DeleteUserResponse> listener) {
        String username = request.getUsername();
        userService.deleteUser(username, listener);
    }

}
