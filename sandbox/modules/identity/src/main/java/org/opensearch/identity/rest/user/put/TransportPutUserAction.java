/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.put;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.identity.User;
import org.opensearch.identity.rest.user.UserService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.Map;
import java.util.Set;

/**
 * Transport action for Creating a user
 */
public class TransportPutUserAction extends HandledTransportAction<PutUserRequest, PutUserResponse> {
    private final UserService userService;

    @Inject
    public TransportPutUserAction(TransportService transportService, ActionFilters actionFilters, UserService userService) {
        super(PutUserAction.NAME, transportService, actionFilters, PutUserRequest::new);
        this.userService = userService;
    }

    /**
     * Invokes 'create a user' workflow
     */
    @Override
    protected void doExecute(Task task, PutUserRequest request, ActionListener<PutUserResponse> listener) {
        String username = request.getUsername();
        String password = request.getPassword();
        Map<String, String> attributes = request.getAttributes();
        Set<String> permissions = request.getPermissions();
        User user = new User(username, password, attributes, permissions);
        userService.createOrUpdateUser(user, listener);
    }

}
