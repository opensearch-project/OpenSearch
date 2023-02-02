/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.read;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.identity.rest.user.UserService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for Creating a user
 */
public class TransportGetUserAction extends HandledTransportAction<GetUserRequest, GetUserResponse> {
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final UserService userService;

    @Inject
    public TransportGetUserAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedWriteableRegistry namedWriteableRegistry,
        UserService userService
    ) {
        super(GetUserAction.NAME, transportService, actionFilters, GetUserRequest::new);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.userService = userService;
    }

    /**
     * Invokes 'create a user' workflow
     */
    @Override
    protected void doExecute(Task task, GetUserRequest request, ActionListener<GetUserResponse> listener) {
        String username = request.getUsername();
        String password = request.getPassword();
        userService.createUser(username, password, listener);
    }

}
