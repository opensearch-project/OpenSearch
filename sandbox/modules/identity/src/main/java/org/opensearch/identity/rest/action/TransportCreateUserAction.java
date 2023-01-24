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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.identity.rest.request.CreateUserRequest;
import org.opensearch.identity.rest.response.CreateUserResponse;
import org.opensearch.identity.rest.service.UserService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for deleting point in time searches - supports deleting list and all point in time searches
 */
public class TransportCreateUserAction extends HandledTransportAction<CreateUserRequest, CreateUserResponse> {
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final UserService userService;

    @Inject
    public TransportCreateUserAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedWriteableRegistry namedWriteableRegistry,
        UserService pitService
    ) {
        super(CreateUserAction.NAME, transportService, actionFilters, CreateUserRequest::new);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.userService = pitService;
    }

    /**
     * Invoke 'create a user' workflow
     */
    @Override
    protected void doExecute(Task task, CreateUserRequest request, ActionListener<CreateUserResponse> listener) {
        String username = request.getUsername();
        String password = request.getPassword();
        userService.createUser(username, password, listener);
    }

}
