/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.get.multi;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.identity.rest.user.UserService;
import org.opensearch.identity.rest.user.get.single.GetUserAction;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for retrieving multiple users
 */
public class TransportMultiGetUserAction extends HandledTransportAction<MultiGetUserRequest, MultiGetUserResponse> {
    private final UserService userService;

    @Inject
    public TransportMultiGetUserAction(TransportService transportService, ActionFilters actionFilters, UserService userService) {
        super(GetUserAction.NAME, transportService, actionFilters, MultiGetUserRequest::new);
        this.userService = userService;
    }

    /**
     * Invokes 'get multiple users' workflow
     */
    @Override
    protected void doExecute(Task task, MultiGetUserRequest request, ActionListener<MultiGetUserResponse> listener) {
        userService.getUsers(listener);
    }

}
