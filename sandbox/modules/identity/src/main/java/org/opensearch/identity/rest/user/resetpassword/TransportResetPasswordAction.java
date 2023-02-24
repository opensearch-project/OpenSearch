/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.resetpassword;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.identity.rest.user.UserService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for Creating a user
 */
public class TransportResetPasswordAction extends HandledTransportAction<ResetPasswordRequest, ResetPasswordResponse> {
    private final UserService userService;

    @Inject
    public TransportResetPasswordAction(TransportService transportService, ActionFilters actionFilters, UserService userService) {
        super(ResetPasswordAction.NAME, transportService, actionFilters, ResetPasswordRequest::new);
        this.userService = userService;
    }

    /**
     * Invokes 'create a user' workflow
     */
    @Override
    protected void doExecute(Task task, ResetPasswordRequest request, ActionListener<ResetPasswordResponse> listener) {
        userService.resetPassword(request, listener);
    }
}
