/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.resetpassword;

import org.opensearch.action.ActionType;

/**
 * Action type for creating or updating a user
 */
public class ResetPasswordAction extends ActionType<ResetPasswordResponse> {

    public static final ResetPasswordAction INSTANCE = new ResetPasswordAction();

    // TODO : revisit this action type
    public static final String NAME = "cluster:admin/user/resetpassword";

    private ResetPasswordAction() {
        super(NAME, ResetPasswordResponse::new);
    }
}
