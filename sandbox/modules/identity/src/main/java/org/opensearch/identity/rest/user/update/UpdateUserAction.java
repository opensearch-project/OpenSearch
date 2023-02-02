/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.update;

import org.opensearch.action.ActionType;

/**
 * Action type for creating a user
 */
public class UpdateUserAction extends ActionType<UpdateUserResponse> {

    public static final UpdateUserAction INSTANCE = new UpdateUserAction();

    // TODO : revisit this action type
    public static final String NAME = "cluster:admin/user/create";

    private UpdateUserAction() {
        super(NAME, UpdateUserResponse::new);
    }
}
