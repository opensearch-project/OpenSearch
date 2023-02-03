/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.delete;

import org.opensearch.action.ActionType;

/**
 * Action type for deleting a single user
 */
public class DeleteUserAction extends ActionType<DeleteUserResponse> {

    public static final DeleteUserAction INSTANCE = new DeleteUserAction();

    // TODO : revisit this action type
    public static final String NAME = "cluster:admin/user/delete";

    private DeleteUserAction() {
        super(NAME, DeleteUserResponse::new);
    }
}
