/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.put;

import org.opensearch.action.ActionType;

/**
 * Action type for creating or updating a user
 */
public class PutUserAction extends ActionType<PutUserResponse> {

    public static final PutUserAction INSTANCE = new PutUserAction();

    // TODO : revisit this action type
    public static final String NAME = "cluster:admin/user/create";

    private PutUserAction() {
        super(NAME, PutUserResponse::new);
    }
}
