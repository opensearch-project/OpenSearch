/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user.get.multi;

import org.opensearch.action.ActionType;

/**
 * Action type for getting multiple user
 */
public class MultiGetUserAction extends ActionType<MultiGetUserResponse> {

    public static final MultiGetUserAction INSTANCE = new MultiGetUserAction();

    // TODO : revisit this action type
    public static final String NAME = "cluster:admin/user/mget";

    private MultiGetUserAction() {
        super(NAME, MultiGetUserResponse::new);
    }
}
