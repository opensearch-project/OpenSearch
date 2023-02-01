/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.add;

import org.opensearch.action.ActionType;

/**
 * This class defines the AddPermissionAction ActionType which corresponds to an action that grant a subject permissions while using Identity.
 */
public class AddPermissionAction extends ActionType<AddPermissionResponse> {

    public static final AddPermissionAction INSTANCE = new AddPermissionAction();

    // TODO : revisit this action type

    // The action name
    public static final String NAME = "cluster:permissions/add";

    AddPermissionAction() {
        super(NAME, AddPermissionResponse::new);
    }
}
