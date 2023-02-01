/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission.check;

import org.opensearch.action.ActionType;

/**
 * This class defines the CheckPermissionAction ActionType which corresponds to an action that checks if a subject has a specific permission while using Identity.
 */
public class CheckPermissionAction extends ActionType<CheckPermissionResponse> {

    public static final CheckPermissionAction INSTANCE = new CheckPermissionAction();

    // TODO : revisit this action type

    // The action name
    public static final String NAME = "cluster:permission/check";

    CheckPermissionAction() {
        super(NAME, CheckPermissionResponse::new);
    }
}
