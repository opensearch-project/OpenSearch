/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.get;

import org.opensearch.action.ActionType;

/**
 * This class defines the GetPermissionAction ActionType which corresponds to an action that list the permissions of a subject while using Identity.
 */
public class GetPermissionAction extends ActionType<GetPermissionResponse> {

    public static final GetPermissionAction INSTANCE = new GetPermissionAction();
    public static final String NAME = "cluster:admin/permissions/get";

    GetPermissionAction() {
        super(NAME, GetPermissionResponse::new);
    }
}
