/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action;

import org.opensearch.action.ActionType;
import org.opensearch.identity.rest.response.DeletePermissionResponse;

/**
 * This class defines the DeletePermissionAction ActionType which corresponds to an action that deletes a specific permission from the permission store while using Identity.
 */
public class DeletePermissionAction extends ActionType<DeletePermissionResponse> {

    public static final DeletePermissionAction INSTANCE = new DeletePermissionAction();

    // TODO : revisit this action type
    public static final String NAME = "cluster:permission";

    DeletePermissionAction() {
        super(NAME, DeletePermissionResponse::new);
    }
}
