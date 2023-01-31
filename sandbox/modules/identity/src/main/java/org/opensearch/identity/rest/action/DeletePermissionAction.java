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

public class DeletePermissionAction extends ActionType<DeletePermissionResponse> {

    public static final DeletePermissionAction INSTANCE = new DeletePermissionAction();

    // TODO : revisit this action type
    public static final String TYPE = "cluster:permission";

    DeletePermissionAction() {
        super(TYPE, DeletePermissionResponse::new);
    }
}
