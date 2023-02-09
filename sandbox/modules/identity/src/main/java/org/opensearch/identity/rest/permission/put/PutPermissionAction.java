/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.permission.put;

import org.opensearch.action.ActionType;

/**
 * This class defines the PutPermissionAction ActionType which corresponds to an action that grant a subject a permission while using Identity.
 */
public class PutPermissionAction extends ActionType<PutPermissionResponse> {

    public static final PutPermissionAction INSTANCE = new PutPermissionAction();
    public static final String NAME = "cluster:admin/permissions/put";

    PutPermissionAction() {
        super(NAME, PutPermissionResponse::new);
    }
}
