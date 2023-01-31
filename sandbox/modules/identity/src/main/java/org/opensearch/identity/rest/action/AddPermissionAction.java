/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action;

import org.opensearch.action.ActionType;
import org.opensearch.identity.rest.response.AddPermissionResponse;

public class AddPermissionAction extends ActionType<AddPermissionResponse> {

    public static final AddPermissionAction INSTANCE = new AddPermissionAction();

    // TODO : revisit this action type
    public static final String TYPE = "cluster:permission";

    AddPermissionAction() {
        super(TYPE, AddPermissionResponse::new);
    }
}
