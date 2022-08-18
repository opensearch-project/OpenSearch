/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.get;

import org.opensearch.action.ActionType;

public class GetDecommissionAction extends ActionType<GetDecommissionResponse> {

    public static final GetDecommissionAction INSTANCE = new GetDecommissionAction();
    public static final String NAME = "cluster:admin/decommission/awareness/get";

    private GetDecommissionAction() {
        super(NAME, GetDecommissionResponse::new);
    }
}
