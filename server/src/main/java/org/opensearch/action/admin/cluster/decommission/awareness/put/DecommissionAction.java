/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import org.opensearch.action.ActionType;

/**
 * Register decommission action
 *
 * @opensearch.internal
 */
public final class DecommissionAction extends ActionType<DecommissionResponse> {
    public static final DecommissionAction INSTANCE = new DecommissionAction();
    public static final String NAME = "cluster:admin/decommission/awareness/put";

    private DecommissionAction() {
        super(NAME, DecommissionResponse::new);
    }
}
