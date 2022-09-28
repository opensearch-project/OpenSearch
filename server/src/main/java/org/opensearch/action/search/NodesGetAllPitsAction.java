/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

/**
 * Action type for retrieving all PIT reader contexts from nodes
 */
public class NodesGetAllPitsAction extends ActionType<GetAllPitNodesResponse> {
    public static final NodesGetAllPitsAction INSTANCE = new NodesGetAllPitsAction();
    public static final String NAME = "cluster:admin/point_in_time/read_from_nodes";

    private NodesGetAllPitsAction() {
        super(NAME, GetAllPitNodesResponse::new);
    }
}
