/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.Nodes;

import org.opensearch.action.ActionType;

/**
 * Transport action for cat nodes
 *
 * @opensearch.internal
 */
public class CatNodesAction extends ActionType<CatNodesResponse> {
    public static final CatNodesAction INSTANCE = new CatNodesAction();
    public static final String NAME = "cluster:monitor/nodes/cat";

    public CatNodesAction() {
        super(NAME, CatNodesResponse::new);
    }
}
