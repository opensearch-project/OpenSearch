/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.action.ActionType;

/**
 * Action to retrieve DataFusion info from nodes
 */
public class NodesDataFusionInfoAction extends ActionType<NodesDataFusionInfoResponse> {
    /**
     * Singleton instance of NodesDataFusionInfoAction.
     */
    public static final NodesDataFusionInfoAction INSTANCE = new NodesDataFusionInfoAction();
    /**
     * Name of this action.
     */
    public static final String NAME = "cluster:admin/datafusion/info";

    NodesDataFusionInfoAction() {
        super(NAME, NodesDataFusionInfoResponse::new);
    }
}
