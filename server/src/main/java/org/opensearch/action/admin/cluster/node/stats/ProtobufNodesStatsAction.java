/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.ProtobufActionType;

/**
 * Transport action for obtaining OpenSearch Node Stats
*
* @opensearch.internal
*/
public class ProtobufNodesStatsAction extends ProtobufActionType<ProtobufNodesStatsResponse> {

    public static final ProtobufNodesStatsAction INSTANCE = new ProtobufNodesStatsAction();
    public static final String NAME = "cluster:monitor/nodes/stats";

    private ProtobufNodesStatsAction() {
        super(NAME, ProtobufNodesStatsResponse::new);
    }
}
