/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.action.ProtobufActionType;

/**
 * Transport action for OpenSearch Node Information
*
* @opensearch.internal
*/
public class ProtobufNodesInfoAction extends ProtobufActionType<ProtobufNodesInfoResponse> {

    public static final ProtobufNodesInfoAction INSTANCE = new ProtobufNodesInfoAction();
    public static final String NAME = "cluster:monitor/nodes/info";

    private ProtobufNodesInfoAction() {
        super(NAME, ProtobufNodesInfoResponse::new);
    }
}
