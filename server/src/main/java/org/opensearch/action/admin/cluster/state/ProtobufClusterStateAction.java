/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.state;

import org.opensearch.action.ProtobufActionType;

/**
 * Transport action for obtaining cluster state
*
* @opensearch.internal
*/
public class ProtobufClusterStateAction extends ProtobufActionType<ProtobufClusterStateResponse> {

    public static final ProtobufClusterStateAction INSTANCE = new ProtobufClusterStateAction();
    public static final String NAME = "cluster:monitor/state";

    private ProtobufClusterStateAction() {
        super(NAME, ProtobufClusterStateResponse::new);
    }
}
