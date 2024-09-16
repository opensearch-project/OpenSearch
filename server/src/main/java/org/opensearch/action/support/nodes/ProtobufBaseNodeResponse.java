/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support.nodes;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.transport.TransportResponse;

/**
 * A base class for node level operations.
*
* @opensearch.internal
*/
public abstract class ProtobufBaseNodeResponse extends TransportResponse {

    private DiscoveryNode node;

    protected ProtobufBaseNodeResponse(byte[] data) {

    }

    protected ProtobufBaseNodeResponse(DiscoveryNode node) {
        assert node != null;
        this.node = node;
    }

    /**
     * The node this information relates to.
    */
    public DiscoveryNode getNode() {
        return node;
    }

}
