/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.action.support.nodes;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.transport.ProtobufTransportResponse;

import java.io.IOException;

/**
 * A base class for node level operations.
*
* @opensearch.internal
*/
public abstract class ProtobufBaseNodeResponse extends ProtobufTransportResponse {

    private ProtobufDiscoveryNode node;

    protected ProtobufBaseNodeResponse(CodedInputStream in) throws IOException {
        super(in);
        node = new ProtobufDiscoveryNode(in);
    }

    protected ProtobufBaseNodeResponse(ProtobufDiscoveryNode node) {
        assert node != null;
        this.node = node;
    }

    /**
     * The node this information relates to.
    */
    public ProtobufDiscoveryNode getNode() {
        return node;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        node.writeTo(out);
    }
}
